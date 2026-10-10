// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package plugins

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/db"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
)

// ConnPool 按 "user@host:port" 缓存 *sql.DB，并采用懒拨号方式建立连接。
// 它可安全地并发使用，设计为供单个 Agent 进程管理的每个插件和每个数据库实例
// 共享。
type ConnPool struct {
	maxOpen    int
	maxIdle    int
	idleTime   time.Duration
	maxEntries int

	// dialer 执行实际的连接操作。默认为 dialReal，测试中可替换，
	// 以避免建立真实的数据库连接。
	dialer dialFn

	mu      sync.Mutex
	entries map[string]*poolEntry
	dialing map[string]*sync.Mutex // 按 key 划分的 singleflight 锁
}

type dialFn func(ctx context.Context, dbType, addr, user, pass string) (*sql.DB, error)

type poolEntry struct {
	db       *sql.DB
	lastUsed time.Time
}

// NewConnPool 根据 cfg 构建连接池（请先调用 config.PluginPoolConfig.WithDefaults）。
func NewConnPool(cfg config.PluginPoolConfig) (*ConnPool, error) {
	idleTime, err := time.ParseDuration(cfg.IdleTime)
	if err != nil || idleTime <= 0 {
		idleTime = 10 * time.Minute
	}
	p := &ConnPool{
		maxOpen:    cfg.MaxOpenConns,
		maxIdle:    cfg.MaxIdleConns,
		idleTime:   idleTime,
		maxEntries: cfg.MaxEntries,
		entries:    make(map[string]*poolEntry),
		dialing:    make(map[string]*sync.Mutex),
	}
	p.dialer = p.dialReal
	return p, nil
}

// key 按数据库类型、身份和用户区分连接，因此不同数据库类型（或同一地址
// 使用不同账号访问）永远不会共享同一个 *sql.DB。密码不包含在 key 中。
func poolKey(dbType, addr, user string) string {
	return dbType + "|" + user + "@" + addr
}

// Get 返回 addr 对应的已缓存 DB，若未缓存则使用给定凭证拨号建立一个。
// 针对同一尚未缓存目标的并发请求只会拨号一次。
func (p *ConnPool) Get(ctx context.Context, dbType, addr, user, pass string) (*sql.DB, error) {
	key := poolKey(dbType, addr, user)

	p.mu.Lock()
	if e, ok := p.entries[key]; ok {
		e.lastUsed = time.Now()
		p.mu.Unlock()
		return e.db, nil
	}
	keyLock, ok := p.dialing[key]
	if !ok {
		keyLock = &sync.Mutex{}
		p.dialing[key] = keyLock
	}
	p.mu.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	// 获取按 key 划分的锁之后进行二次检查。
	p.mu.Lock()
	if e, ok := p.entries[key]; ok {
		e.lastUsed = time.Now()
		p.mu.Unlock()
		return e.db, nil
	}
	p.mu.Unlock()

	db, err := p.dialer(ctx, dbType, addr, user, pass)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.entries[key] = &poolEntry{db: db, lastUsed: time.Now()}
	p.evictOldestIfOverLimit(key)
	p.mu.Unlock()

	return db, nil
}

func (p *ConnPool) dialReal(ctx context.Context, dbType, addr, user, pass string) (*sql.DB, error) {
	dialect, err := db.GetDialect(dbType)
	if err != nil {
		return nil, fmt.Errorf("resolve dialect: %w", err)
	}

	dsn := dialect.BuildDSN(db.ConnSpec{
		Host:     userHost(addr),
		Port:     userPort(addr),
		User:     user,
		Password: pass,
		Timeout:  "5s",
	})
	d, err := sql.Open(dialect.SQLDriverName(), dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", addr, err)
	}
	d.SetMaxOpenConns(p.maxOpen)
	d.SetMaxIdleConns(p.maxIdle)
	d.SetConnMaxIdleTime(p.idleTime)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.PingContext(pingCtx); err != nil {
		d.Close()
		return nil, fmt.Errorf("ping %s: %w", addr, err)
	}
	return d, nil
}

// userHost / userPort 将拨号地址（"host:port"）拆回各组成部分供 dialect 使用。
// 它们是小型本地辅助函数，使连接池不必依赖调用方再次单独传入 host 和 port。
func userHost(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

func userPort(addr string) int32 {
	if _, p, err := net.SplitHostPort(addr); err == nil {
		if n, err := parseInt32(p); err == nil {
			return n
		}
	}
	return 0
}

func parseInt32(s string) (int32, error) {
	n, err := strconv.ParseInt(s, 10, 32)
	return int32(n), err
}

// evictOldestIfOverLimit 强制执行 LRU 容量上限。调用方必须持有 p.mu，且
// 刚插入的 key 永远不会成为淘汰对象。
func (p *ConnPool) evictOldestIfOverLimit(keep string) {
	if p.maxEntries <= 0 || len(p.entries) <= p.maxEntries {
		return
	}

	var oldestKey string
	var oldestAt time.Time
	for k, e := range p.entries {
		if k == keep {
			continue
		}
		if oldestKey == "" || e.lastUsed.Before(oldestAt) {
			oldestKey, oldestAt = k, e.lastUsed
		}
	}
	if oldestKey != "" {
		p.entries[oldestKey].db.Close()
		delete(p.entries, oldestKey)
		log.Printf("[PluginPool] entry limit %d reached, evicted oldest slot %s", p.maxEntries, oldestKey)
	}
}

// CloseIdle 关闭并移除最近使用时间早于 maxIdle 的槽位。
func (p *ConnPool) CloseIdle(maxIdle time.Duration) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-maxIdle)
	evicted := 0
	for k, e := range p.entries {
		if e.lastUsed.Before(cutoff) {
			e.db.Close()
			delete(p.entries, k)
			evicted++
		}
	}
	if evicted > 0 {
		log.Printf("[PluginPool] reclaimed %d idle slot(s) unused for more than %s", evicted, maxIdle)
	}
	return evicted
}

// StartReclaimer 周期性淘汰超过 reclaimAfter 未使用的槽位。
// ctx 取消时停止。每个连接池调用一次是安全的。
func (p *ConnPool) StartReclaimer(ctx context.Context, interval, reclaimAfter time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if reclaimAfter <= 0 {
		reclaimAfter = 30 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.CloseIdle(reclaimAfter)
		}
	}
}

// Len 返回当前缓存的 *sql.DB 槽位数量。
func (p *ConnPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

// Close 关闭所有缓存的连接。在 Agent 关闭时调用。
func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	closed := 0
	for k, e := range p.entries {
		e.db.Close()
		delete(p.entries, k)
		closed++
	}
	if closed > 0 {
		log.Printf("[PluginPool] closed %d cached connection slot(s) on shutdown", closed)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 进程级默认连接池：由 cmd/agent 根据配置构建，供各插件使用
// ---------------------------------------------------------------------------

var (
	defaultPool *ConnPool
	defaultMu   sync.Mutex
)

// SetDefaultPool 安装供插件使用的共享连接池。
func SetDefaultPool(p *ConnPool) {
	defaultMu.Lock()
	defaultPool = p
	defaultMu.Unlock()
}

// DefaultPool 返回已配置的共享连接池；若进程未安装，则使用默认设置
// 懒创建一个（用于测试 / 独立运行场景）。
func DefaultPool() *ConnPool {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultPool == nil {
		p, _ := NewConnPool(config.DefaultPluginPoolConfig())
		defaultPool = p
	}
	return defaultPool
}

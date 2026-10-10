// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package plugins

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	_ "github.com/yunnnnn1/go-helix/pkg/db/drivers/mysql" // 为假/真实拨号器注册 MySQL 驱动和 dialect
)

// testHooks 控制假拨号器。
type testHooks struct {
	dialCount *int32
	block     func() // 使后续拨号阻塞，直到被释放
	release   func() // 解除被阻塞拨号的阻塞状态
}

// newTestPool 构建一个使用假拨号器的连接池，该拨号器从不打开真实
// 连接；它为每个 key 返回一个独立的懒加载 *sql.DB。
func newTestPool(t *testing.T, maxEntries int) (*ConnPool, testHooks) {
	t.Helper()
	cfg := config.DefaultPluginPoolConfig()
	if maxEntries > 0 {
		cfg.MaxEntries = maxEntries
	}
	p, err := NewConnPool(cfg)
	if err != nil {
		t.Fatalf("NewConnPool: %v", err)
	}

	var dialCount int32
	var blocking int32
	releaseCh := make(chan struct{})
	var once sync.Once

	p.dialer = func(ctx context.Context, dbType, addr, user, pass string) (*sql.DB, error) {
		atomic.AddInt32(&dialCount, 1)
		if atomic.LoadInt32(&blocking) == 1 {
			<-releaseCh
		}
		db, _ := sql.Open("mysql", "u:p@tcp("+addr+")/")
		return db, nil
	}

	hooks := testHooks{
		dialCount: &dialCount,
		block:     func() { atomic.StoreInt32(&blocking, 1) },
		release:   func() { once.Do(func() { close(releaseCh) }) },
	}
	return p, hooks
}

func TestPool_GetReusesConnection(t *testing.T) {
	p, hooks := newTestPool(t, 0)
	defer p.Close()
	ctx := context.Background()

	db1, err := p.Get(ctx, "mysql", "10.0.0.1:3306", "u", "p")
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	db2, err := p.Get(ctx, "mysql", "10.0.0.1:3306", "u", "p")
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if db1 != db2 {
		t.Error("expected same *sql.DB to be returned for the same target")
	}
	if p.Len() != 1 {
		t.Errorf("expected 1 slot, got %d", p.Len())
	}
	if atomic.LoadInt32(hooks.dialCount) != 1 {
		t.Errorf("expected exactly 1 dial, got %d", *hooks.dialCount)
	}
}

func TestPool_DifferentUserSeparateSlots(t *testing.T) {
	p, hooks := newTestPool(t, 0)
	defer p.Close()
	ctx := context.Background()

	if _, err := p.Get(ctx, "mysql", "10.0.0.1:3306", "u1", "p"); err != nil {
		t.Fatalf("Get u1: %v", err)
	}
	if _, err := p.Get(ctx, "mysql", "10.0.0.1:3306", "u2", "p"); err != nil {
		t.Fatalf("Get u2: %v", err)
	}
	if p.Len() != 2 {
		t.Errorf("expected 2 slots for different users, got %d", p.Len())
	}
	if atomic.LoadInt32(hooks.dialCount) != 2 {
		t.Errorf("expected 2 dials, got %d", *hooks.dialCount)
	}
}

func TestPool_SingleflightDialsOnce(t *testing.T) {
	p, hooks := newTestPool(t, 0)
	defer p.Close()
	ctx := context.Background()

	hooks.block() // 拨号器会一直阻塞直到被释放，期间持有按 key 划分的锁

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			p.Get(ctx, "mysql", "10.0.0.9:3306", "u", "p")
		}()
	}

	// 让所有 goroutine 排队，然后解除唯一在途拨号的阻塞。
	time.Sleep(100 * time.Millisecond)
	hooks.release()
	wg.Wait()

	if got := atomic.LoadInt32(hooks.dialCount); got != 1 {
		t.Errorf("expected 1 dial under concurrent same-target Get, got %d", got)
	}
	if p.Len() != 1 {
		t.Errorf("expected 1 slot, got %d", p.Len())
	}
}

func TestPool_LRUEvictsOldest(t *testing.T) {
	p, _ := newTestPool(t, 2)
	defer p.Close()
	ctx := context.Background()

	a, _ := p.Get(ctx, "mysql", "10.0.0.1:3306", "u", "p")
	time.Sleep(5 * time.Millisecond)
	b, _ := p.Get(ctx, "mysql", "10.0.0.2:3306", "u", "p")
	time.Sleep(5 * time.Millisecond)
	c, _ := p.Get(ctx, "mysql", "10.0.0.3:3306", "u", "p")

	if p.Len() != 2 {
		t.Fatalf("expected slot cap 2, got %d", p.Len())
	}

	// 最旧的条目（地址 a）应已被淘汰；重新 Get 会返回新的句柄。
	aAgain, _ := p.Get(ctx, "mysql", "10.0.0.1:3306", "u", "p")
	if aAgain == a {
		t.Error("expected oldest entry to have been evicted and re-created")
	}
	if b == nil || c == nil {
		t.Error("unexpected nil handles")
	}
}

func TestPool_CloseIdleEvictsStaleSlots(t *testing.T) {
	p, _ := newTestPool(t, 0)
	defer p.Close()
	ctx := context.Background()

	p.Get(ctx, "mysql", "10.0.0.1:3306", "u", "p")
	p.Get(ctx, "mysql", "10.0.0.2:3306", "u", "p")

	// 负值 maxIdle 会使截止点落在“未来”，因此每个槽位都已过期。
	if evicted := p.CloseIdle(-time.Second); evicted != 2 {
		t.Errorf("expected 2 evicted slots, got %d", evicted)
	}
	if p.Len() != 0 {
		t.Errorf("expected 0 slots after reclaim, got %d", p.Len())
	}
}

func TestPool_RealDialer_BadAddressNoSlot(t *testing.T) {
	cfg := config.DefaultPluginPoolConfig()
	p, err := NewConnPool(cfg) // 默认的真实拨号器
	if err != nil {
		t.Fatalf("NewConnPool: %v", err)
	}
	defer p.Close()

	// 回环地址的 1 端口必然被拒绝；ping 失败时不得缓存槽位。
	if _, err := p.Get(context.Background(), "mysql", "127.0.0.1:1", "u", "p"); err == nil {
		t.Error("expected error connecting to unreachable target")
	}
	if p.Len() != 0 {
		t.Errorf("expected no slot cached on failed dial, got %d", p.Len())
	}
}

func TestPool_DefaultPoolLazy(t *testing.T) {
	defaultMu.Lock()
	old := defaultPool
	defaultPool = nil
	defaultMu.Unlock()
	defer func() {
		defaultMu.Lock()
		defaultPool = old
		defaultMu.Unlock()
	}()

	if p := DefaultPool(); p == nil {
		t.Error("DefaultPool should lazily create a pool")
	}
}

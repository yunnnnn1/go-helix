// Package db 包含项目中共享的、与具体数据库类型无关的抽象。
package db

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ConnSpec 是与具体数据库无关的、描述如何连接到某个数据库的信息。Dialect
// 会将其转换为特定驱动的 DSN。
type ConnSpec struct {
	Host     string
	Port     int32
	User     string
	Password string
	DBName   string
	Timeout  string

	// Path 供基于文件的数据库（SQLite）使用。
	Path string
}

// Dialect 封装了如何与某种特定数据库类型进行通信。它取代了此前硬编码在连接
// 池和数据中心客户端中的 MySQL DSN 字面量。
type Dialect interface {
	// Type 返回规范的数据库类型名（例如 "mysql"）。
	Type() string
	// SQLDriverName 返回在 database/sql 中注册的名称
	// （例如 "mysql"、"pgx"）。
	SQLDriverName() string
	// BuildDSN 为 s 渲染特定驱动的数据源名称。
	BuildDSN(s ConnSpec) string
	// DeriveAgentPort 将数据库监听端口映射为与其一同运行的 Agent gRPC 端口
	// （例如 MySQL 为 dbPort + 20000）。
	DeriveAgentPort(dbPort int32) int32
	// Probe 通过使用其线路协议（TCP 连接 + 问候报文校验）而不进行认证，来
	// 验证 addr 处的数据库进程是否存活。
	Probe(ctx context.Context, addr string, timeout time.Duration) error
}

var (
	registryMu sync.RWMutex
	registry   = map[string]Dialect{}
)

// RegisterDialect 使 d 可在其类型名下使用。若 d 为 nil 则会 panic。
func RegisterDialect(d Dialect) {
	if d == nil {
		panic("db: RegisterDialect called with nil dialect")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[d.Type()] = d
}

// GetDialect 返回为 dbType 注册的方言。
func GetDialect(dbType string) (Dialect, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	d, ok := registry[dbType]
	if !ok {
		return nil, fmt.Errorf("db: dialect not registered for type %q", dbType)
	}
	return d, nil
}

// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package sqlite

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/db"

	_ "modernc.org/sqlite" // 注册名为 "sqlite" 的 database/sql 驱动（纯 Go，无 cgo）
)

// sqliteDialect 为 SQLite 实现 db.Dialect。SQLite 是一个嵌入式、基于文件的
// 数据库，因此其 DSN/连接语义与 TCP 服务器不同。
type sqliteDialect struct{}

func init() {
	db.RegisterDialect(sqliteDialect{})
}

func (sqliteDialect) Type() string          { return "sqlite" }
func (sqliteDialect) SQLDriverName() string { return "sqlite" }

// BuildDSN 渲染 modernc.org/sqlite 的数据源名称。数据库文件路径取自 s.Path。
// WAL 日志模式加上忙碌超时可使多个进程共享该文件（多读者、单写者串行化）。
func (sqliteDialect) BuildDSN(s db.ConnSpec) string {
	path := s.Path
	if path == "" {
		// 为仍将路径放在 DBName 中的调用方提供回退。
		path = s.DBName
	}

	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")

	// 普通的相对/绝对路径也受支持，但这里使用显式的 "file:" URI 形式，以便
	// 查询参数能够生效。
	return "file:" + path + "?" + q.Encode()
}

// DeriveAgentPort 对嵌入式 SQLite 元数据没有实际意义。方言接口与受管的 TCP
// 数据库共用；为完整性提供相同的数值规则（元数据客户端并不使用它）。
func (sqliteDialect) DeriveAgentPort(dbPort int32) int32 {
	return dbPort + 20000
}

// Probe 表明 SQLite 没有 TCP 端点。它不会被元数据客户端调用（仅由受管 TCP
// 发现流程调用）。
func (sqliteDialect) Probe(_ context.Context, addr string, _ time.Duration) error {
	return fmt.Errorf("sqlite: file-based database has no TCP endpoint (%s)", addr)
}

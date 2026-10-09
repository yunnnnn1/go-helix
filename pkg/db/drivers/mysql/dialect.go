package mysql

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/db"
	log "github.com/yunnnnn1/go-helix/pkg/logging"

	_ "github.com/go-sql-driver/mysql" // 注册名为 "mysql" 的 database/sql 驱动
)

// agentPortOffset 会加到 MySQL 端口上，用于推导同机部署的 Agent 端口。自动
// 发现将 Agent 绑定在 dbPort + 20000，当元数据存储缺少显式值时，Manager 也用
// 同一规则推导该端口。
const agentPortOffset int32 = 20000

// mysqlDialect 为 MySQL 实现 db.Dialect。所有 MySQL 特有的细节（DSN 形式、
// 端口映射、线路协议）都集中在这里，而不是重复散落在连接池、数据中心客户端
// 和监控器中。
type mysqlDialect struct{}

func init() {
	db.RegisterDialect(mysqlDialect{})
}

func (mysqlDialect) Type() string          { return "mysql" }
func (mysqlDialect) SQLDriverName() string { return "mysql" }

func (mysqlDialect) BuildDSN(s db.ConnSpec) string {
	timeout := s.Timeout
	if timeout == "" {
		timeout = "5s"
	}
	dbName := s.DBName
	// loc=Local：DATETIME 本身不带时区，MySQL 端用 NOW()（time_zone=SYSTEM）
	// 写入的是主机本地墙钟时间；读取时必须按同一本地时区锚定，否则驱动默认
	// 按 UTC 解释，会让心跳时间整体偏移一个时区（实测快 8 小时）。
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local&timeout=%s",
		s.User, s.Password, s.Host, s.Port, dbName, timeout)
}

func (mysqlDialect) DeriveAgentPort(dbPort int32) int32 {
	return dbPort + agentPortOffset
}

// Probe 拨号连接 addr，并校验 MySQL 握手的前几个字节：byte[3] 是序列号
// （初始问候报文为 0），byte[4] 是服务器协议版本（10）。若 TCP 连接返回的是
// 其他内容，则说明该端口背后的进程不是一个健康的 MySQL 服务器。
func (mysqlDialect) Probe(ctx context.Context, addr string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	dialer := net.Dialer{}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := dialer.DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set deadline %s: %w", addr, err)
	}
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read greeting from %s: %w", addr, err)
	}

	// MySQL 协议：byte[3]=序列号（问候报文为 0），byte[4]=协议版本（10）
	if header[3] != 0 || header[4] != 10 {
		return fmt.Errorf("%s responded but not MySQL protocol (seq=%d, ver=%d)", addr, header[3], header[4])
	}
	log.Printf("[MySQLDialect] probe ok: %s speaks MySQL protocol", addr)
	return nil
}

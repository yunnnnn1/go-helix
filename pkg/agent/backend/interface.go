// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package backend

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ReconnectBackoff 是后端（Backend）可选实现的一项能力，用于在数据库不可达
// 或拒绝探测连接时节流重连尝试。心跳借助它避免在每个 tick 都新建一个连接
// （从而导致一次失败的服务端握手），否则会触发数据库服务端按主机统计的连接
// 错误次数上限（MySQL Error 1129：主机被屏蔽）。
type ReconnectBackoff interface {
	// ShouldReconnect 报告当前是否可以尝试重连。
	ShouldReconnect(now time.Time) bool
	// NoteConnectFailure 记录一次失败的尝试，并返回在下次尝试前应用的退避
	// 延迟。
	NoteConnectFailure() time.Duration
}

// InstanceInfo 表示在本机上发现的一个数据库实例
type InstanceInfo struct {
	Type string
	Host string
	Port int
	PID  int
}

// Backend 定义 Agent 侧数据库操作的接口
type Backend interface {
	Ping(ctx context.Context) (bool, error)
	GetStatus(ctx context.Context) (map[string]string, error)
	KillProcess(ctx context.Context, pid int64, isOSPID bool) error
	ExecuteOperation(ctx context.Context, operation string, params map[string]string) (map[string]string, error)
	// Configure 传入用于健康检查的探测（业务）凭据。
	Configure(host string, port int, user, pass string) error
	// SetReplicationCredentials 保存仅供副本 IO 线程使用的复制专用账号；
	// Agent 绝不会使用该账号建立客户端连接。
	SetReplicationCredentials(user, pass string) error
	// ConfigureManager 传入用于故障切换/主备切换以及 GTID 操作的管理员
	// 账号。
	ConfigureManager(host string, port int, user, pass string) error
	// SetCluster 为后端标记其集群名，用于日志输出。
	SetCluster(name string)
	// Discover 发现本机上该数据库类型的实例
	Discover() ([]InstanceInfo, error)
	// Clone 创建一个新的后端实例（用于多实例支持）
	Clone() Backend
}

// Factory 管理已注册的后端
type Factory struct {
	mu       sync.RWMutex
	backends map[string]Backend
}

var GlobalFactory = &Factory{
	backends: make(map[string]Backend),
}

func Register(dbType string, backend Backend) {
	GlobalFactory.mu.Lock()
	defer GlobalFactory.mu.Unlock()
	GlobalFactory.backends[dbType] = backend
}

func GetBackend(dbType string) (Backend, error) {
	GlobalFactory.mu.RLock()
	defer GlobalFactory.mu.RUnlock()
	backend, ok := GlobalFactory.backends[dbType]
	if !ok {
		return nil, fmt.Errorf("backend not found for db type: %s", dbType)
	}
	return backend.Clone(), nil
}

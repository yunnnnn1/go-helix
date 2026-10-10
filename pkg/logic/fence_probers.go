// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logic

import (
	"sync"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// FenceProbers 提供带外手段，用于在旧主库自身的 Agent 不可达时
// 验证旧主库无法再接受写入。它打破了 pkg/logic 与
// pkg/manager/monitor 之间的导入循环：cmd/manager 将 monitor 已有
// 的探测方法接入本接口，而 logic 层只依赖这一抽象。
//
// 所有方法都受调用方 context/超时的约束，且必须支持并发安全使用。
type FenceProbers interface {
	// DirectDBReachable 在 Manager 侧对 node 的数据库端口发起
	// 数据库线协议探测（不涉及 Agent）。true = 有数据库应答。
	DirectDBReachable(node *datacenter.Node) bool

	// HostReachable 在 Manager 侧对节点主机发起 TCP 探测
	// （通常为 SSH/22）。true = 主机网络栈可达。
	HostReachable(node *datacenter.Node) bool

	// PeerAgentProbe 请求同集群内一个健康的对等 Agent，对旧主库
	// 的 targetPort 发起 TCP 探测（主机用 22，数据库用 DB 端口）。
	// reachable=true 表示该端口上「有东西应答」。
	// hasPeer 表示是否找到了可用的对等 Agent：false 意味着
	// 找不到可咨询的对等节点（调用方不得把 reachable=false
	// 当作旧主库已宕机的证据）。
	PeerAgentProbe(node *datacenter.Node, targetPort int32) (reachable bool, hasPeer bool)
}

// fenceProbers 保存进程级注入的实现。它可能为 nil
// （例如单元测试）；此时围栏层跳过 L2/L3，回退到
// 本地 Agent（L1）行为，保持安全的默认策略。
var (
	fenceProbersMu sync.RWMutex
	fenceProbers   FenceProbers
)

// SetFenceProbers 安装进程级的探测器（由 cmd/manager 调用）。
func SetFenceProbers(p FenceProbers) {
	fenceProbersMu.Lock()
	defer fenceProbersMu.Unlock()
	fenceProbers = p
}

// GetFenceProbers 返回注入的探测器；未安装时返回 nil。
func GetFenceProbers() FenceProbers {
	fenceProbersMu.RLock()
	defer fenceProbersMu.RUnlock()
	return fenceProbers
}

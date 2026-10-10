// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package monitor

import (
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logic"
)

// fenceProbers 将 Monitor 现有的探测方法适配到
// logic.FenceProbers 接口，使故障切换层能够带外验证旧主库，
// 而无需导入本包（否则会形成循环导入）。
type fenceProbers struct{ m *Monitor }

// NewFenceProbers 返回一个由本 Monitor 的直接 / 主机 / 对等节点
// 探测能力支撑的 logic.FenceProbers。
func NewFenceProbers(m *Monitor) logic.FenceProbers {
	return &fenceProbers{m: m}
}

func (a *fenceProbers) DirectDBReachable(node *datacenter.Node) bool {
	return a.m.directDBCheck(node)
}

func (a *fenceProbers) HostReachable(node *datacenter.Node) bool {
	return a.m.bypassCheckPing(node.Host)
}

func (a *fenceProbers) PeerAgentProbe(node *datacenter.Node, targetPort int32) (bool, bool) {
	// secondaryProbePort 通过 peerAvailable 区分“没有可用对等节点”
	// 与“对等节点存在但端口关闭”两种情况。
	return a.m.secondaryProbePort(node.Host, node.ClusterName, targetPort)
}

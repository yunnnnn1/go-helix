// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package monitor

import (
	"fmt"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// preflightNodeView 是回滚预览所使用的节点只读快照。
type preflightNodeView struct {
	Addr            string `json:"addr"`
	Role            string `json:"role"`
	Status          string `json:"status"`
	Upstream        string `json:"upstream"`
	ReceiverRunning bool   `json:"receiver_running"`
	ApplierRunning  bool   `json:"applier_running"`
	SecondsBehind   int64  `json:"seconds_behind"`
	GtidExecuted    string `json:"gtid_executed"`
	Present         bool   `json:"present"`
}

// PreflightCheck 表示一项具体的前置条件及其结果和供人阅读的详情。
type PreflightCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
	// Required 标记硬性阻断项。为 false 时该检查仅作信息参考：
	// 回滚本身会重新指向副本（STOP/CHANGE MASTER/START），
	// 并通过 WaitCatchup 验证收敛，因此线程已停止、复制源指向过期
	// 或当前存在延迟，都不应使回滚变得不可行。
	Required bool `json:"required"`
}

// RollbackPreflight 是回滚预览中渲染的结构化结果。
type RollbackPreflight struct {
	Cluster  string                   `json:"cluster"`
	Feasible bool                     `json:"feasible"`
	Record   *datacenter.SwitchRecord `json:"record"`
	Current  *preflightNodeView       `json:"current"`
	Restore  *preflightNodeView       `json:"restore"`
	Checks   []*PreflightCheck        `json:"checks"`
}

func nodeView(n *datacenter.Node) *preflightNodeView {
	if n == nil {
		return &preflightNodeView{Present: false}
	}
	upstream := ""
	if n.MasterHost != "" {
		upstream = fmt.Sprintf("%s:%d", n.MasterHost, n.MasterPort)
	}
	return &preflightNodeView{
		Addr:            n.Addr(),
		Role:            n.Role,
		Status:          string(n.Status),
		Upstream:        upstream,
		ReceiverRunning: n.ReceiverRunning,
		ApplierRunning:  n.ApplierRunning,
		SecondsBehind:   n.SecondsBehind,
		GtidExecuted:    n.GtidExecuted,
		Present:         true,
	}
}

// PreflightRollback 是只读操作：它定位最近一次成功的、改变拓扑的
// 切换，并验证切换前的主库能否无损地重新承担该角色。它不会改变任何状态。
func (m *Monitor) PreflightRollback(clusterName string) (*RollbackPreflight, error) {
	rec, err := m.dcClient.GetLatestSwitch(clusterName)
	if err != nil {
		return nil, fmt.Errorf("read switch history: %w", err)
	}

	nodes, err := m.dcClient.GetClusterNodes(clusterName)
	if err != nil {
		return nil, fmt.Errorf("get cluster nodes: %w", err)
	}

	current := findNode(nodes, rec.NewMaster)
	restore := findNode(nodes, rec.OldMaster)

	out := &RollbackPreflight{
		Cluster: clusterName,
		Record:  rec,
		Current: nodeView(current),
		Restore: nodeView(restore),
	}

	upstreamOK := restore != nil && current != nil &&
		restore.MasterHost == current.Host && restore.MasterPort == current.Port

	checks := []*PreflightCheck{
		{
			Name:     "存在可回滚的成功切换记录",
			OK:       rec != nil,
			Required: true,
			Detail: fmt.Sprintf("#%d %s @ %s：%s → %s", rec.ID, rec.Operation,
				rec.CreateTime.Format("2006-01-04 15:04:05"), rec.OldMaster, rec.NewMaster),
		},
		{
			Name:     "当前主库仍在集群中",
			OK:       current != nil,
			Required: true,
			Detail:   rec.NewMaster,
		},
		{
			Name:     "待恢复主库仍在集群中",
			OK:       restore != nil,
			Required: true,
			Detail:   rec.OldMaster,
		},
		{
			Name:     "待恢复节点当前角色为从库",
			OK:       restore != nil && restore.Role == "slave",
			Required: true,
			Detail:   roleOf(restore),
		},
		// 以下检查仅作信息参考。回滚会重新指向副本（停止 / 重启两个
		// 线程，并将复制源设置为当前主库），再通过 WaitCatchup 验证
		// 数据收敛，因此这些检查不得阻断回滚。
		{
			Name:     "待恢复节点复制源指向当前主库",
			OK:       upstreamOK,
			Required: false,
			Detail:   upstreamOf(restore) + "（不一致时回滚会自动 CHANGE MASTER 修正）",
		},
		{
			Name:     "复制接收线程运行中",
			OK:       restore != nil && restore.ReceiverRunning,
			Required: false,
			Detail:   threadDetail(restore, true),
		},
		{
			Name:     "复制重放线程运行中",
			OK:       restore != nil && restore.ApplierRunning,
			Required: false,
			Detail:   threadDetail(restore, false),
		},
		{
			Name:     "数据可追平（当前延迟）",
			OK:       restore != nil,
			Required: false,
			Detail:   behindDetail(restore) + "（回滚会等待追平后再提升）",
		},
	}

	out.Checks = checks
	feasible := true
	for _, c := range checks {
		// 只有 Required 检查才是硬性阻断项。
		if c.Required && !c.OK {
			feasible = false
			break
		}
	}
	out.Feasible = feasible
	return out, nil
}

func roleOf(n *datacenter.Node) string {
	if n == nil {
		return "节点不存在"
	}
	if n.Role == "" {
		return "未知角色"
	}
	return n.Role
}

func upstreamOf(n *datacenter.Node) string {
	if n == nil {
		return "节点不存在"
	}
	if n.MasterHost == "" {
		return "未配置复制源"
	}
	return fmt.Sprintf("%s:%d", n.MasterHost, n.MasterPort)
}

func threadDetail(n *datacenter.Node, io bool) string {
	if n == nil {
		return "节点不存在"
	}
	running := n.ApplierRunning
	if io {
		running = n.ReceiverRunning
	}
	if running {
		return "运行中"
	}
	return "已停止（回滚前可先用「恢复复制」修复）"
}

func behindDetail(n *datacenter.Node) string {
	if n == nil {
		return "节点不存在"
	}
	return fmt.Sprintf("%d 秒", n.SecondsBehind)
}

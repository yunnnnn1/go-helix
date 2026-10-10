// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logic

import (
	"context"
	"fmt"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logging"
)

// HandleRecoverReplica 修复单个副本到当前主库的链路。
func HandleRecoverReplica(ctx context.Context, master, target *datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	switcher, err := GetSwitcher(target.DbType)
	if err != nil {
		return err
	}
	fl.Printf("[Logic] HandleRecoverReplica cluster=%s target=%s master=%s",
		target.ClusterName, target.Addr(), master.Addr())
	return switcher.RecoverReplica(ctx, master, target)
}

// HandleRebuildTopology 将整个集群收敛到 desiredMaster。
func HandleRebuildTopology(ctx context.Context, desiredMaster *datacenter.Node, allNodes []*datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	switcher, err := GetSwitcher(desiredMaster.DbType)
	if err != nil {
		return err
	}
	fl.Printf("[Logic] HandleRebuildTopology cluster=%s master=%s nodes=%d",
		desiredMaster.ClusterName, desiredMaster.Addr(), len(allNodes))
	return switcher.RebuildTopology(ctx, desiredMaster, allNodes)
}

// HandleRollback 撤销最近一次改变拓扑的切换：将主库角色
// 从 currentMaster 移回 restoreMaster（该切换之前记录的主库）。
// 该操作以无损的计划内接管方式执行。
func HandleRollback(ctx context.Context, currentMaster, restoreMaster *datacenter.Node, allNodes []*datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)

	if restoreMaster == nil {
		return fmt.Errorf("no master to restore found in switch history")
	}
	if restoreMaster.Addr() == currentMaster.Addr() {
		return fmt.Errorf("master to restore (%s) equals current master; nothing to roll back", restoreMaster.Addr())
	}

	switcher, err := GetSwitcher(currentMaster.DbType)
	if err != nil {
		return err
	}

	// 其他节点需排除当前主库和被恢复的节点；
	// GracefulMasterTakeover 自身也会重新指向当前（旧）主库。
	var others []*datacenter.Node
	for _, n := range allNodes {
		if n.Addr() != currentMaster.Addr() && n.Addr() != restoreMaster.Addr() {
			others = append(others, n)
		}
	}

	fl.Printf("[Logic] HandleRollback cluster=%s current=%s restore=%s otherNodes=%d",
		currentMaster.ClusterName, currentMaster.Addr(), restoreMaster.Addr(), len(others))

	election := &ElectionResult{NewMaster: restoreMaster, Slaves: others}
	return switcher.GracefulMasterTakeover(ctx, currentMaster, election)
}

// HandleConvergeTopology 重新指向各节点，使集群与 edges 完全一致
// （回滚后精确恢复拓扑）。
func HandleConvergeTopology(ctx context.Context, allNodes []*datacenter.Node, edges []TopologyEdge) error {
	if len(allNodes) == 0 {
		return fmt.Errorf("converge topology: no nodes")
	}
	switcher, err := GetSwitcher(allNodes[0].DbType)
	if err != nil {
		return err
	}
	return switcher.RehomeToEdges(ctx, allNodes, edges)
}

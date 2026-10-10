// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logic

import (
	"context"
	"fmt"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logging"
)

// HandleFailure 是处理已确认节点故障的主入口。
func HandleFailure(ctx context.Context, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) error {
	return HandleFailureWithAnalysis(ctx, DeadMaster, failedNode, clusterNodes, preferredNode)
}

// SwitchResult 概括一次已完成的切换；目前仅包含新主库信息。
type SwitchResult struct {
	NewMasterAddr string
}

// HandleFailureWithAnalysisResult 执行切换流程并返回最终的
// 主库身份，便于调用方持久化结构化的切换历史。
func HandleFailureWithAnalysisResult(ctx context.Context, code AnalysisCode, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) (*SwitchResult, error) {
	fl := logging.FailoverLogger(ctx)

	// 计划内切换 -> 主动切换，其他所有 code -> 故障切换。
	opLabel := "Failover"
	if code == GracefulMasterTakeover {
		opLabel = "Switchover"
	}
	fl.Printf("[Logic] Handle%s code=%s cluster=%s node=%s", opLabel, code, failedNode.ClusterName, failedNode.Addr())

	switcher, err := GetSwitcher(failedNode.DbType)
	if err != nil {
		return nil, err
	}

	switch code {
	case NonWriteableMaster:
		fl.Printf("[Logic] NonWriteableMaster on %s — skipping switch, consider restoring write access", failedNode.Addr())
		return nil, nil

	case DeadIntermediateMaster:
		// 中间主恢复不会改变可写主库；
		// 仅提升一个下游副本接替中间主位置。
		if !config.EffectiveTopology(failedNode.ClusterName).IntermediateRecoverEnabled() {
			fl.Printf("[Logic] DeadIntermediateMaster on cluster %s %s — intermediate_master_recovery disabled, skipping",
				failedNode.ClusterName, failedNode.Addr())
			return nil, nil
		}
		fl.Printf("[Logic] DeadIntermediateMaster on %s — recovering intermediate slot", failedNode.Addr())
		if rErr := switcher.RecoverIntermediateMaster(ctx, failedNode, clusterNodes); rErr != nil {
			return nil, rErr
		}
		// 可写主库保持不变；回报其现有身份。
		return &SwitchResult{NewMasterAddr: currentWritableMaster(clusterNodes)}, nil

	case DeadMasterAndSlaves:
		fl.Printf("[Logic] DeadMasterAndSlaves in cluster %s — all slaves also down, attempting best-effort election", failedNode.ClusterName)

	case DeadMasterAndSomeSlaves:
		fl.Printf("[Logic] DeadMasterAndSomeSlaves in cluster %s — electing from surviving slaves", failedNode.ClusterName)

	case LockedSemiSyncMaster:
		fl.Printf("[Logic] LockedSemiSyncMaster on %s — semi-sync stuck with no ack-capable slaves, initiating emergency failover", failedNode.Addr())

	case UnreachableMaster:
		fl.Printf("[Logic] UnreachableMaster on %s — host unreachable from all angles, initiating failover", failedNode.Addr())
	}

	electTimeout := 20 * time.Second
	if cfg := config.Get(); cfg != nil {
		electTimeout = config.ParseDuration(cfg.Manager.ElectionTimeout, electTimeout)
	}

	electCtx, cancel := context.WithTimeout(ctx, electTimeout)
	defer cancel()

	election, err := switcher.Elect(electCtx, failedNode, clusterNodes, preferredNode)
	if err != nil {
		return nil, err
	}
	if election == nil || election.NewMaster == nil {
		return nil, fmt.Errorf("election returned no new master for cluster %s", failedNode.ClusterName)
	}

	var execErr error
	if code == GracefulMasterTakeover {
		execErr = switcher.GracefulMasterTakeover(ctx, failedNode, election)
	} else {
		execErr = switcher.Failover(ctx, failedNode, election)
	}
	if execErr != nil {
		return nil, execErr
	}
	return &SwitchResult{NewMasterAddr: election.NewMaster.Addr()}, nil
}

// currentWritableMaster 返回被标记为主库的节点地址；
// 找不到时返回空字符串。供保持可写主库不变的中间主恢复使用。
func currentWritableMaster(nodes []*datacenter.Node) string {
	for _, n := range nodes {
		if n.Role == "master" {
			return n.Addr()
		}
	}
	return ""
}

// HandleFailureWithAnalysis 保留原先仅返回 error 的入口。
func HandleFailureWithAnalysis(ctx context.Context, code AnalysisCode, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) error {
	_, err := HandleFailureWithAnalysisResult(ctx, code, failedNode, clusterNodes, preferredNode)
	return err
}

// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package monitor

import (
	"context"
	"fmt"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/notify"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

// 恢复模式，与请求载荷中的取值一一对应。
const (
	RecoverReplica  = "replica"  // 修复单个副本的复制链路
	RecoverTopology = "topology" // 使整个集群收敛到一个主库
	RecoverRollback = "rollback" // 撤销最近一次切换
)

// findNode 按精确的 host:port 或 host 定位节点，解析方式与
// TriggerManualFailover 相同。
func findNode(nodes []*datacenter.Node, spec string) *datacenter.Node {
	if spec == "" {
		return nil
	}
	for _, n := range nodes {
		if spec == n.Addr() || spec == n.Host {
			return n
		}
	}
	return nil
}

// TriggerRecover 校验输入并异步启动所请求的恢复
// （执行模型与手动故障切换相同）。
func (m *Monitor) TriggerRecover(clusterName, mode, targetNode string) error {
	if !m.raftNode.IsLeader() {
		return fmt.Errorf("not raft leader")
	}
	switch mode {
	case RecoverReplica, RecoverTopology, RecoverRollback:
	default:
		return fmt.Errorf("unknown recover mode %q", mode)
	}

	nodes, err := m.dcClient.GetClusterNodes(clusterName)
	if err != nil {
		return fmt.Errorf("get nodes for %s: %w", clusterName, err)
	}

	var master *datacenter.Node
	for _, n := range nodes {
		if n.Role == "master" {
			master = n
		}
	}

	go m.runRecover(clusterName, mode, targetNode, nodes, master)
	return nil
}

// recoverBannerName 将恢复模式映射为控制台所跟踪的
// Start/End 横幅中使用的首字母大写单词。
func recoverBannerName(mode string) string {
	switch mode {
	case RecoverReplica:
		return "Replica"
	case RecoverTopology:
		return "Topology"
	case RecoverRollback:
		return "Rollback"
	default:
		return "Recover"
	}
}

// runRecover 使用专用的恢复日志执行一次恢复，持久化一条
// switch_history 记录，并写入控制台所跟踪的 Start/End 标记。
func (m *Monitor) runRecover(clusterName, mode, targetSpec string, nodes []*datacenter.Node, master *datacenter.Node) {
	cfg := config.Get()
	bannerName := recoverBannerName(mode)

	fl := log.Default()
	if cfg != nil && cfg.Manager.LogDir != "" {
		fl = log.NewOperationLogger(cfg.Manager.LogDir, clusterName, log.KindForRecover(mode))
	}

	fl.Printf("========== %s Start ==========", bannerName)
	fl.Printf("Recover mode=%s cluster=%s target=%s", mode, clusterName, targetSpec)
	log.Printf("Recover triggered mode=%s cluster=%s target=%s", mode, clusterName, targetSpec)

	// 通知恢复流程已开始（离散生命周期事件，SendImmediate 不节流）。
	targetLabel := targetSpec
	if targetLabel == "" {
		targetLabel = "默认"
	}
	notify.SendImmediate(&notify.Event{
		Title:    fmt.Sprintf("%s恢复开始：%s", bannerName, clusterName),
		Severity: notify.SeverityInfo,
		Cluster:  clusterName,
		Message:  fmt.Sprintf("%s 恢复开始执行（mode=%s，目标 %s）。", bannerName, mode, targetLabel),
	})

	// 审计：恢复生命周期开始（离散事件，由 Leader 提交）。
	m.raftNode.EmitAudit(&raft.AuditEntry{
		Category: raft.AuditLifecycle,
		Action:   "recover." + mode + ".start",
		Cluster:  clusterName,
		Target:   targetSpec,
		Result:   "started",
		Node:     m.raftNode.RaftID,
		Detail:   "mode=" + mode,
	})

	m.ResetSilence(clusterName)
	ctx := log.WithFailoverLogger(context.Background(), fl)

	// 切换前后的主库身份，用于写入 switch_history。
	oldMasterAddr := ""
	if master != nil {
		oldMasterAddr = master.Addr()
	}
	newMasterAddr := oldMasterAddr

	var err error
	switch mode {
	case RecoverReplica:
		target := findNode(nodes, targetSpec)
		switch {
		case target == nil:
			err = fmt.Errorf("target node %q not found in cluster %s", targetSpec, clusterName)
		case master == nil:
			err = fmt.Errorf("no current master; cannot repair replica (use topology rebuild)")
		default:
			err = logic.HandleRecoverReplica(ctx, master, target)
		}

	case RecoverTopology:
		desired := findNode(nodes, targetSpec)
		if desired == nil {
			if master == nil {
				err = fmt.Errorf("no target specified and no current master for cluster %s", clusterName)
				break
			}
			desired = master
			fl.Printf("No target specified; using current master %s", desired.Addr())
		}
		if err = logic.HandleRebuildTopology(ctx, desired, nodes); err == nil {
			newMasterAddr = desired.Addr()
		}

	case RecoverRollback:
		rec, hErr := m.dcClient.GetLatestSwitch(clusterName)
		if hErr != nil {
			err = fmt.Errorf("read switch history for rollback: %w", hErr)
			break
		}
		fl.Printf("Rollback reversing record id=%d (%s) %s -> %s at %s",
			rec.ID, rec.Operation, rec.NewMaster, rec.OldMaster,
			rec.CreateTime.Format("2006-01-04 15:04:05"))

		current := findNode(nodes, rec.NewMaster)
		restore := findNode(nodes, rec.OldMaster)
		switch {
		case current == nil:
			err = fmt.Errorf("current master %s from history not present in cluster nodes", rec.NewMaster)
		case restore == nil:
			err = fmt.Errorf("master to restore %s is absent/removed from cluster", rec.OldMaster)
		default:
			if err = logic.HandleRollback(ctx, current, restore, nodes); err == nil {
				oldMasterAddr = current.Addr()
				newMasterAddr = restore.Addr()

				// 精确恢复拓扑：将各节点重新指向被撤销切换之前
				// 保存的边集合。快照缺失或不可读并非致命错误
				// （主库身份已经无损恢复）；告警并继续。
				if edgesJSON, snapErr := m.dcClient.GetTopologySnapshot(clusterName); snapErr != nil {
					fl.Printf("Warning: rollback topology snapshot unavailable (%v); master identity restored, exact shape not applied", snapErr)
				} else if edges, pErr := parseTopologyEdges(edgesJSON); pErr != nil {
					fl.Printf("Warning: parse rollback topology snapshot failed: %v", pErr)
				} else if cErr := logic.HandleConvergeTopology(ctx, nodes, edges); cErr != nil {
					fl.Printf("Warning: apply rollback topology edges failed: %v", cErr)
				} else {
					fl.Printf("Exact pre-switch topology restored (%d edge(s))", len(edges))
				}
			}
		}
	}

	result := "SUCCESS"
	if err != nil {
		result = "FAILED"
		fl.Printf("Recover sequence failed for %s: %v", clusterName, err)
		fl.Printf("========== %s End (FAILED) ==========", bannerName)
		log.Printf("Recover sequence failed for %s: %v", clusterName, err)
	} else {
		fl.Printf("Recover sequence completed for %s.", clusterName)
		fl.Printf("========== %s End (SUCCESS) ==========", bannerName)
		log.Printf("Recover sequence completed for %s.", clusterName)
	}

	// 通知恢复结果（离散生命周期事件，绝不节流）：失败 critical 需人工介入。
	if err != nil {
		notify.SendImmediate(&notify.Event{
			Title:    fmt.Sprintf("%s恢复失败：%s", bannerName, clusterName),
			Severity: notify.SeverityCritical,
			Cluster:  clusterName,
			Message:  fmt.Sprintf("%s 恢复失败（mode=%s）：%v，请人工介入。", bannerName, mode, err),
		})
	} else {
		notify.SendImmediate(&notify.Event{
			Title:    fmt.Sprintf("%s恢复完成：%s", bannerName, clusterName),
			Severity: notify.SeverityInfo,
			Cluster:  clusterName,
			Message:  fmt.Sprintf("%s 恢复已完成（%s → %s）。", bannerName, oldMasterAddr, newMasterAddr),
		})
	}

	rec := &datacenter.SwitchRecord{
		ClusterName: clusterName,
		Operation:   "recover",
		RecoverMode: mode,
		OldMaster:   oldMasterAddr,
		NewMaster:   newMasterAddr,
		Result:      result,
	}
	if hErr := m.dcClient.InsertSwitchHistory(rec); hErr != nil {
		fl.Printf("Warning: failed to persist switch history: %v", hErr)
	}

	// 审计：恢复生命周期结束。
	endResult := "success"
	endDetail := oldMasterAddr + " -> " + newMasterAddr
	if err != nil {
		endResult = "failed"
		endDetail = "error=" + err.Error()
	}
	m.raftNode.EmitAudit(&raft.AuditEntry{
		Category: raft.AuditLifecycle,
		Action:   "recover." + mode + ".end",
		Cluster:  clusterName,
		Target:   targetSpec,
		Result:   endResult,
		Node:     m.raftNode.RaftID,
		Detail:   endDetail,
	})

	// 镜像同一条结构化记录到 Raft，供控制面高可用历史查询。
	m.mirrorSwitchToRaft(rec)
}

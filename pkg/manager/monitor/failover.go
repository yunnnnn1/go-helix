package monitor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/notify"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

// HandleConfirmedFailure 是对 logic 层入口的封装。
func (m *Monitor) HandleConfirmedFailure(failedNode *datacenter.Node, preferredNode *datacenter.Node) {
	m.HandleConfirmedFailureWithCode(logic.DeadMaster, failedNode, preferredNode)
}

// HandleConfirmedFailureWithCode 使用指定的分析码触发切换流程。
// code == GracefulMasterTakeover 表示计划内主动切换；
// 其他所有 code 均按紧急故障切换处理。
func (m *Monitor) HandleConfirmedFailureWithCode(code logic.AnalysisCode, failedNode *datacenter.Node, preferredNode *datacenter.Node) {
	// 纵深防御：每一次已确认的切换都只能在 Leader 上执行。
	// 调用方（由 Leader 入队的 worker、TriggerManualFailover）已经
	// 保证了这一点，但绝不能在 Follower 上执行切换。
	if !m.raftNode.IsLeader() {
		log.Printf("Skip switch for %s: node is not raft leader", failedNode.ClusterName)
		return
	}

	// 唯一的静默闸门。此前静默只在读取失联主库时检查，导致网络探测
	// 抖动的 worker 可能在每个节拍重新进入切换流程并绕过静默——在数秒
	// 内产生多次故障切换。所有切换路径都会汇集到此处，因此在这里强制
	// 执行检查。计划内（GracefulMasterTakeover）切换属于操作人员的明确
	// 动作，允许放行。
	if code != logic.GracefulMasterTakeover {
		m.mu.Lock()
		until, silenced := m.silencedClusters[failedNode.ClusterName]
		active := silenced && time.Now().Before(until)
		if silenced && !active {
			delete(m.silencedClusters, failedNode.ClusterName)
		}
		m.mu.Unlock()
		if active {
			log.Printf("Skip switch for %s: cluster within silence period (until %s)",
				failedNode.ClusterName, until.Format("15:04:05"))
			return
		}
	}

	cfg := config.Get()

	// 术语与物理日志位置：计划内 -> Switchover/switchover，
	// 其他情况 -> Failover/failover。
	label := "Failover"
	kind := log.KindFailover
	if code == logic.GracefulMasterTakeover {
		label = "Switchover"
		kind = log.KindSwitchover
	}

	// 创建与 manager 主日志相互隔离的集群专属操作日志记录器
	fl := log.Default()
	if cfg != nil && cfg.Manager.LogDir != "" {
		fl = log.NewOperationLogger(cfg.Manager.LogDir, failedNode.ClusterName, kind)
	}

	nodeAddr := failedNode.Addr()
	log.Printf("Triggering %s code=%s cluster=%s failedNode=%s", label, code, failedNode.ClusterName, nodeAddr)
	fl.Printf("========== %s Start ==========", label)
	fl.Printf("Triggering %s code=%s cluster=%s failedNode=%s", label, code, failedNode.ClusterName, nodeAddr)

	clusterNodes, err := m.dcClient.GetClusterNodes(failedNode.ClusterName)
	if err != nil {
		fl.Printf("Failed to get cluster nodes: %v", err)
		log.Printf("Failed to get cluster nodes for %s: %v", failedNode.ClusterName, err)
		return
	}

	ctx := log.WithFailoverLogger(context.Background(), fl)

	// 读取 cluster_meta 中存储的权威 VIP/域名，并将其带入切换流程，
	// 使 hook 能拿到持久化的端点（而不是要求在 manager 配置文件中
	// 再重复配置一份）。
	if meta, mErr := m.dcClient.GetClusterMeta(failedNode.ClusterName); mErr == nil {
		if logic.HasRealEndpoint(meta.EndpointType, meta.EndpointValue) {
			ctx = logic.WithClusterEndpoint(ctx, meta.EndpointType, meta.EndpointValue)
			fl.Printf("[Endpoint] loaded stored endpoint from cluster_meta: %s=%s",
				meta.EndpointType, meta.EndpointValue)
		}
	}

	// 持久化切换前完整的复制边集合，以便后续回滚时恢复确切的
	// 拓扑形态（而不是扁平布局）。
	if edgesJSON, snapErr := topologyEdgesJSON(clusterNodes); snapErr != nil {
		fl.Printf("Warning: build topology snapshot failed: %v", snapErr)
	} else if sErr := m.dcClient.SaveTopologySnapshot(failedNode.ClusterName, edgesJSON); sErr != nil {
		fl.Printf("Warning: persist topology snapshot failed: %v", sErr)
	}

	// 告警提示切换流程已真正开始执行（此时集群节点与切换前快照均已
	// 处理完毕）。计划内主动切换为 info 级别；所有紧急 code 均为
	// critical。SendImmediate 可避免这些离散的生命周期事件被
	// 重复发送节流。
	notify.SendImmediate(&notify.Event{
		Title:    fmt.Sprintf("%s开始：%s", label, failedNode.ClusterName),
		Severity: map[bool]notify.Severity{true: notify.SeverityInfo, false: notify.SeverityCritical}[code == logic.GracefulMasterTakeover],
		Cluster:  failedNode.ClusterName,
		Code:     string(code),
		Message:  fmt.Sprintf("节点 %s 的 %s 流程开始执行。", nodeAddr, label),
	})

	// 审计：切换生命周期开始（离散事件，由 Leader 提交）。
	m.raftNode.EmitAudit(&raft.AuditEntry{
		Category: raft.AuditLifecycle,
		Action:   kind + ".start",
		Cluster:  failedNode.ClusterName,
		Target:   nodeAddr,
		Result:   "started",
		Node:     m.raftNode.RaftID,
		Detail:   "code=" + string(code),
	})

	res, seqErr := logic.HandleFailureWithAnalysisResult(ctx, code, failedNode, clusterNodes, preferredNode)

	opName := "failover"
	if code == logic.GracefulMasterTakeover {
		opName = "switchover"
	}
	newMasterAddr := ""
	if res != nil {
		newMasterAddr = res.NewMasterAddr
	}

	resultStr := "SUCCESS"
	resultLabel := "success"
	if seqErr != nil {
		resultStr = "FAILED"
		// 围栏中止是另一类结果（未尝试提升）；通过错误文本识别，
		// 便于运维人员将脑裂中止与普通失败区分开来。
		resultLabel = "failed"
		if strings.Contains(seqErr.Error(), "fencing required") {
			resultLabel = "aborted"
		}
		failoverCounter.Inc(resultLabel)
		fl.Printf("%s sequence failed for %s on %s: %v", label, failedNode.ClusterName, nodeAddr, seqErr)
		fl.Printf("========== %s End (FAILED) ==========", label)
		log.Printf("%s sequence failed for %s on %s: %v", label, failedNode.ClusterName, nodeAddr, seqErr)
	} else {
		failoverCounter.Inc(resultLabel)
		fl.Printf("%s sequence completed for %s.", label, failedNode.ClusterName)
		fl.Printf("========== %s End (SUCCESS) ==========", label)
		log.Printf("%s sequence completed for %s.", label, failedNode.ClusterName)
		silence := config.ParseDuration(cfg.Manager.SilencePeriod, 5*time.Minute)
		m.mu.Lock()
		m.silencedClusters[failedNode.ClusterName] = time.Now().Add(silence)
		m.mu.Unlock()
		log.Printf("Cluster %s entered silence period for %s.", failedNode.ClusterName, silence)
	}

	// 告警通知切换结果（离散生命周期事件，绝不节流）。
	if seqErr != nil {
		notify.SendImmediate(&notify.Event{
			Title:    fmt.Sprintf("%s失败：%s", label, failedNode.ClusterName),
			Severity: notify.SeverityCritical,
			Cluster:  failedNode.ClusterName,
			Code:     string(code),
			Message:  fmt.Sprintf("节点 %s 的 %s 流程失败：%v，请人工介入。", nodeAddr, label, seqErr),
		})
	} else {
		notify.SendImmediate(&notify.Event{
			Title:    fmt.Sprintf("%s完成：%s", label, failedNode.ClusterName),
			Severity: notify.SeverityInfo,
			Cluster:  failedNode.ClusterName,
			Code:     string(code),
			Message:  fmt.Sprintf("%s 已完成，切换后写入节点：%s（原节点 %s）。", label, newMasterAddr, nodeAddr),
		})
	}

	// 持久化结构化的切换历史（尽力而为）；后续回滚读取的正是这些
	// 结构化数据，而不是去解析格式自由的日志文本。
	rec := &datacenter.SwitchRecord{
		ClusterName:  failedNode.ClusterName,
		Operation:    opName,
		OldMaster:    failedNode.Addr(),
		NewMaster:    newMasterAddr,
		AnalysisCode: string(code),
		Result:       resultStr,
	}
	if hErr := m.dcClient.InsertSwitchHistory(rec); hErr != nil {
		fl.Printf("Warning: failed to persist switch history: %v", hErr)
	}

	// 审计：切换生命周期结束。resultLabel 已区分 success / failed / aborted。
	endDetail := ""
	switch {
	case seqErr != nil:
		endDetail = "error=" + seqErr.Error()
	case newMasterAddr != "":
		endDetail = "new_master=" + newMasterAddr
	}
	m.raftNode.EmitAudit(&raft.AuditEntry{
		Category: raft.AuditLifecycle,
		Action:   kind + ".end",
		Cluster:  failedNode.ClusterName,
		Target:   nodeAddr,
		Result:   resultLabel,
		Node:     m.raftNode.RaftID,
		Detail:   endDetail,
	})

	// 镜像同一条结构化记录到 Raft，供控制面高可用历史查询。
	m.mirrorSwitchToRaft(rec)
}

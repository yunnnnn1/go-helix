// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package monitor

import (
	"fmt"
	"sync"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/notify"

	// 导入 provider 以完成注册
	_ "github.com/yunnnnn1/go-helix/pkg/db/ops/mysql"
	_ "github.com/yunnnnn1/go-helix/pkg/logic/mysql"

	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/raft"

	"google.golang.org/grpc"
)

// investigationJob 将一个节点及其初始分析码携带到 worker 池中。
type investigationJob struct {
	Node        *datacenter.Node
	InitialCode logic.AnalysisCode
}

// GetClusterNodes 通过委托 datacenter 客户端返回指定集群的全部节点
func (m *Monitor) GetClusterNodes(clusterName string) ([]*datacenter.Node, error) {
	return m.dcClient.GetClusterNodes(clusterName)
}

// GetClusters 返回所有受管集群的聚合概览。当本 Manager 集群的层级为
// P0 时，仅返回已注册的 P0 集群。
func (m *Monitor) GetClusters() ([]*datacenter.ClusterSummary, error) {
	summaries, err := m.dcClient.GetClusters()
	if err != nil {
		return nil, err
	}
	return filterByTier(summaries, m.Tier()), nil
}

// Tier 返回本 Manager 集群的服务层级（P0..P3）。
func (m *Monitor) Tier() string {
	t, err := config.ManagerTier()
	if err != nil {
		return ""
	}
	return t
}

// filterByTier 按层级执行专用过滤。P0 仅保留已注册的 P0
// 集群；其他层级保留全部集群（维持现状）。
func filterByTier(summaries []*datacenter.ClusterSummary, tier string) []*datacenter.ClusterSummary {
	if tier != "P0" {
		return summaries
	}
	out := summaries[:0]
	for _, s := range summaries {
		if s.Registered && s.ClusterPriority == datacenter.PriorityP0 {
			out = append(out, s)
		}
	}
	return out
}

// managesCluster 报告本 Manager 集群是否可以管理指定集群。
// 对于 P0 层级，该集群必须是已注册的 P0 集群；其他层级接受任意集群。
func (m *Monitor) managesCluster(cluster string) bool {
	if m.Tier() != "P0" {
		return true
	}
	meta, err := m.dcClient.GetClusterMeta(cluster)
	if err != nil {
		return false
	}
	return meta.Registered && meta.ClusterPriority == datacenter.PriorityP0
}

// GetClusterMeta 返回单个集群的集群级属性。
func (m *Monitor) GetClusterMeta(cluster string) (datacenter.ClusterMeta, error) {
	return m.dcClient.GetClusterMeta(cluster)
}

// SetClusterPriority 更新插入（upsert）单个集群的优先级。
func (m *Monitor) SetClusterPriority(cluster string, p datacenter.ClusterPriority) error {
	return m.dcClient.SetClusterPriority(cluster, p)
}

// RegisterCluster 更新插入（upsert）一条已注册的 cluster_meta 记录。
func (m *Monitor) RegisterCluster(meta datacenter.ClusterMeta) error {
	return m.dcClient.RegisterCluster(meta)
}

// DefaultWorkPoolSize 定义并发调查 worker 数的兜底值
const DefaultWorkPoolSize = 10

// conflictDecayAfter 限定冲突历史的保留时长。超过该时段没有新冲突时，
// 计数会重置，因此一个反复触发瞬时冲突（Agent 健康）的主机永远不会被
// 永久跳过。
const conflictDecayAfter = 5 * time.Minute

// conflictState 记录某台主机的 DC 状态与 Agent 视图发生冲突的轮数，
// 以及最近一次冲突的时间（用于衰减）。
type conflictState struct {
	count  int
	lastAt time.Time
}

// isStale 报告该状态是否已经衰减过期。
func (s *conflictState) isStale(now time.Time) bool {
	return now.Sub(s.lastAt) > conflictDecayAfter
}

// conflictCount 返回指定主机当前有效的冲突计数。已衰减的记录会被清除
// 并按零上报，从而重新开启对该主机的调查。
func (m *Monitor) conflictCount(host string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.conflictCounts[host]
	if !ok {
		return 0
	}
	if st.isStale(time.Now()) {
		delete(m.conflictCounts, host)
		log.Printf("Conflict count for %s decayed to 0 after %s without a new conflict", host, conflictDecayAfter)
		return 0
	}
	return st.count
}

// recordConflict 将指定主机的冲突计数加一；若上一条记录已衰减，
// 则重新开始计数。
func (m *Monitor) recordConflict(host string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.conflictCounts[host]
	if !ok || st.isStale(time.Now()) {
		st = &conflictState{}
	}
	st.count++
	st.lastAt = time.Now()
	m.conflictCounts[host] = st
}

// resetConflict 清除指定主机的冲突计数（状态已变化 / 已确认故障）。
func (m *Monitor) resetConflict(host string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conflictCounts, host)
}

// bumpFailoverConfirmation 再记录一个怀疑主库已宕机的连续绕过（bypass）
// 节拍，并返回新的累计值。
func (m *Monitor) bumpFailoverConfirmation(host string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failoverConfirmations[host]++
	return m.failoverConfirmations[host]
}

// resetFailoverConfirmations 在切换已被执行（或节点已恢复）后，
// 清除确认计数器。
func (m *Monitor) resetFailoverConfirmations(host string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failoverConfirmations, host)
}

type Monitor struct {
	dcClient *datacenter.Client
	raftNode *raft.Node
	stopCh   chan struct{}

	mu               sync.Mutex
	conflictCounts   map[string]*conflictState
	silencedClusters map[string]time.Time // ClusterName -> SilenceUntil（集群名 -> 静默截止时间）

	// failoverConfirmations 统计绕过检查连续判定主库不可达 / 未按数据库
	// 协议通信的节拍数。它与 conflictCounts（在冲突过多时抑制调查）相互
	// 独立，因为两者含义恰好相反：只有这里的计数达到阈值，才最终允许
	// 执行切换。
	failoverConfirmations map[string]int

	// Worker 池
	jobCh chan *investigationJob

	// 连接缓存
	grpcConns sync.Map
}

func NewMonitor(raftNode *raft.Node, dcClient *datacenter.Client) *Monitor {
	m := &Monitor{
		dcClient:              dcClient,
		raftNode:              raftNode,
		stopCh:                make(chan struct{}),
		conflictCounts:        make(map[string]*conflictState),
		silencedClusters:      make(map[string]time.Time),
		failoverConfirmations: make(map[string]int),
		jobCh:                 make(chan *investigationJob, 1000),
	}
	return m
}

// mirrorSwitchToRaft 将一条已写入元数据库 switch_history 的结构化切换记录
// 镜像到 Raft，使 Manager 控制面在外部元数据库不可用时仍保留该历史。
// 该镜像为尽力而为：任何失败都只记录日志，绝不影响主流程。
func (m *Monitor) mirrorSwitchToRaft(rec *datacenter.SwitchRecord) {
	if rec == nil {
		return
	}
	r := &raft.SwitchRecord{
		Time:         rec.CreateTime,
		Cluster:      rec.ClusterName,
		Operation:    rec.Operation,
		RecoverMode:  rec.RecoverMode,
		OldMaster:    rec.OldMaster,
		NewMaster:    rec.NewMaster,
		AnalysisCode: rec.AnalysisCode,
		Result:       rec.Result,
		Node:         m.raftNode.RaftID,
	}
	// INSERT 不回填 CreateTime，缺失时以当前时间作为镜像时间。
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	if err := m.raftNode.AppendSwitch(r); err != nil {
		log.Printf("[audit] mirror switch record cluster=%s op=%s to raft failed: %v",
			rec.ClusterName, rec.Operation, err)
	}
}

func (m *Monitor) Start() {
	// 确定 Worker 池大小
	poolSize := DefaultWorkPoolSize
	if cfg := config.Get(); cfg != nil && cfg.Monitor.WorkerPoolSize > 0 {
		poolSize = cfg.Monitor.WorkerPoolSize
	}

	// 启动 Worker 池
	log.Printf("Starting Monitor with %d concurrent investigation workers", poolSize)
	for i := 0; i < poolSize; i++ {
		go m.worker(i)
	}
	go m.loop()
}

func (m *Monitor) Stop() {
	close(m.stopCh)

	// 关闭所有缓存的连接
	m.grpcConns.Range(func(key, value interface{}) bool {
		conn := value.(*grpc.ClientConn)
		conn.Close()
		return true
	})
}

func (m *Monitor) loop() {
	// 从配置读取初始间隔，否则使用默认值
	interval := 3 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Monitor.Interval != "" {
		if d, err := time.ParseDuration(cfg.Monitor.Interval); err == nil {
			interval = d
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 周期性检查配置更新
	// 由于不借助额外循环或复杂 select 就很难重置 ticker，
	// 我们直接在每个节拍上检查配置。如果间隔变化明显，可能需要重置 ticker。
	// 但在 select 中重置 ticker 比较棘手。
	// 更简单的间隔热更新方式是使用很短的节拍（如 1s）并检查已经过时间，
	// 或者在配置变化时直接重建 ticker。

	// 这里采用动态方案：每次循环都检查配置。

	lastInterval := interval

	for {
		select {
		case <-ticker.C:
			// 检查配置是否变化
			if cfg := config.Get(); cfg != nil && cfg.Monitor.Interval != "" {
				if d, err := time.ParseDuration(cfg.Monitor.Interval); err == nil {
					if d != lastInterval {
						log.Printf("Monitor interval changed from %v to %v", lastInterval, d)
						ticker.Reset(d)
						lastInterval = d
					}
				}
			}

			if m.raftNode.IsLeader() {
				m.checkFailedNodes()
			}
		case <-m.stopCh:
			return
		}
	}
}

func (m *Monitor) checkFailedNodes() {
	// 从 DC 获取失联的主库节点（心跳超过超时时间）
	timeout := 60 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Manager.StaleTimeout != "" {
		if d, err := time.ParseDuration(cfg.Manager.StaleTimeout); err == nil {
			timeout = d
		}
	}

	nodes, err := m.dcClient.GetStaleMasterNodes(timeout)
	if err != nil {
		log.Printf("Error getting stale master nodes: %v", err)
		return
	}

	// 主库自身失联的集群：本轮推迟中间主恢复，因为主库故障切换
	// （保留层级结构）优先级更高，否则会与中间主恢复发生竞争。
	staleMasterClusters := make(map[string]bool)

	// 按集群分组
	clusterStaleNodes := make(map[string][]*datacenter.Node)
	for _, node := range nodes {
		clusterStaleNodes[node.ClusterName] = append(clusterStaleNodes[node.ClusterName], node)
		staleMasterClusters[node.ClusterName] = true
	}

	// 逐个处理集群
	for clusterName, staleList := range clusterStaleNodes {
		// 层级专用：跳过本 Manager 集群无权管理的集群。
		if !m.managesCluster(clusterName) {
			continue
		}

		var masterNode *datacenter.Node
		var slaveNodes []*datacenter.Node

		for _, node := range staleList {
			if node.Role == "master" {
				masterNode = node
			} else {
				slaveNodes = append(slaveNodes, node)
			}
		}

		m.mu.Lock()
		// 检查静默状态
		if until, ok := m.silencedClusters[clusterName]; ok {
			if time.Now().Before(until) {
				m.mu.Unlock()
				continue
			}
			delete(m.silencedClusters, clusterName) // 已过期
		}
		m.mu.Unlock()

		// 1. 若主库失联 -> 探测主库（发起调查）
		if masterNode != nil {
			count := m.conflictCount(masterNode.Host)

			if count >= requiredFailoverConfirmations {
				log.Printf("Cluster %s Master %s stale, but reached retry limit (%d). Skipping.", clusterName, masterNode.Host, count)
				continue
			}

			// 根据本集群中从库的健康状况确定初始 AnalysisCode
			code := logic.DeadMaster
			allNodes, err := m.dcClient.GetClusterNodes(clusterName)
			if err == nil {
				totalSlaves := 0
				for _, n := range allNodes {
					if n.Role == "slave" {
						totalSlaves++
					}
				}
				staleSlaveCount := len(slaveNodes)
				if totalSlaves > 0 && staleSlaveCount >= totalSlaves {
					code = logic.DeadMasterAndSlaves
					log.Printf("Cluster %s: Master AND all %d slaves are stale → DeadMasterAndSlaves", clusterName, totalSlaves)
				} else if staleSlaveCount > 0 {
					code = logic.DeadMasterAndSomeSlaves
					log.Printf("Cluster %s: Master AND %d/%d slaves are stale → DeadMasterAndSomeSlaves", clusterName, staleSlaveCount, totalSlaves)
				}
			}

			// 送入 Worker 池（非阻塞，防止停止时发生死锁）
			select {
			case m.jobCh <- &investigationJob{Node: masterNode, InitialCode: code}:
			case <-m.stopCh:
				return
			}
		} else {
			// 2. 仅从库失联 -> 只告警
			for _, slave := range slaveNodes {
				log.Printf("[ALERT] Slave Node %s:%d (Cluster: %s) is stale! UpdateTime: %v",
					slave.Host, slave.Port, slave.ClusterName, slave.UpdateTime)
				notify.Send(&notify.Event{
					Title:    "从节点心跳过期",
					Severity: notify.SeverityWarning,
					Cluster:  slave.ClusterName,
					Code:     "StaleSlave",
					Message: fmt.Sprintf("从节点 %s 心跳过期，最近更新时间 %v，请确认实例与复制状态。",
						slave.Addr(), slave.UpdateTime),
				})
			}
		}
	}

	// 3. 在失联从库中检测已死亡的中间主（主库健康）。
	m.checkStaleIntermediateMasters(timeout, staleMasterClusters)
}

// checkStaleIntermediateMasters 扫描失联从库节点，对于自身带有下游副本
// （即中间主）的节点逐一加入调查队列；普通的失联叶子从库仅作告警。
// 主库自身失联的集群会被跳过（主库故障切换优先）。
func (m *Monitor) checkStaleIntermediateMasters(timeout time.Duration, staleMasterClusters map[string]bool) {
	staleSlaves, err := m.dcClient.GetStaleSlaveNodes(timeout)
	if err != nil {
		log.Printf("Error getting stale slave nodes: %v", err)
		return
	}

	// 每个集群只构建一次拓扑，以便检测下游边。
	clusterGraph := make(map[string]*logic.Topology)
	getGraph := func(cluster string) *logic.Topology {
		if g, ok := clusterGraph[cluster]; ok {
			return g
		}
		allNodes, err := m.dcClient.GetClusterNodes(cluster)
		if err != nil {
			return nil
		}
		g := logic.BuildTopology(allNodes)
		clusterGraph[cluster] = g
		return g
	}

	for _, slave := range staleSlaves {
		cluster := slave.ClusterName

		if staleMasterClusters[cluster] {
			continue // 主库故障切换优先
		}

		// 集群静默同样适用于中间主恢复。
		if m.clusterSilenced(cluster) {
			continue
		}

		graph := getGraph(cluster)
		if graph == nil {
			continue
		}

		// 失联节点必须是带有下游副本的从库，才有资格被判定为
		// 已死亡的中间主。
		if !graph.IsIntermediateMaster(slave) {
			log.Printf("[ALERT] Slave Node %s (Cluster: %s) is stale but has no downstream replicas; leaf slave only",
				slave.Addr(), cluster)
			notify.Send(&notify.Event{
				Title:    "从节点心跳过期（无下游）",
				Severity: notify.SeverityWarning,
				Cluster:  cluster,
				Code:     "StaleSlaveLeaf",
				Message: fmt.Sprintf("从节点 %s 心跳过期且无下游副本，仅作普通从库告警。",
					slave.Addr()),
			})
			continue
		}

		log.Printf("Suspected dead intermediate master %s (Cluster: %s) with %d downstream replica(s); enqueueing investigation",
			slave.Addr(), cluster, len(graph.DirectChildren(slave)))

		select {
		case m.jobCh <- &investigationJob{Node: slave, InitialCode: logic.DeadIntermediateMaster}:
		case <-m.stopCh:
			return
		}
	}
}

// clusterSilenced 报告集群当前是否处于静默窗口内，并清除已过期的条目。
func (m *Monitor) clusterSilenced(cluster string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	until, ok := m.silencedClusters[cluster]
	if !ok {
		return false
	}
	if time.Now().Before(until) {
		return true
	}
	delete(m.silencedClusters, cluster)
	return false
}

// TriggerManualFailover 手动为某个集群发起一次故障切换。
func (m *Monitor) TriggerManualFailover(clusterName, preferredNode string, graceful bool) error {
	if !m.raftNode.IsLeader() {
		return fmt.Errorf("not raft leader")
	}
	if !m.managesCluster(clusterName) {
		return fmt.Errorf("cluster %s is not managed by this %s Manager cluster (not registered or wrong tier)",
			clusterName, m.Tier())
	}

	nodes, err := m.dcClient.GetClusterNodes(clusterName)
	if err != nil {
		return fmt.Errorf("failed to get nodes for cluster %s: %v", clusterName, err)
	}

	var masterNode *datacenter.Node
	var prefNode *datacenter.Node

	for _, n := range nodes {
		if n.Role == "master" {
			masterNode = n
		}
		if preferredNode != "" {
			hostPort := fmt.Sprintf("%s:%d", n.Host, n.Port)
			if preferredNode == n.Host || preferredNode == hostPort {
				prefNode = n
			}
		}
	}

	if masterNode == nil {
		return fmt.Errorf("no master found for cluster %s", clusterName)
	}

	log.Printf("Manual Failover Triggered for %s. Old Master: %s, Graceful: %v", clusterName, masterNode.Host, graceful)

	go func() {
		m.ResetSilence(clusterName)
		code := logic.DeadMaster
		if graceful {
			code = logic.GracefulMasterTakeover
		}
		m.HandleConfirmedFailureWithCode(code, masterNode, prefNode)
	}()

	return nil
}

// ResetSilence 清除指定集群的静默期。
func (m *Monitor) ResetSilence(clusterName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.silencedClusters, clusterName)
}

// JoinNode 将一个新节点加入 Raft 集群
func (m *Monitor) JoinNode(nodeID, addr string) error {
	if !m.raftNode.IsLeader() {
		return fmt.Errorf("not raft leader")
	}
	return m.raftNode.Join(nodeID, addr)
}

// RemoveNode 将一个成员从 Raft 集群中永久移除。只有 Leader
// 可以执行该操作；移除后，Leader 将停止与该节点联系。
func (m *Monitor) RemoveNode(nodeID string) error {
	if !m.raftNode.IsLeader() {
		return fmt.Errorf("not raft leader")
	}
	return m.raftNode.Remove(nodeID)
}

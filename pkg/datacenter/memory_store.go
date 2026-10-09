package datacenter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// memoryStore 在进程内实现 MetaStore。当未配置元数据 DB
// 或连接失败时使用（优雅降级）。切换历史
// 不会在内存中持久化。
type memoryStore struct {
	mu    sync.RWMutex
	nodes map[string]*Node

	// snapshots 以集群为键保存拓扑边集合 JSON，最近一次
	// 排在最后（仅存内存；进程重启后丢失）。
	snapshots map[string][]string

	// clusterMeta 在内存中保存集群级属性（优先级/端点）。
	clusterMeta map[string]ClusterMeta
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		nodes:       make(map[string]*Node),
		snapshots:   make(map[string][]string),
		clusterMeta: make(map[string]ClusterMeta),
	}
}

func (s *memoryStore) Close() error { return nil }

func (s *memoryStore) EnsureSchema(_ context.Context) error { return nil }

func (s *memoryStore) GetStaleNodes(_ context.Context, role string, olderThan time.Duration) ([]*Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var staleNodes []*Node
	now := time.Now()

	for _, node := range s.nodes {
		if node.IsDeleted || node.Status == StatusPending {
			continue
		}
		if role != "" && node.Role != role {
			continue
		}
		if now.Sub(node.UpdateTime) > olderThan {
			staleNodes = append(staleNodes, node)
		}
	}
	return staleNodes, nil
}

func (s *memoryStore) GetClusterNodes(_ context.Context, clusterName string) ([]*Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var nodes []*Node
	for _, node := range s.nodes {
		if node.ClusterName == clusterName && !node.IsDeleted {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func (s *memoryStore) ListClusters(_ context.Context) ([]*ClusterSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	agg := make(map[string]*ClusterSummary)
	var order []string
	for _, n := range s.nodes {
		if n.IsDeleted {
			continue
		}
		cs, ok := agg[n.ClusterName]
		if !ok {
			cs = &ClusterSummary{ClusterName: n.ClusterName}
			agg[n.ClusterName] = cs
			order = append(order, n.ClusterName)
		}
		cs.NodeCount++
		if n.Role == "master" {
			cs.MasterCount++
		}
		if n.Role == "slave" {
			cs.SlaveCount++
		}
		if n.Status == StatusOnline {
			cs.OnlineCount++
		}
		if n.UpdateTime.After(cs.LastHeartbeat) {
			cs.LastHeartbeat = n.UpdateTime
		}
	}

	out := make([]*ClusterSummary, 0, len(order))
	for _, name := range order {
		cs := agg[name]
		cs.Healthy = cs.MasterCount == 1 && cs.OnlineCount == cs.NodeCount
		out = append(out, cs)
	}
	mergeClusterMeta(out, s.clusterMeta)
	return out, nil
}

func (s *memoryStore) GetClusterMeta(_ context.Context, cluster string) (ClusterMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m, ok := s.clusterMeta[cluster]; ok {
		return m, nil
	}
	return DefaultClusterMeta(cluster), nil
}

func (s *memoryStore) ListClusterMetas(_ context.Context) (map[string]ClusterMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]ClusterMeta, len(s.clusterMeta))
	for k, v := range s.clusterMeta {
		out[k] = v
	}
	return out, nil
}

func (s *memoryStore) SetClusterPriority(_ context.Context, cluster string, p ClusterPriority) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.clusterMeta[cluster]; ok {
		m.ClusterPriority = p
		m.UpdateTime = time.Now()
		s.clusterMeta[cluster] = m
		return nil
	}
	now := time.Now()
	s.clusterMeta[cluster] = ClusterMeta{
		ClusterName:     cluster,
		ClusterPriority: p,
		Registered:      false,
		EndpointType:    EndpointTypeNone,
		CreateTime:      now,
		UpdateTime:      now,
	}
	return nil
}

func (s *memoryStore) RegisterCluster(_ context.Context, m ClusterMeta) error {
	endpointType, err := NormalizeEndpointType(m.EndpointType)
	if err != nil {
		return err
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.clusterMeta[m.ClusterName]; ok {
		existing.ClusterPriority = m.ClusterPriority
		existing.Registered = true
		existing.EndpointType = endpointType
		existing.EndpointValue = m.EndpointValue
		existing.Remark = m.Remark
		existing.UpdateTime = now
		s.clusterMeta[m.ClusterName] = existing
		return nil
	}
	m.Registered = true
	m.EndpointType = endpointType
	m.CreateTime = now
	m.UpdateTime = now
	s.clusterMeta[m.ClusterName] = m
	return nil
}

// ResolveClusterName 针对内存节点表复刻数据库算法。
func (s *memoryStore) ResolveClusterName(_ context.Context, hb Heartbeat) (string, error) {
	rs := hb.replica()

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 1. 自身已知
	if n, ok := s.nodes[nodeKey(hb.Host, hb.Port)]; ok && n.ClusterName != "" {
		return n.ClusterName, nil
	}

	// 2. 上游已知
	if rs.UpstreamHost != "" {
		if n, ok := s.nodes[nodeKey(rs.UpstreamHost, int(rs.UpstreamPort))]; ok && n.ClusterName != "" {
			return n.ClusterName, nil
		}
	}

	// 3. 指向我的下游
	for _, n := range s.nodes {
		if n.MasterHost == hb.Host && int(n.MasterPort) == hb.Port && n.ClusterName != "" {
			return n.ClusterName, nil
		}
	}

	// 4. 分配下一个 clusterN
	maxN := 0
	for _, n := range s.nodes {
		if v := matchClusterN(n.ClusterName); v > maxN {
			maxN = v
		}
	}
	return fmt.Sprintf("cluster%d", maxN+1), nil
}

func (s *memoryStore) UpsertHeartbeat(_ context.Context, hb Heartbeat) error {
	rs := hb.replica()
	binDir, binBase := splitBinlogBasename(rs.Extra["log_bin_basename"])

	s.mu.Lock()
	defer s.mu.Unlock()

	key := nodeKey(hb.Host, hb.Port)
	if n, ok := s.nodes[key]; ok {
		n.UpdateTime = time.Now()
		n.Role = hb.Role
		n.DbVersion = hb.Version
		n.DiskUsagePct = hb.DiskUsage
		n.Load = hb.Load
		n.AgentPort = rs.AgentPort
		n.MasterHost = rs.UpstreamHost
		n.MasterPort = rs.UpstreamPort
		n.GtidExecuted = rs.gtidExecuted()
		n.BinlogFile = rs.binlogFile()
		n.BinlogPos = rs.binlogPos()
		n.BinlogDir = binDir
		n.BinlogBaseName = binBase
		n.ReceiverRunning = rs.ReceiverRunning
		n.ApplierRunning = rs.ApplierRunning
		n.SemiSyncEnabled = rs.SyncReplication
		n.SecondsBehind = rs.LagSeconds
		n.UsingGTID = rs.usingGTID()
		return nil
	}

	s.nodes[key] = &Node{
		ID:              uint64(len(s.nodes) + 1),
		ClusterName:     hb.Cluster,
		Role:            hb.Role,
		Host:            hb.Host,
		Port:            int32(hb.Port),
		AgentPort:       rs.AgentPort,
		MasterHost:      rs.UpstreamHost,
		MasterPort:      rs.UpstreamPort,
		DbType:          hb.DBType,
		DbVersion:       hb.Version,
		Status:          StatusOnline,
		UpdateTime:      time.Now(),
		CreateTime:      time.Now(),
		DiskUsagePct:    hb.DiskUsage,
		Load:            hb.Load,
		GtidExecuted:    rs.gtidExecuted(),
		BinlogFile:      rs.binlogFile(),
		BinlogPos:       rs.binlogPos(),
		BinlogDir:       binDir,
		BinlogBaseName:  binBase,
		ReceiverRunning: rs.ReceiverRunning,
		ApplierRunning:  rs.ApplierRunning,
		SemiSyncEnabled: rs.SyncReplication,
		SecondsBehind:   rs.LagSeconds,
		UsingGTID:       rs.usingGTID(),
	}
	return nil
}

func (s *memoryStore) InsertSwitchRecord(_ context.Context, _ *SwitchRecord) error {
	return fmt.Errorf("metadata store unavailable (in-memory simulation mode)")
}

func (s *memoryStore) GetLatestSwitch(_ context.Context, _ string) (*SwitchRecord, error) {
	return nil, fmt.Errorf("metadata store unavailable (in-memory simulation mode)")
}

// InsertTopologySnapshot 在内存中追加一条切换前的边集合 JSON。
func (s *memoryStore) InsertTopologySnapshot(_ context.Context, cluster, edgesJSON string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots[cluster] = append(s.snapshots[cluster], edgesJSON)
	return nil
}

// GetLatestTopologySnapshot 返回最近一次保存的边集合 JSON。
func (s *memoryStore) GetLatestTopologySnapshot(_ context.Context, cluster string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := s.snapshots[cluster]
	if len(list) == 0 {
		return "", fmt.Errorf("no topology snapshot for cluster %s", cluster)
	}
	return list[len(list)-1], nil
}

// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package datacenter

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"database/sql"

	_ "modernc.org/sqlite"
)

// newTestSQLiteStore 构建一个由临时文件支撑的 sqliteStore。
func newTestSQLiteStore(t *testing.T) (*sqliteStore, context.Context) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	s := &sqliteStore{db: db}
	ctx := context.Background()
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return s, ctx
}

func hb(host string, port int, role string, upstreamHost string, upstreamPort int32) Heartbeat {
	h := Heartbeat{
		Host: host, Port: port, DBType: "mysql", Role: role,
		Version: "9.5.0", DiskUsage: 10, Load: 1,
		Replica: &ReplicaStatus{AgentPort: int32(port + 20000)},
	}
	if upstreamHost != "" {
		h.Replica.UpstreamHost = upstreamHost
		h.Replica.UpstreamPort = upstreamPort
		h.Replica.ReceiverRunning = true
		h.Replica.ApplierRunning = true
	}
	return h
}

func TestSQLiteStore_HeartbeatAndClusters(t *testing.T) {
	s, ctx := newTestSQLiteStore(t)

	master := hb("10.0.0.1", 3306, "master", "", 0)

	// 全新节点的第一次心跳。
	name, err := s.ResolveClusterName(ctx, master)
	if err != nil {
		t.Fatalf("resolve master: %v", err)
	}
	if name != "cluster1" {
		t.Fatalf("first master should resolve cluster1, got %q", name)
	}
	master.Cluster = name
	if err := s.UpsertHeartbeat(ctx, master); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	// 新记录为 PENDING：陈旧检测必须将其排除。负阈值会把
	// 截止时间推到未来（时间谓词恒为真），
	// 从而单独隔离 PENDING 状态过滤条件。
	stale, err := s.GetStaleNodes(ctx, "master", -time.Hour)
	if err != nil {
		t.Fatalf("stale pending: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("PENDING row must be excluded from stale, got %d", len(stale))
	}

	// 第二次心跳将其翻转为 ONLINE。
	if err := s.UpsertHeartbeat(ctx, master); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	stale, _ = s.GetStaleNodes(ctx, "master", -time.Hour)
	if len(stale) != 1 {
		t.Fatalf("ONLINE master should be stale-eligible, got %d", len(stale))
	}

	clusters, err := s.ListClusters(ctx)
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}
	if len(clusters) != 1 {
		t.Fatalf("want 1 cluster, got %d", len(clusters))
	}
	c := clusters[0]
	if c.ClusterName != "cluster1" || c.NodeCount != 1 || c.MasterCount != 1 ||
		c.OnlineCount != 1 || !c.Healthy {
		t.Fatalf("unexpected cluster summary: %+v", c)
	}

	// 上游为该主节点的从节点归属到 cluster1。
	slave := hb("10.0.0.2", 3306, "slave", "10.0.0.1", 3306)
	sname, err := s.ResolveClusterName(ctx, slave)
	if err != nil {
		t.Fatalf("resolve slave: %v", err)
	}
	if sname != "cluster1" {
		t.Fatalf("slave should attach to cluster1, got %q", sname)
	}
	slave.Cluster = sname
	if err := s.UpsertHeartbeat(ctx, slave); err != nil {
		t.Fatalf("upsert slave: %v", err)
	}
	if err := s.UpsertHeartbeat(ctx, slave); err != nil {
		t.Fatalf("upsert slave online: %v", err)
	}

	nodes, err := s.GetClusterNodes(ctx, "cluster1")
	if err != nil {
		t.Fatalf("get nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("cluster1 should have 2 nodes, got %d", len(nodes))
	}

	clusters, _ = s.ListClusters(ctx)
	c = clusters[0]
	if c.NodeCount != 2 || c.MasterCount != 1 || c.SlaveCount != 1 ||
		c.OnlineCount != 2 || !c.Healthy {
		t.Fatalf("unexpected 2-node cluster: %+v", c)
	}

	// 没有关系的独立主节点 -> cluster2。
	indie := hb("10.0.0.3", 3306, "master", "", 0)
	iname, err := s.ResolveClusterName(ctx, indie)
	if err != nil {
		t.Fatalf("resolve indie: %v", err)
	}
	if iname != "cluster2" {
		t.Fatalf("independent master should be cluster2, got %q", iname)
	}
}

func TestSQLiteStore_ClusterMetaRoundTrip(t *testing.T) {
	s, ctx := newTestSQLiteStore(t)

	// 记录缺失：返回默认值，不报错。
	got, err := s.GetClusterMeta(ctx, "cluster1")
	if err != nil {
		t.Fatalf("get missing meta: %v", err)
	}
	if got.ClusterPriority != PriorityP3 || got.Registered {
		t.Fatalf("missing row should yield defaults, got %+v", got)
	}

	// 更新优先级会创建一条未注册的记录。
	if err := s.SetClusterPriority(ctx, "cluster1", PriorityP0); err != nil {
		t.Fatalf("set priority: %v", err)
	}
	got, _ = s.GetClusterMeta(ctx, "cluster1")
	if got.ClusterPriority != PriorityP0 || got.Registered ||
		got.EndpointType != EndpointTypeNone {
		t.Fatalf("after set priority: %+v", got)
	}

	// 注册会将 registered 置为真并存入端点信息。
	reg := ClusterMeta{
		ClusterName: "cluster1", ClusterPriority: PriorityP0,
		EndpointType: EndpointTypeVIP, EndpointValue: "10.0.0.1",
	}
	if err := s.RegisterCluster(ctx, reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, _ = s.GetClusterMeta(ctx, "cluster1")
	if !got.Registered || got.ClusterPriority != PriorityP0 ||
		got.EndpointType != EndpointTypeVIP || got.EndpointValue != "10.0.0.1" {
		t.Fatalf("after register: %+v", got)
	}

	// ListClusterMetas 返回实际存在的记录。
	all, err := s.ListClusterMetas(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("list metas: %v len=%d", err, len(all))
	}

	// 重复注册（例如端点变更）会就地更新或写入。
	reg.EndpointValue = "10.0.0.2"
	if err := s.RegisterCluster(ctx, reg); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	got, _ = s.GetClusterMeta(ctx, "cluster1")
	if got.EndpointValue != "10.0.0.2" {
		t.Fatalf("re-register should update endpoint, got %q", got.EndpointValue)
	}

	// ListClusters 会合并属性。集群由节点记录聚合而来，
	// 因此先为 cluster1 插入一条心跳。
	nodeHB := hb("10.0.0.1", 3306, "master", "", 0)
	cname, err := s.ResolveClusterName(ctx, nodeHB)
	if err != nil {
		t.Fatalf("resolve node: %v", err)
	}
	nodeHB.Cluster = cname
	if err := s.UpsertHeartbeat(ctx, nodeHB); err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	if err := s.UpsertHeartbeat(ctx, nodeHB); err != nil {
		t.Fatalf("upsert node online: %v", err)
	}

	clusters, err := s.ListClusters(ctx)
	if err != nil {
		t.Fatalf("list clusters: %v", err)
	}
	if len(clusters) != 1 || clusters[0].ClusterPriority != PriorityP0 ||
		!clusters[0].Registered {
		t.Fatalf("merged cluster summary mismatch: %+v", clusters)
	}
}

func TestSQLiteStore_SwitchHistoryRoundTrip(t *testing.T) {
	s, ctx := newTestSQLiteStore(t)

	rec := &SwitchRecord{
		ClusterName: "cluster1", Operation: "failover",
		OldMaster: "10.0.0.1:3306", NewMaster: "10.0.0.2:3306",
		AnalysisCode: "DeadMaster", Result: "SUCCESS",
	}
	if err := s.InsertSwitchRecord(ctx, rec); err != nil {
		t.Fatalf("insert record: %v", err)
	}

	got, err := s.GetLatestSwitch(ctx, "cluster1")
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if got.Operation != "failover" || got.OldMaster != rec.OldMaster ||
		got.NewMaster != rec.NewMaster || got.Result != "SUCCESS" {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	// 仅修复副本的记录不得作为拓扑变更返回。
	rep := &SwitchRecord{
		ClusterName: "cluster1", Operation: "recover", RecoverMode: "replica",
		OldMaster: "10.0.0.1:3306", NewMaster: "10.0.0.1:3306", Result: "SUCCESS",
	}
	if err := s.InsertSwitchRecord(ctx, rep); err != nil {
		t.Fatalf("insert replica record: %v", err)
	}
	got, err = s.GetLatestSwitch(ctx, "cluster1")
	if err != nil {
		t.Fatalf("get latest after replica: %v", err)
	}
	if got.Operation != "failover" {
		t.Fatalf("latest should remain the failover, got %q", got.Operation)
	}
}

func TestSQLiteStore_NodeBinlogRescueRoundTrip(t *testing.T) {
	s, ctx := newTestSQLiteStore(t)

	// 无记录：默认 enabled=false（fail-closed）。
	got, err := s.GetNodeBinlogRescue(ctx, "10.0.0.1", 3306)
	if err != nil {
		t.Fatalf("读取缺失记录：%v", err)
	}
	if got.Enabled {
		t.Fatalf("无节点行应默认关闭，得到 %+v", got)
	}

	// 节点尚未上报时配置策略：必须报错，避免“成功但无效”的假阳性。
	before := NodeBinlogRescue{Host: "10.0.0.1", Port: 3306, Enabled: true}
	if err := s.UpsertNodeBinlogRescue(ctx, before); err == nil {
		t.Fatal("节点不存在时配置救援策略应失败")
	}

	// 先有节点心跳，并上报真实 binlog 路径。
	h := hb("10.0.0.1", 3306, "master", "", 0)
	h.Replica.Extra = map[string]string{
		"log_bin_basename": filepath.Join("/var/lib/mysql", "mysql-bin"),
	}
	if err := s.UpsertHeartbeat(ctx, h); err != nil {
		t.Fatalf("写入节点心跳：%v", err)
	}

	// 运维仅配置策略三列；路径由心跳提供，二者在 meta_data 同表收敛。
	cfg := NodeBinlogRescue{
		Host: "10.0.0.1", Port: 3306, Enabled: true,
		AccessRoots: []string{"/var/lib/mysql"}, MaxBytes: 1 << 30,
	}
	if err := s.UpsertNodeBinlogRescue(ctx, cfg); err != nil {
		t.Fatalf("upsert 策略：%v", err)
	}
	got, _ = s.GetNodeBinlogRescue(ctx, "10.0.0.1", 3306)
	if !got.Enabled || got.BinlogDir != "/var/lib/mysql" || got.BinlogBaseName != "mysql-bin" ||
		len(got.AccessRoots) != 1 || got.AccessRoots[0] != "/var/lib/mysql" || got.MaxBytes != 1<<30 {
		t.Fatalf("往返结果不符：%+v", got)
	}
}

func TestSQLiteStore_HeartbeatRecordsBinlogLocation(t *testing.T) {
	s, ctx := newTestSQLiteStore(t)

	// 心跳在 Extra 中携带真实 log_bin_basename，UpsertHeartbeat 应
	// 自动记录其目录/基础名（rescue_enabled 保持默认 false，不被改写）。
	h := hb("10.0.0.1", 3306, "master", "", 0)
	h.Replica.Extra = map[string]string{
		"log_bin_basename": filepath.Join("/var/lib/mysql", "mysql-bin"),
	}
	if err := s.UpsertHeartbeat(ctx, h); err != nil {
		t.Fatalf("upsert：%v", err)
	}
	got, err := s.GetNodeBinlogRescue(ctx, "10.0.0.1", 3306)
	if err != nil {
		t.Fatalf("读取：%v", err)
	}
	if got.BinlogDir != "/var/lib/mysql" || got.BinlogBaseName != "mysql-bin" || got.Enabled {
		t.Fatalf("心跳应记录 binlog 路径且保持救援关闭，得到 %+v", got)
	}
}

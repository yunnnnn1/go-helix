package datacenter

import (
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/config"
)

func TestNewClient(t *testing.T) {
	// 使用 nil 配置测试（应使用内存模拟）
	nilClient := NewClient(nil)
	if nilClient == nil {
		t.Fatal("NewClient with nil config should return non-nil client")
	}

	// 使用非法配置测试（应使用内存模拟）
	invalidConfig := &config.DatabaseConfig{
		Type:     "mysql",
		Host:     "invalid-host",
		Port:     9999,
		User:     "test",
		Password: "test",
		DBName:   "test",
		Timeout:  "1s",
	}
	invalidClient := NewClient(invalidConfig)
	if invalidClient == nil {
		t.Fatal("NewClient with invalid config should return non-nil client")
	}

	// 使用结构合法的配置测试（即使连接失败，也应返回 client）
	validConfig := &config.DatabaseConfig{
		Type:     "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "",
		DBName:   "dc",
		Timeout:  "1s",
	}
	validClient := NewClient(validConfig)
	if validClient == nil {
		t.Fatal("NewClient with valid config should return non-nil client")
	}
}

func TestNodeStatusConstants(t *testing.T) {
	// 测试状态常量是否为预期值
	if StatusOnline != "ONLINE" {
		t.Errorf("Expected StatusOnline to be 'ONLINE', got %s", StatusOnline)
	}
	if StatusOffline != "OFFLINE" {
		t.Errorf("Expected StatusOffline to be 'OFFLINE', got %s", StatusOffline)
	}
	if StatusFailed != "FAILED" {
		t.Errorf("Expected StatusFailed to be 'FAILED', got %s", StatusFailed)
	}
}

// TestAutoAllocateCluster 验证 cluster_N 的自动分配、通过复制边完成归属、
// 序号递增，以及对显式指定名称的尊重。
func TestAutoAllocateCluster(t *testing.T) {
	client := NewClient(nil) // 内存模式

	// 1. 第一个主节点，没有上游 -> cluster1
	if err := client.RegisterOrUpdateHeartbeat("10.0.0.1", 3306, "mysql", "", "master", "8.0", 10, 1,
		&ReplicaStatus{AgentPort: 53306}); err != nil {
		t.Fatalf("register first master: %v", err)
	}
	if n := mustCount(t, client, "cluster1"); n != 1 {
		t.Fatalf("cluster1 expected 1 node, got %d", n)
	}

	// 2. 上游为该主节点的从节点 -> 加入 cluster1
	if err := client.RegisterOrUpdateHeartbeat("10.0.0.2", 3306, "mysql", "", "slave", "8.0", 20, 1,
		&ReplicaStatus{AgentPort: 53306, UpstreamHost: "10.0.0.1", UpstreamPort: 3306}); err != nil {
		t.Fatalf("register slave: %v", err)
	}
	if n := mustCount(t, client, "cluster1"); n != 2 {
		t.Fatalf("cluster1 expected 2 nodes after slave attach, got %d", n)
	}

	// 3. 相互独立的主节点，没有关系 -> cluster2
	if err := client.RegisterOrUpdateHeartbeat("10.0.0.3", 3306, "mysql", "", "master", "8.0", 10, 1,
		&ReplicaStatus{AgentPort: 53306}); err != nil {
		t.Fatalf("register second master: %v", err)
	}
	if n := mustCount(t, client, "cluster2"); n != 1 {
		t.Fatalf("cluster2 expected 1 node, got %d", n)
	}

	// 4. 显式指定的集群名原样保留
	if err := client.RegisterOrUpdateHeartbeat("10.0.0.4", 3306, "mysql", "manual_cluster", "master", "8.0", 10, 1,
		&ReplicaStatus{AgentPort: 53306}); err != nil {
		t.Fatalf("register explicit cluster: %v", err)
	}
	if n := mustCount(t, client, "manual_cluster"); n != 1 {
		t.Fatalf("manual_cluster expected 1 node, got %d", n)
	}
}

// TestSlaveComesUpFirst 验证当从节点先于其主节点注册时，主节点随后能通过
// 下游边加入该集群（不产生空号）。
func TestSlaveComesUpFirst(t *testing.T) {
	client := NewClient(nil)

	// 从节点先注册，上游尚未注册 -> cluster1
	if err := client.RegisterOrUpdateHeartbeat("10.0.0.2", 3306, "mysql", "", "slave", "8.0", 20, 1,
		&ReplicaStatus{AgentPort: 53306, UpstreamHost: "10.0.0.1", UpstreamPort: 3306}); err != nil {
		t.Fatalf("register slave first: %v", err)
	}

	// 主节点随后注册 -> 找到该下游并加入 cluster1
	if err := client.RegisterOrUpdateHeartbeat("10.0.0.1", 3306, "mysql", "", "master", "8.0", 10, 1,
		&ReplicaStatus{AgentPort: 53306}); err != nil {
		t.Fatalf("register master after: %v", err)
	}

	if n := mustCount(t, client, "cluster1"); n != 2 {
		t.Fatalf("cluster1 expected master+slave = 2 nodes, got %d", n)
	}
	if n := mustCount(t, client, "cluster2"); n != 0 {
		t.Fatalf("cluster2 should not be created when downstream edge exists, got %d", n)
	}
}

func mustCount(t *testing.T, client *Client, cluster string) int {
	t.Helper()
	nodes, err := client.GetClusterNodes(cluster)
	if err != nil {
		t.Fatalf("get nodes for %s: %v", cluster, err)
	}
	return len(nodes)
}

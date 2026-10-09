package logic

import (
	"context"
	"fmt"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// MockSwitcher 实现了 Switcher，供测试使用
type MockSwitcher struct {
	electFunc    func(ctx context.Context, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) (*ElectionResult, error)
	failoverFunc func(ctx context.Context, failedNode *datacenter.Node, result *ElectionResult) error
}

func (m *MockSwitcher) Elect(ctx context.Context, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) (*ElectionResult, error) {
	if m.electFunc != nil {
		return m.electFunc(ctx, failedNode, clusterNodes, preferredNode)
	}
	return &ElectionResult{NewMaster: &datacenter.Node{Host: "mock-master"}}, nil
}

func (m *MockSwitcher) Failover(ctx context.Context, failedNode *datacenter.Node, result *ElectionResult) error {
	if m.failoverFunc != nil {
		return m.failoverFunc(ctx, failedNode, result)
	}
	return nil
}

func (m *MockSwitcher) GracefulMasterTakeover(ctx context.Context, oldMaster *datacenter.Node, result *ElectionResult) error {
	return nil
}

func (m *MockSwitcher) RecoverReplica(ctx context.Context, master, target *datacenter.Node) error {
	return nil
}

func (m *MockSwitcher) RebuildTopology(ctx context.Context, desiredMaster *datacenter.Node, allNodes []*datacenter.Node) error {
	return nil
}

func (m *MockSwitcher) RecoverIntermediateMaster(ctx context.Context, failedIntermediate *datacenter.Node, allNodes []*datacenter.Node) error {
	return nil
}

func (m *MockSwitcher) RehomeToEdges(ctx context.Context, allNodes []*datacenter.Node, edges []TopologyEdge) error {
	return nil
}

func TestHandleFailure(t *testing.T) {
	// 注册 mock switcher
	Register("mock", &MockSwitcher{})

	failedNode := &datacenter.Node{
		ClusterName: "test-cluster",
		Host:        "failed-host",
		DbType:      "mock",
	}

	clusterNodes := []*datacenter.Node{
		failedNode,
		{Host: "slave-1"},
	}

	// 测试成功的故障切换
	err := HandleFailure(context.Background(), failedNode, clusterNodes, nil)
	if err != nil {
		t.Fatalf("HandleFailure failed: %v", err)
	}

	// 测试不支持的数据库类型
	unsupportedNode := &datacenter.Node{
		ClusterName: "test-cluster",
		Host:        "failed-host",
		DbType:      "unsupported",
	}
	err = HandleFailure(context.Background(), unsupportedNode, clusterNodes, nil)
	if err == nil {
		t.Fatal("Expected error for unsupported db type")
	}

	// 测试选举失败
	Register("mock-fail-elect", &MockSwitcher{
		electFunc: func(ctx context.Context, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) (*ElectionResult, error) {
			return nil, fmt.Errorf("election failed")
		},
	})
	failedNode.DbType = "mock-fail-elect"
	err = HandleFailure(context.Background(), failedNode, clusterNodes, nil)
	if err == nil || err.Error() != "election failed" {
		t.Fatalf("Expected election failed error, got: %v", err)
	}

	// 测试故障切换失败
	Register("mock-fail-failover", &MockSwitcher{
		failoverFunc: func(ctx context.Context, failedNode *datacenter.Node, result *ElectionResult) error {
			return fmt.Errorf("failover failed")
		},
	})
	failedNode.DbType = "mock-fail-failover"
	err = HandleFailure(context.Background(), failedNode, clusterNodes, nil)
	if err == nil || err.Error() != "failover failed" {
		t.Fatalf("Expected failover failed error, got: %v", err)
	}
}

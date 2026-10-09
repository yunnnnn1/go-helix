package monitor

import (
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

func TestFilterByTier(t *testing.T) {
	summaries := []*datacenter.ClusterSummary{
		{ClusterName: "cluster1", ClusterPriority: datacenter.PriorityP0, Registered: true},
		{ClusterName: "cluster2", ClusterPriority: datacenter.PriorityP0, Registered: false},
		{ClusterName: "cluster3", ClusterPriority: datacenter.PriorityP1, Registered: true},
	}

	// 非 P0 层级保留全部集群。
	if out := filterByTier(summaries, "P3"); len(out) != 3 {
		t.Fatalf("P3 tier should keep all clusters, got %d", len(out))
	}

	// P0 仅保留已注册的 P0 集群。
	out := filterByTier(summaries, "P0")
	if len(out) != 1 || out[0].ClusterName != "cluster1" {
		t.Fatalf("P0 tier should keep only registered P0, got %+v", out)
	}
}

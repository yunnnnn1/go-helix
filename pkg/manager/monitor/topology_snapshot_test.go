package monitor

import (
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

func snapNode(role, host string, port int32, upHost string, upPort int32) *datacenter.Node {
	return &datacenter.Node{
		Role:       role,
		Host:       host,
		Port:       port,
		MasterHost: upHost,
		MasterPort: upPort,
	}
}

func TestTopologyEdgesJSON_RoundTrip(t *testing.T) {
	nodes := []*datacenter.Node{
		snapNode("master", "10.0.0.1", 3306, "", 0),
		snapNode("slave", "10.0.0.2", 3306, "10.0.0.1", 3306),
		snapNode("slave", "10.0.0.3", 3306, "10.0.0.2", 3306),
	}

	edgesJSON, err := topologyEdgesJSON(nodes)
	if err != nil {
		t.Fatalf("topologyEdgesJSON: %v", err)
	}

	edges, err := parseTopologyEdges(edgesJSON)
	if err != nil {
		t.Fatalf("parseTopologyEdges: %v", err)
	}
	if len(edges) != 2 {
		t.Fatalf("edges = %d, want 2", len(edges))
	}

	got := map[string]string{}
	for _, e := range edges {
		got[e.Child] = e.Upstream
	}
	want := map[string]string{
		"10.0.0.2:3306": "10.0.0.1:3306",
		"10.0.0.3:3306": "10.0.0.2:3306",
	}
	for child, up := range want {
		if got[child] != up {
			t.Errorf("edge %s -> %s, want %s", child, got[child], up)
		}
	}
}

func TestParseTopologyEdges_InvalidJSON(t *testing.T) {
	if _, err := parseTopologyEdges("{not json"); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

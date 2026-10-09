package datacenter

import "testing"

func TestParseClusterPriority(t *testing.T) {
	cases := []struct {
		in   string
		want ClusterPriority
	}{
		{"P0", PriorityP0},
		{" p1 ", PriorityP1},
		{"p2", PriorityP2},
		{"P3", PriorityP3},
		{"", PriorityP3},
	}
	for _, c := range cases {
		got, err := ParseClusterPriority(c.in)
		if err != nil {
			t.Errorf("ParseClusterPriority(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseClusterPriority(%q) = %q, want %q", c.in, got, c.want)
		}
	}

	for _, bad := range []string{"P4", "P", "x0", "high"} {
		if _, err := ParseClusterPriority(bad); err == nil {
			t.Errorf("ParseClusterPriority(%q) expected error", bad)
		}
	}
}

func TestClusterPriorityNumeric(t *testing.T) {
	want := []int{0, 1, 2, 3}
	for i, p := range []ClusterPriority{PriorityP0, PriorityP1, PriorityP2, PriorityP3} {
		if p.Numeric() != want[i] {
			t.Errorf("%s.Numeric() = %d, want %d", p, p.Numeric(), want[i])
		}
	}
}

func TestNormalizeEndpointType(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", EndpointTypeNone},
		{"VIP", EndpointTypeVIP},
		{" domain ", EndpointTypeDomain},
		{"none", EndpointTypeNone},
	}
	for _, c := range cases {
		got, err := NormalizeEndpointType(c.in)
		if err != nil || got != c.want {
			t.Errorf("NormalizeEndpointType(%q) = %q,%v want %q", c.in, got, err, c.want)
		}
	}
	if _, err := NormalizeEndpointType("lb"); err == nil {
		t.Error("NormalizeEndpointType(lb) expected error")
	}
}

func TestDefaultClusterMeta(t *testing.T) {
	m := DefaultClusterMeta("cluster9")
	if m.ClusterName != "cluster9" || m.ClusterPriority != PriorityP3 ||
		m.Registered || m.EndpointType != EndpointTypeNone || m.EndpointValue != "" {
		t.Fatalf("unexpected default meta: %+v", m)
	}
}

func TestMergeClusterMeta(t *testing.T) {
	summaries := []*ClusterSummary{
		{ClusterName: "cluster1"},
		{ClusterName: "cluster2"},
		{ClusterName: "cluster3"},
	}
	metas := map[string]ClusterMeta{
		"cluster1": {ClusterName: "cluster1", ClusterPriority: PriorityP0, Registered: true,
			EndpointType: EndpointTypeVIP, EndpointValue: "10.0.0.1"},
		// cluster2 缺失：适用默认值。
		"cluster3": {ClusterName: "cluster3", ClusterPriority: PriorityP2},
	}

	mergeClusterMeta(summaries, metas)

	if summaries[0].ClusterPriority != PriorityP0 || !summaries[0].Registered ||
		summaries[0].EndpointType != EndpointTypeVIP || summaries[0].EndpointValue != "10.0.0.1" {
		t.Fatalf("cluster1 merge mismatch: %+v", summaries[0])
	}
	if summaries[1].ClusterPriority != PriorityP3 || summaries[1].Registered ||
		summaries[1].EndpointType != EndpointTypeNone {
		t.Fatalf("cluster2 should keep defaults: %+v", summaries[1])
	}
	if summaries[2].ClusterPriority != PriorityP2 {
		t.Fatalf("cluster3 mismatch: %+v", summaries[2])
	}
}

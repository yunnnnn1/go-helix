// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logic

import (
	"strconv"
	"strings"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// splitAddr 将 host:port 拆分为主机和数字端口（int32）。
func splitAddr(addr string) (string, int32) {
	host, p, _ := strings.Cut(addr, ":")
	n, _ := strconv.Atoi(p)
	return host, int32(n)
}

// graphNode 构建一个带数据库地址和可选上游地址的节点。
func graphNode(role, addr, upstream string) *datacenter.Node {
	n := &datacenter.Node{Role: role}
	host, port := splitAddr(addr)
	n.Host = host
	n.Port = port
	if upstream != "" {
		uh, up := splitAddr(upstream)
		n.MasterHost = uh
		n.MasterPort = up
	}
	return n
}

// TestBuildTopology_Hierarchy 构建如下结构：
//
//	M
//	├─IM1
//	│ └─L1
//	└─L2
func TestBuildTopology_Hierarchy(t *testing.T) {
	m := graphNode("master", "10.0.0.1:3306", "")
	im1 := graphNode("slave", "10.0.0.2:3306", "10.0.0.1:3306")
	l1 := graphNode("slave", "10.0.0.3:3306", "10.0.0.2:3306")
	l2 := graphNode("slave", "10.0.0.4:3306", "10.0.0.1:3306")

	nodes := []*datacenter.Node{m, im1, l1, l2}
	g := BuildTopology(nodes)

	if root := g.Root(); root != m {
		t.Fatalf("root = %s, want master", root.Addr())
	}
	if p := g.Parent(im1); p != m {
		t.Errorf("parent(IM1) = %v, want M", p)
	}
	if p := g.Parent(l1); p != im1 {
		t.Errorf("parent(L1) = %v, want IM1", p)
	}

	kids := g.DirectChildren(m)
	if len(kids) != 2 {
		t.Fatalf("direct children of M = %d, want 2", len(kids))
	}
	if len(g.DirectChildren(im1)) != 1 {
		t.Errorf("direct children of IM1 = %d, want 1", len(g.DirectChildren(im1)))
	}

	if !g.IsIntermediateMaster(im1) {
		t.Error("IM1 must be detected as intermediate master")
	}
	if g.IsIntermediateMaster(m) || g.IsIntermediateMaster(l1) {
		t.Error("master and leaf must not be intermediate masters")
	}

	ims := g.IntermediateMasters()
	if len(ims) != 1 || ims[0] != im1 {
		t.Errorf("intermediate masters = %v, want [IM1]", ims)
	}

	leaves := g.Leaves()
	if len(leaves) != 2 {
		t.Errorf("leaves = %d, want 2 (L1,L2)", len(leaves))
	}

	desc := g.Descendants(m)
	if len(desc) != 3 {
		t.Errorf("descendants of M = %d, want 3", len(desc))
	}
	if len(g.Subtree(im1)) != 2 {
		t.Errorf("subtree of IM1 = %d, want 2 (IM1,L1)", len(g.Subtree(im1)))
	}

	edges := g.Edges()
	if len(edges) != 3 {
		t.Fatalf("edges = %d, want 3", len(edges))
	}
	got := map[string]string{}
	for _, e := range edges {
		got[e.Child] = e.Upstream
	}
	want := map[string]string{
		"10.0.0.2:3306": "10.0.0.1:3306",
		"10.0.0.3:3306": "10.0.0.2:3306",
		"10.0.0.4:3306": "10.0.0.1:3306",
	}
	if len(got) != len(want) {
		t.Fatalf("edge map = %v, want %v", got, want)
	}
	for child, up := range want {
		if got[child] != up {
			t.Errorf("edge child %s -> %s, want %s", child, got[child], up)
		}
	}
}

func TestBuildTopology_UnresolvedUpstream(t *testing.T) {
	// 上游不在节点集合中的节点虽没有 parent，但仍必须
	// 可以被查询。
	orphan := graphNode("slave", "10.0.0.9:3306", "10.9.9.9:3306")
	g := BuildTopology([]*datacenter.Node{orphan})
	if g.Parent(orphan) != nil {
		t.Error("unresolved upstream must leave the node parentless")
	}
	if len(g.Edges()) != 0 {
		t.Error("no edges must be produced for an unresolved upstream")
	}
}

func TestBuildTopology_Empty(t *testing.T) {
	if BuildTopology(nil).Root() != nil {
		t.Error("root of an empty graph must be nil")
	}
}

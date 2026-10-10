// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logic

import (
	"fmt"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// TopologyEdge 表示一条复制边：Child 节点从 Upstream
// 节点复制。地址为规范的 host:port 数据库地址。
type TopologyEdge struct {
	Upstream string `json:"upstream"`
	Child    string `json:"child"`
}

// Topology 是根据各节点的上游（master_host/master_port）
// 边构建的有向复制图。它与具体数据库无关，构建后不可变；
// 所有查询在较小的节点集合上均为 O(1)/O(n)。
type Topology struct {
	nodes  []*datacenter.Node
	byAddr map[string]*datacenter.Node

	// parent 将 child 地址映射到其上游节点（无上游时为 nil）。
	parent map[string]*datacenter.Node
	// children 将 parent 地址映射到其直属子节点，顺序稳定。
	children map[string][]*datacenter.Node
}

// BuildTopology 构建复制图。无法解析到已知节点的上游引用
// 会使对应 child 没有 parent（防御性设计：指向所上报节点集合
// 之外的边不得破坏图查询）。
func BuildTopology(nodes []*datacenter.Node) *Topology {
	t := &Topology{
		nodes:    nodes,
		byAddr:   make(map[string]*datacenter.Node, len(nodes)),
		parent:   make(map[string]*datacenter.Node, len(nodes)),
		children: make(map[string][]*datacenter.Node),
	}
	for _, n := range nodes {
		t.byAddr[n.Addr()] = n
	}

	for _, n := range nodes {
		if n.MasterHost == "" {
			continue
		}
		upAddr := fmt.Sprintf("%s:%d", n.MasterHost, n.MasterPort)
		if up, ok := t.byAddr[upAddr]; ok {
			t.parent[n.Addr()] = up
			t.children[upAddr] = append(t.children[upAddr], n)
		}
	}
	return t
}

// Nodes 返回构建该图时使用的全部节点。
func (t *Topology) Nodes() []*datacenter.Node { return t.nodes }

// Node 按 host:port 地址解析节点，找不到时返回 nil。
func (t *Topology) Node(addr string) *datacenter.Node { return t.byAddr[addr] }

// Root 返回集群主库。优先选择 role 为 master 的节点；
// 若没有任何节点被标记（恢复场景），则回退到第一个无 parent 的
// 节点。空图返回 nil。
func (t *Topology) Root() *datacenter.Node {
	var firstParentless *datacenter.Node
	for _, n := range t.nodes {
		if n.Role == "master" {
			return n
		}
		if firstParentless == nil && t.parent[n.Addr()] == nil {
			firstParentless = n
		}
	}
	return firstParentless
}

// Parent 返回 n 的直属上游；没有时返回 nil。
func (t *Topology) Parent(n *datacenter.Node) *datacenter.Node {
	return t.parent[n.Addr()]
}

// DirectChildren 返回直接从 n 复制的节点。
func (t *Topology) DirectChildren(n *datacenter.Node) []*datacenter.Node {
	return t.children[n.Addr()]
}

// IsIntermediateMaster 判断 n 是否为一个自身带有下游
// 副本的副本（即复制图中的非叶子节点）。
func (t *Topology) IsIntermediateMaster(n *datacenter.Node) bool {
	return n.Role == "slave" && len(t.children[n.Addr()]) > 0
}

// IntermediateMasters 返回所有带有下游副本的 slave 节点。
func (t *Topology) IntermediateMasters() []*datacenter.Node {
	var out []*datacenter.Node
	for _, n := range t.nodes {
		if t.IsIntermediateMaster(n) {
			out = append(out, n)
		}
	}
	return out
}

// Leaves 返回没有下游副本的节点。
func (t *Topology) Leaves() []*datacenter.Node {
	var out []*datacenter.Node
	for _, n := range t.nodes {
		if len(t.children[n.Addr()]) == 0 {
			out = append(out, n)
		}
	}
	return out
}

// Descendants 按广度优先顺序返回 n 之下传递可达的全部节点
// （不包含 n）。
func (t *Topology) Descendants(n *datacenter.Node) []*datacenter.Node {
	var out []*datacenter.Node
	frontier := []*datacenter.Node{n}
	for len(frontier) > 0 {
		var next []*datacenter.Node
		for _, cur := range frontier {
			kids := t.children[cur.Addr()]
			out = append(out, kids...)
			next = append(next, kids...)
		}
		frontier = next
	}
	return out
}

// Subtree 返回 n 及其全部后代（以 n 为根的子树）。
func (t *Topology) Subtree(n *datacenter.Node) []*datacenter.Node {
	return append([]*datacenter.Node{n}, t.Descendants(n)...)
}

// Edges 按稳定的节点顺序返回全部复制边。拓扑快照持久化的
// 正是这些边，用于回滚时精确恢复。
func (t *Topology) Edges() []TopologyEdge {
	var edges []TopologyEdge
	for _, n := range t.nodes {
		if p := t.parent[n.Addr()]; p != nil {
			edges = append(edges, TopologyEdge{Upstream: p.Addr(), Child: n.Addr()})
		}
	}
	return edges
}

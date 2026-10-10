// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"fmt"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
)

// RehomeToEdges 将集群恢复为给定的边集合。各边从根节点开始按
// 广度优先应用，使每个上游在其下游副本之前先被重新指向（并
// 追平），从而保持 relay 的连续性。
func (s *MySQLSwitcher) RehomeToEdges(ctx context.Context, allNodes []*datacenter.Node, edges []logic.TopologyEdge) error {
	byAddr := make(map[string]*datacenter.Node, len(allNodes))
	for _, n := range allNodes {
		byAddr[n.Addr()] = n
	}

	parentToChildren := make(map[string][]string)
	hasParent := make(map[string]bool)
	var root string
	for _, e := range edges {
		if _, ok := byAddr[e.Child]; !ok {
			return fmt.Errorf("restore edge child %s not found in cluster nodes", e.Child)
		}
		if _, ok := byAddr[e.Upstream]; !ok {
			return fmt.Errorf("restore edge upstream %s not found in cluster nodes", e.Upstream)
		}
		parentToChildren[e.Upstream] = append(parentToChildren[e.Upstream], e.Child)
		hasParent[e.Child] = true
	}
	// 根 = 从不作为子节点出现的节点。
	for addr := range byAddr {
		if !hasParent[addr] {
			root = addr
			break
		}
	}

	fl := logging.FailoverLogger(ctx)
	fl.Printf("[TopologyRestore] applying %d edge(s) BFS from root %s", len(edges), root)

	// 广度优先；第 0 层 = 根。对遇到的每个子节点，先将其重新指向
	// 目标上游并等待追平，然后再进入更深层。
	frontier := []string{root}
	applied := 0
	for len(frontier) > 0 {
		var next []string
		for _, parentAddr := range frontier {
			for _, childAddr := range parentToChildren[parentAddr] {
				child := byAddr[childAddr]
				upstream := byAddr[parentAddr]
				if child == nil || upstream == nil {
					return fmt.Errorf("cannot resolve edge %s -> %s", parentAddr, childAddr)
				}
				if err := repointOne(ctx, child, upstream); err != nil {
					return fmt.Errorf("repoint %s to %s: %w", childAddr, parentAddr, err)
				}
				if err := waitReplicaCatchup(ctx, child); err != nil {
					return fmt.Errorf("catchup %s after restore: %w", childAddr, err)
				}
				applied++
				next = append(next, childAddr)
			}
		}
		frontier = next
	}

	if applied != len(edges) {
		return fmt.Errorf("topology restore applied %d edge(s), expected %d (check for cycles/disconnected edges)", applied, len(edges))
	}
	fl.Printf("[TopologyRestore] exact topology restored (%d edge(s))", applied)
	return nil
}

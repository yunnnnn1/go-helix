// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package monitor

import (
	"encoding/json"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logic"
)

// topologyEdgesJSON 构建复制图，并将其完整边集合以 JSON 形式返回，
// 用于持久化切换前快照，以便回滚时精确恢复。
func topologyEdgesJSON(nodes []*datacenter.Node) (string, error) {
	edges := logic.BuildTopology(nodes).Edges()
	b, err := json.Marshal(edges)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// parseTopologyEdges 将保存的拓扑快照解码为边集合。
func parseTopologyEdges(edgesJSON string) ([]logic.TopologyEdge, error) {
	var edges []logic.TopologyEdge
	if err := json.Unmarshal([]byte(edgesJSON), &edges); err != nil {
		return nil, err
	}
	return edges, nil
}

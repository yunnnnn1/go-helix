// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logic

import (
	"context"
	"fmt"
	"sync"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// ElectionResult 保存一次选举的结果
type ElectionResult struct {
	NewMaster *datacenter.Node
	Slaves    []*datacenter.Node
}

// Switcher 定义特定数据库故障切换逻辑的接口
type Switcher interface {
	Elect(ctx context.Context, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) (*ElectionResult, error)
	Failover(ctx context.Context, failedNode *datacenter.Node, election *ElectionResult) error
	GracefulMasterTakeover(ctx context.Context, oldMaster *datacenter.Node, election *ElectionResult) error

	// RecoverReplica 修复单个副本到当前主库的复制链路
	// （stop -> change source -> start），并等待其追平。
	RecoverReplica(ctx context.Context, master, target *datacenter.Node) error
	// RebuildTopology 将集群收敛为单一主库（desiredMaster），
	// 其他所有节点都从该主库复制。
	RebuildTopology(ctx context.Context, desiredMaster *datacenter.Node, allNodes []*datacenter.Node) error

	// RecoverIntermediateMaster 修复已死亡的中间主：提升其
	// 下游的一个副本接替其位置——被提升的节点重新指向死亡中间主
	// 的上游，其余直属副本则重新指向被提升的节点。
	RecoverIntermediateMaster(ctx context.Context, failedIntermediate *datacenter.Node, allNodes []*datacenter.Node) error

	// RehomeToEdges 重新指向各节点，使集群与给定的复制
	// 边完全一致（供回滚时用于恢复已保存的拓扑形态）。
	RehomeToEdges(ctx context.Context, allNodes []*datacenter.Node, edges []TopologyEdge) error
}

// Factory 管理已注册的 switcher
type Factory struct {
	mu        sync.RWMutex
	switchers map[string]Switcher
}

var GlobalFactory = &Factory{
	switchers: make(map[string]Switcher),
}

func Register(dbType string, switcher Switcher) {
	GlobalFactory.mu.Lock()
	defer GlobalFactory.mu.Unlock()
	GlobalFactory.switchers[dbType] = switcher
}

func GetSwitcher(dbType string) (Switcher, error) {
	GlobalFactory.mu.RLock()
	defer GlobalFactory.mu.RUnlock()
	switcher, ok := GlobalFactory.switchers[dbType]
	if !ok {
		return nil, fmt.Errorf("failover switcher not found for db type: %s", dbType)
	}
	return switcher, nil
}

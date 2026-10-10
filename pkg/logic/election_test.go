// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logic

import (
	"strings"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// testPosition 是与具体数据库无关的 ReplicationPosition 桩实现，用于
// 在不依赖具体（MySQL）实现的情况下演练选举框架。
type testPosition int64

var _ ReplicationPosition = testPosition(0)

func (p testPosition) Compare(other ReplicationPosition) int {
	o := other.(testPosition)
	switch {
	case p > o:
		return 1
	case p < o:
		return -1
	}
	return 0
}

func TestElectionStrategy_SelectCandidate(t *testing.T) {
	strategy := &DefaultElectionStrategy{}

	failedMaster := &datacenter.Node{
		Host:   "master-host",
		Region: "Region-A",
		DC:     "DC-1",
	}

	tests := []struct {
		name           string
		candidates     []*Candidate
		expectedWinner string
	}{
		{
			name: "No valid candidates",
			candidates: []*Candidate{
				{
					Node:    &datacenter.Node{Host: "host1", Status: datacenter.StatusOffline},
					IsAlive: false,
				},
				{
					Node:    &datacenter.Node{Host: "host2", Status: datacenter.StatusOnline, IsBad: true},
					IsAlive: true,
				},
			},
			expectedWinner: "",
		},
		{
			name: "Position takes precedence",
			candidates: []*Candidate{
				{
					Node:     &datacenter.Node{Host: "host1", Status: datacenter.StatusOnline, Priority: 100},
					IsAlive:  true,
					Position: testPosition(100), // 落后
				},
				{
					Node:     &datacenter.Node{Host: "host2", Status: datacenter.StatusOnline, Priority: 10},
					IsAlive:  true,
					Position: testPosition(105), // 尽管优先级较低，但位点领先
				},
			},
			expectedWinner: "host2",
		},
		{
			name: "Sync replication takes precedence if position is equal",
			candidates: []*Candidate{
				{
					Node:       &datacenter.Node{Host: "host1", Status: datacenter.StatusOnline, Priority: 100},
					IsAlive:    true,
					Position:   testPosition(100),
					IsSemiSync: false,
				},
				{
					Node:       &datacenter.Node{Host: "host2", Status: datacenter.StatusOnline, Priority: 10},
					IsAlive:    true,
					Position:   testPosition(100),
					IsSemiSync: true, // 因同步复制而胜出
				},
			},
			expectedWinner: "host2",
		},
		{
			name: "Priority takes precedence if position and sync are equal",
			candidates: []*Candidate{
				{
					Node:       &datacenter.Node{Host: "host1", Status: datacenter.StatusOnline, Priority: 50},
					IsAlive:    true,
					Position:   testPosition(100),
					IsSemiSync: true,
				},
				{
					Node:       &datacenter.Node{Host: "host2", Status: datacenter.StatusOnline, Priority: 100},
					IsAlive:    true,
					Position:   testPosition(100),
					IsSemiSync: true,
				},
			},
			expectedWinner: "host2",
		},
		{
			name: "Location takes precedence if position, sync, and Priority are equal",
			candidates: []*Candidate{
				{
					Node:       &datacenter.Node{Host: "host1", Status: datacenter.StatusOnline, Priority: 100, Region: "Region-B", DC: "DC-2"}, // 跨区域
					IsAlive:    true,
					Position:   testPosition(100),
					IsSemiSync: true,
				},
				{
					Node:       &datacenter.Node{Host: "host2", Status: datacenter.StatusOnline, Priority: 100, Region: "Region-A", DC: "DC-1"}, // 同机房
					IsAlive:    true,
					Position:   testPosition(100),
					IsSemiSync: true,
				},
				{
					Node:       &datacenter.Node{Host: "host3", Status: datacenter.StatusOnline, Priority: 100, Region: "Region-A", DC: "DC-3"}, // 同区域
					IsAlive:    true,
					Position:   testPosition(100),
					IsSemiSync: true,
				},
			},
			expectedWinner: "host2", // 同机房胜过同区域和跨区域
		},
		{
			name: "Density takes precedence if all above are equal",
			candidates: []*Candidate{
				{
					Node:          &datacenter.Node{Host: "host1", Status: datacenter.StatusOnline, Priority: 100, Region: "Region-A", DC: "DC-1"},
					IsAlive:       true,
					Position:      testPosition(100),
					IsSemiSync:    true,
					InstanceCount: 5, // 密度较高
				},
				{
					Node:          &datacenter.Node{Host: "host2", Status: datacenter.StatusOnline, Priority: 100, Region: "Region-A", DC: "DC-1"},
					IsAlive:       true,
					Position:      testPosition(100),
					IsSemiSync:    true,
					InstanceCount: 2, // 密度较低，胜出
				},
			},
			expectedWinner: "host2",
		},
		{
			name: "Disk usage takes precedence if all above are equal",
			candidates: []*Candidate{
				{
					Node:          &datacenter.Node{Host: "host1", Status: datacenter.StatusOnline, Priority: 100, Region: "Region-A", DC: "DC-1", DiskUsagePct: 80.5},
					IsAlive:       true,
					Position:      testPosition(100),
					IsSemiSync:    true,
					InstanceCount: 2,
				},
				{
					Node:          &datacenter.Node{Host: "host2", Status: datacenter.StatusOnline, Priority: 100, Region: "Region-A", DC: "DC-1", DiskUsagePct: 40.5},
					IsAlive:       true,
					Position:      testPosition(100),
					IsSemiSync:    true,
					InstanceCount: 2,
				},
			},
			expectedWinner: "host2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			winner, logStr := strategy.SelectCandidate(tt.candidates, failedMaster)
			if tt.expectedWinner == "" {
				if winner != nil {
					t.Errorf("Expected no winner, got %s", winner.Node.Host)
				}
			} else {
				if winner == nil {
					t.Fatalf("Expected winner %s, got nil", tt.expectedWinner)
				}
				if winner.Node.Host != tt.expectedWinner {
					t.Errorf("Expected winner %s, got %s", tt.expectedWinner, winner.Node.Host)
				}
			}

			if winner != nil && !strings.Contains(logStr, winner.Node.Host) {
				t.Errorf("Expected log to contain winner host %s", winner.Node.Host)
			}
		})
	}
}

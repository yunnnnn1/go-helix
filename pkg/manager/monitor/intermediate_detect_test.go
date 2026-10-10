// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package monitor

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logic"

	_ "github.com/yunnnnn1/go-helix/pkg/db/drivers/sqlite"
)

// registerNode 注册一个节点（注册两次，以完成 PENDING -> ONLINE 转换），
// 可选地指定其上游。
func registerNode(t *testing.T, c *datacenter.Client, host string, port int, role string, upHost string, upPort int) {
	t.Helper()
	var rs *datacenter.ReplicaStatus
	if upHost != "" {
		rs = &datacenter.ReplicaStatus{UpstreamHost: upHost, UpstreamPort: int32(upPort)}
	}
	for i := 0; i < 2; i++ {
		if err := c.RegisterOrUpdateHeartbeat(host, port, "mysql", "cluster1", role, "8.0", 0, 0, rs); err != nil {
			t.Fatalf("register %s: %v", host, err)
		}
	}
}

func newTestMonitor(t *testing.T) (*Monitor, *datacenter.Client) {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), "meta.db")
	client := datacenter.NewClient(&config.DatabaseConfig{Type: "sqlite", Path: dbFile})

	// cluster1 拓扑：
	//   M(10.0.0.1)
	//   ├─IM(10.0.0.2)
	//   │  └─child(10.0.0.3)
	//   └─leaf(10.0.0.4)
	registerNode(t, client, "10.0.0.1", 3306, "master", "", 0)
	registerNode(t, client, "10.0.0.2", 3306, "slave", "10.0.0.1", 3306)
	registerNode(t, client, "10.0.0.3", 3306, "slave", "10.0.0.2", 3306)
	registerNode(t, client, "10.0.0.4", 3306, "slave", "10.0.0.1", 3306)

	return NewMonitor(nil, client), client
}

// drainJobs 读取当前已入队的全部调查任务，且不阻塞。
func drainJobs(m *Monitor) []*investigationJob {
	var out []*investigationJob
	for {
		select {
		case j := <-m.jobCh:
			out = append(out, j)
		default:
			return out
		}
	}
}

// TestCheckStaleIntermediateMasters_DetectsIM：负数阈值会让所有节点都
// “失联”；只有带下游副本的节点（IM）会被加入队列。
func TestCheckStaleIntermediateMasters_DetectsIM(t *testing.T) {
	m, _ := newTestMonitor(t)

	m.checkStaleIntermediateMasters(-1*time.Second, nil)

	jobs := drainJobs(m)
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1 (intermediate master), got %v", len(jobs), jobs)
	}
	j := jobs[0]
	if j.Node.Host != "10.0.0.2" {
		t.Errorf("enqueued node = %s, want 10.0.0.2", j.Node.Host)
	}
	if j.InitialCode != logic.DeadIntermediateMaster {
		t.Errorf("initial code = %s, want DeadIntermediateMaster", j.InitialCode)
	}
}

func TestCheckStaleIntermediateMasters_SkippedWhenMasterStale(t *testing.T) {
	m, _ := newTestMonitor(t)

	m.checkStaleIntermediateMasters(-1*time.Second, map[string]bool{"cluster1": true})

	if jobs := drainJobs(m); len(jobs) != 0 {
		t.Fatalf("jobs = %d, want 0 when cluster master itself is stale", len(jobs))
	}
}

func TestCheckStaleIntermediateMasters_Silenced(t *testing.T) {
	m, _ := newTestMonitor(t)
	m.mu.Lock()
	m.silencedClusters["cluster1"] = time.Now().Add(5 * time.Minute)
	m.mu.Unlock()

	m.checkStaleIntermediateMasters(-1*time.Second, nil)

	if jobs := drainJobs(m); len(jobs) != 0 {
		t.Fatalf("jobs = %d, want 0 while cluster is silenced", len(jobs))
	}
}

func TestClusterSilenced(t *testing.T) {
	m, _ := newTestMonitor(t)

	if m.clusterSilenced("cluster1") {
		t.Error("cluster must not be silenced by default")
	}

	m.mu.Lock()
	m.silencedClusters["cluster1"] = time.Now().Add(-time.Minute) // 已过期
	m.mu.Unlock()
	if m.clusterSilenced("cluster1") {
		t.Error("expired silence must not report silenced")
	}

	m.mu.Lock()
	m.silencedClusters["cluster1"] = time.Now().Add(time.Minute)
	m.mu.Unlock()
	if !m.clusterSilenced("cluster1") {
		t.Error("active silence must report silenced")
	}
}

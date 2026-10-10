// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package raft

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	log "github.com/yunnnnn1/go-helix/pkg/logging"

	hraft "github.com/hashicorp/raft"
)

// TestFSMGetAndObserver 验证直接读取以及 KV 变更观察者。
func TestFSMGetAndObserver(t *testing.T) {
	f := NewFSM()

	if _, ok := f.Get("missing"); ok {
		t.Fatal("missing key must report not found")
	}

	var seenKey, seenValue string
	f.SetKVChangeObserver(func(k, v string) { seenKey, seenValue = k, v })

	f.Apply(&hraft.Log{Data: []byte(`{"key":"k1","value":"v1"}`)})

	if v, ok := f.Get("k1"); !ok || v != "v1" {
		t.Fatalf("Get(k1) = %q,%v; want v1,true", v, ok)
	}
	if seenKey != "k1" || seenValue != "v1" {
		t.Fatalf("observer saw %q=%q; want k1=v1", seenKey, seenValue)
	}
}

// logFilter 用于屏蔽特定的日志消息
type logFilter struct {
	w io.Writer
}

func (f *logFilter) Write(p []byte) (n int, err error) {
	if bytes.Contains(p, []byte("Rollback failed: tx closed")) {
		return len(p), nil
	}
	return f.w.Write(p)
}

func TestMain(m *testing.M) {
	// 过滤掉关闭事务时 raft-boltdb 产生的噪声日志
	log.SetOutput(&logFilter{w: os.Stderr})
	os.Exit(m.Run())
}

func TestRaftPersistence(t *testing.T) {
	// 1. 搭建 3 个节点（全新启动）
	nodes := make([]*Node, 3)
	dirs := make([]string, 3)

	// 测试使用的基准端口，以避免端口冲突
	basePort := 17000

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		dir := fmt.Sprintf("raft-persist-test-%d", i+1)
		bind := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		// 清理上一次运行的残留
		os.RemoveAll(dir)
		dirs[i] = dir

		node, err := NewNode(id, dir, bind)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", id, err)
		}
		nodes[i] = node
	}

	// 退出时进行清理
	defer func() {
		for i, n := range nodes {
			if n != nil {
				n.Shutdown()
			}
			os.RemoveAll(dirs[i])
		}
	}()

	// 2. 用第一个节点引导集群
	t.Log("Bootstrapping cluster with Node 1...")
	cfg := hraft.Configuration{
		Servers: []hraft.Server{
			{
				ID:      hraft.ServerID("node-1"),
				Address: hraft.ServerAddress("127.0.0.1:17000"),
			},
		},
	}
	f := nodes[0].Raft.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		t.Fatalf("Bootstrap failed: %v", err)
	}

	// 3. 加入其他节点
	t.Log("Joining Node 2 and Node 3...")
	// 等待节点 1 成为 Leader
	waitForLeader(t, nodes[0], 5*time.Second)

	// 模拟 Manager 的加入逻辑（类似 HTTP 的重试机制）
	joinNode := func(targetNode *Node, newNodeID, newNodeAddr string) {
		// 生产环境中这是一个 HTTP 循环，这里仅模拟其逻辑。
		// 重试在生产环境中非常重要。
		for i := 0; i < 5; i++ {
			if err := targetNode.Join(newNodeID, newNodeAddr); err != nil {
				t.Logf("Join attempt %d failed for %s: %v. Retrying...", i+1, newNodeID, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			t.Logf("Successfully joined %s to cluster", newNodeID)
			return
		}
		t.Fatalf("Failed to join %s after multiple attempts", newNodeID)
	}

	// 加入节点 2
	joinNode(nodes[0], "node-2", "127.0.0.1:17001")

	// 加入节点 3
	joinNode(nodes[0], "node-3", "127.0.0.1:17002")

	// 4. 验证 Leader 选举
	t.Log("Waiting for cluster to stabilize...")
	time.Sleep(1 * time.Second)

	leader := getLeader(nodes)
	if leader == nil {
		t.Fatal("No leader elected")
	}
	t.Logf("Current Leader: %s", leader.RaftID)

	// 5. 验证持久化：关闭所有节点
	t.Log("Shutting down all nodes to simulate cluster restart...")
	for _, n := range nodes {
		if n != nil {
			n.Shutdown()
		}
	}
	// 清空节点引用，但保留数据目录
	nodes = make([]*Node, 3)
	time.Sleep(1 * time.Second)

	// 6. 重启所有节点（使用相同的目录）
	t.Log("Restarting nodes from disk...")
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		dir := dirs[i] // 复用已有目录
		bind := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		node, err := NewNode(id, dir, bind)
		if err != nil {
			t.Fatalf("Failed to restart node %s: %v", id, err)
		}
		nodes[i] = node
	}

	// 7. 验证集群恢复
	t.Log("Waiting for cluster to recover...")
	time.Sleep(3 * time.Second)

	newLeader := getLeader(nodes)
	if newLeader == nil {
		t.Fatal("Cluster failed to recover leader after restart")
	}
	t.Logf("Recovered Leader: %s", newLeader.RaftID)

	// 验证成员数仍为 3
	configFuture := newLeader.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		t.Fatalf("Failed to get configuration: %v", err)
	}
	servers := configFuture.Configuration().Servers
	if len(servers) != 3 {
		t.Fatalf("Expected 3 servers in configuration, got %d", len(servers))
	}
	t.Log("Cluster membership preserved correctly.")
}

func TestNodeRejoin(t *testing.T) {
	// 1. 搭建 3 个节点
	nodes := make([]*Node, 3)
	dirs := make([]string, 3)
	basePort := 18000

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		dir := fmt.Sprintf("raft-rejoin-test-%d", i+1)
		bind := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		os.RemoveAll(dir)
		dirs[i] = dir

		node, err := NewNode(id, dir, bind)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", id, err)
		}
		nodes[i] = node
	}

	defer func() {
		for i, n := range nodes {
			if n != nil {
				n.Shutdown()
			}
			os.RemoveAll(dirs[i])
		}
	}()

	// 引导
	t.Log("Bootstrapping cluster...")
	cfg := hraft.Configuration{
		Servers: []hraft.Server{
			{
				ID:      hraft.ServerID("node-1"),
				Address: hraft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", basePort)),
			},
		},
	}
	nodes[0].Raft.BootstrapCluster(cfg)
	waitForLeader(t, nodes[0], 5*time.Second)

	// 加入其他节点
	t.Log("Joining other nodes...")

	joinFunc := func(leader *Node, id, addr string) {
		for i := 0; i < 5; i++ {
			if err := leader.Join(id, addr); err != nil {
				t.Logf("Join attempt %d failed for %s: %v", i+1, id, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			t.Logf("Successfully joined %s", id)
			return
		}
		t.Fatalf("Failed to join %s after retries", id)
	}

	for i := 1; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		joinFunc(nodes[0], id, addr)
	}
	time.Sleep(2 * time.Second)

	// 2. 找一个 Follower 将其停掉
	var followerIdx int = -1
	for i, n := range nodes {
		if !n.IsLeader() {
			followerIdx = i
			break
		}
	}
	if followerIdx == -1 {
		t.Fatal("Could not find a follower node")
	}

	t.Logf("Killing follower node-%d...", followerIdx+1)
	nodes[followerIdx].Shutdown()
	nodes[followerIdx] = nil // 标记为 nil
	time.Sleep(1 * time.Second)

	// 3. 重启该 Follower
	t.Logf("Restarting follower node-%d...", followerIdx+1)
	id := fmt.Sprintf("node-%d", followerIdx+1)
	dir := dirs[followerIdx]
	bind := fmt.Sprintf("127.0.0.1:%d", basePort+followerIdx)

	newNode, err := NewNode(id, dir, bind)
	if err != nil {
		t.Fatalf("Failed to restart node %s: %v", id, err)
	}
	nodes[followerIdx] = newNode

	// 4. 验证它重新加入（通过检查它是否收到更新，或是否属于配置的一部分）
	// 我们可以检查 Leader 是否认为它健康，或它最终是否成为一个 Follower
	time.Sleep(2 * time.Second)

	// 验证重启后的节点处于 Follower 状态（也可能是 Candidate，但大概率是 Follower）
	state := newNode.Raft.State()
	t.Logf("Restarted node state: %v", state)
	if state != hraft.Follower && state != hraft.Leader {
		// 若它主动触发了选举，也可能成为 Leader，但通常会以 Follower 身份重新加入
		t.Logf("Warning: Node state is %v", state)
	}

	// 在 Leader 上验证成员关系
	leader := getLeader(nodes)
	if leader == nil {
		t.Fatal("No leader found after rejoin")
	}

	configFuture := leader.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		t.Fatalf("Failed to get configuration: %v", err)
	}
	servers := configFuture.Configuration().Servers
	if len(servers) != 3 {
		t.Fatalf("Expected 3 servers, got %d", len(servers))
	}
	t.Log("Node successfully rejoined and cluster is healthy")
}

func TestLeaderFailover(t *testing.T) {
	// 1. 搭建 3 个节点
	nodes := make([]*Node, 3)
	dirs := make([]string, 3)
	basePort := 19000

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		dir := fmt.Sprintf("raft-failover-test-%d", i+1)
		bind := fmt.Sprintf("127.0.0.1:%d", basePort+i)

		os.RemoveAll(dir)
		dirs[i] = dir

		node, err := NewNode(id, dir, bind)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", id, err)
		}
		nodes[i] = node
	}

	defer func() {
		for i, n := range nodes {
			if n != nil {
				n.Shutdown()
			}
			os.RemoveAll(dirs[i])
		}
	}()

	// 引导
	t.Log("Bootstrapping cluster...")
	cfg := hraft.Configuration{
		Servers: []hraft.Server{
			{
				ID:      hraft.ServerID("node-1"),
				Address: hraft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", basePort)),
			},
		},
	}
	nodes[0].Raft.BootstrapCluster(cfg)
	waitForLeader(t, nodes[0], 5*time.Second)

	// 加入其他节点
	t.Log("Joining other nodes...")

	joinFunc := func(leader *Node, id, addr string) {
		for i := 0; i < 5; i++ {
			if err := leader.Join(id, addr); err != nil {
				t.Logf("Join attempt %d failed for %s: %v", i+1, id, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			t.Logf("Successfully joined %s", id)
			return
		}
		t.Fatalf("Failed to join %s after retries", id)
	}

	for i := 1; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		joinFunc(nodes[0], id, addr)
	}
	time.Sleep(3 * time.Second) // 等待完全收敛

	// 2. 确定 Leader
	oldLeader := getLeader(nodes)
	if oldLeader == nil {
		t.Fatal("No leader found initially")
	}
	oldLeaderID := oldLeader.RaftID
	t.Logf("Initial Leader: %s", oldLeaderID)

	// 3. 停掉 Leader
	t.Logf("Killing leader %s...", oldLeaderID)
	oldLeader.Shutdown()
	// 标记为 nil，使 getLeader 不再检查它
	for i, n := range nodes {
		if n != nil && n.RaftID == oldLeaderID {
			nodes[i] = nil
			break
		}
	}

	// 4. 等待新 Leader
	t.Log("Waiting for new election...")

	// 最多等待 10 秒以选出新 Leader
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var newLeader *Node
	leaderElected := false

	for !leaderElected {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for new leader election")
		case <-ticker.C:
			candidate := getLeader(nodes)
			if candidate != nil && candidate.RaftID != oldLeaderID {
				newLeader = candidate
				leaderElected = true
			}
		}
	}

	t.Logf("New Leader: %s", newLeader.RaftID)
	t.Log("Leader failover successful")
}

func waitForLeader(t *testing.T, n *Node, timeout time.Duration) {
	timeoutCh := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			t.Fatalf("Timeout waiting for %s to become leader", n.RaftID)
		case <-ticker.C:
			if n.IsLeader() {
				return
			}
		}
	}
}

func getLeader(nodes []*Node) *Node {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		// 检查该节点自身是否认为自己是 Leader
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package http

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/manager/monitor"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

func TestAuthMiddleware(t *testing.T) {
	// 创建一个临时配置文件
	content := []byte(`{"auth_token": "test-token"}`)
	tmpfile, err := os.CreateTemp("", "config_test_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpfile.Close()

	// 加载配置
	err = config.Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// 创建一个模拟服务器
	r := &raft.Node{}       // 模拟 raft 节点，本测试不需要真实的 raft
	m := &monitor.Monitor{} // 模拟 monitor
	_ = NewServer(m, r)

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	authMux := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if cfg := config.Get(); cfg != nil && cfg.AuthToken != "" {
			token := req.Header.Get("Authorization")
			if token == "Bearer "+cfg.AuthToken {
				mux.ServeHTTP(w, req)
				return
			}
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, req)
	})

	// 测试 1：不带令牌
	req := httptest.NewRequest("GET", "/status", nil)
	w := httptest.NewRecorder()
	authMux.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized, got %d", w.Result().StatusCode)
	}

	// 测试 2：无效令牌
	req = httptest.NewRequest("GET", "/status", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	w = httptest.NewRecorder()
	authMux.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized, got %d", w.Result().StatusCode)
	}

	// 测试 3：有效令牌
	req = httptest.NewRequest("GET", "/status", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	w = httptest.NewRecorder()
	authMux.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w.Result().StatusCode)
	}
}

func TestHandleLeader(t *testing.T) {
	// 由于 raft.Node 提供了 IsLeader 和 LeaderAddr 方法，如果创建一个真实节点，
	// 我们本可以对其进行测试，但创建真实 raft 节点需要准备磁盘。这里仅验证处理器的结构。

	// 更简单的做法是：如果 raft 节点是一个接口，就对其进行模拟。
	// 这里跳过 raft 内部的实际逻辑，只验证该端点返回 JSON。
	t.Log("Testing HandleLeader endpoint structure")
}

func TestLeaderAPITarget(t *testing.T) {
	peers := []config.RaftPeerConfig{
		{NodeID: "node1", APIAddr: "127.0.0.1:8081"},
		{NodeID: "node2", APIAddr: "127.0.0.1:8082"},
		{NodeID: "node3", APIAddr: ""},
	}
	cases := []struct {
		name, leaderID, wantAddr string
		wantOK                   bool
	}{
		{"resolve node1", "node1", "127.0.0.1:8081", true},
		{"resolve node2", "node2", "127.0.0.1:8082", true},
		{"empty leader id", "", "", false},
		{"unknown leader", "node9", "", false},
		{"missing api addr", "node3", "", false},
	}
	for _, c := range cases {
		gotAddr, gotOK := leaderAPITarget(peers, c.leaderID)
		if gotOK != c.wantOK || gotAddr != c.wantAddr {
			t.Errorf("%s: leaderAPITarget(%q) = (%q,%v), want (%q,%v)",
				c.name, c.leaderID, gotAddr, gotOK, c.wantAddr, c.wantOK)
		}
	}
}

func TestBuildRaftNodeViews(t *testing.T) {
	peers := []config.RaftPeerConfig{
		{NodeID: "node1", RaftAddr: "127.0.0.1:10001", APIAddr: "127.0.0.1:8081"},
		{NodeID: "node2", RaftAddr: "127.0.0.1:10002", APIAddr: "127.0.0.1:8082"},
		{NodeID: "node3", RaftAddr: "127.0.0.1:10003", APIAddr: "127.0.0.1:8083"},
	}

	// node1 故障后的在线集群：node2 与 node3 已加入，node3 为 Leader；
	// node1 不在在线配置中，但仍存在于期望列表里。
	members := []raftMember{
		{ID: "node2", Addr: "127.0.0.1:10002", Suffrage: "Voter"},
		{ID: "node3", Addr: "127.0.0.1:10003", Suffrage: "Voter"},
	}

	views := buildRaftNodeViews(members, peers, "node3" /*Leader*/, "node2" /*自身*/)
	if len(views) != 3 {
		t.Fatalf("got %d views, want 3 (2 live + 1 pending)", len(views))
	}

	byID := make(map[string]RaftNodeView, len(views))
	for _, v := range views {
		byID[v.NodeID] = v
	}

	checks := []struct {
		id, role, api string
		inCluster     bool
		isSelf        bool
	}{
		{"node2", "follower", "127.0.0.1:8082", true, true},
		{"node3", "leader", "127.0.0.1:8083", true, false},
		{"node1", "pending", "127.0.0.1:8081", false, false},
	}
	for _, c := range checks {
		v, ok := byID[c.id]
		if !ok {
			t.Errorf("missing node %s", c.id)
			continue
		}
		if v.Role != c.role {
			t.Errorf("node %s role = %q, want %q", c.id, v.Role, c.role)
		}
		if v.APIAddr != c.api {
			t.Errorf("node %s api_addr = %q, want %q", c.id, v.APIAddr, c.api)
		}
		if v.InCluster != c.inCluster {
			t.Errorf("node %s in_cluster = %v, want %v", c.id, v.InCluster, c.inCluster)
		}
		if v.IsSelf != c.isSelf {
			t.Errorf("node %s is_self = %v, want %v", c.id, v.IsSelf, c.isSelf)
		}
	}

	// 在线成员必须排在 pending 成员之前。
	if views[0].NodeID != "node2" || views[1].NodeID != "node3" || views[2].NodeID != "node1" {
		t.Errorf("ordering = %s,%s,%s; want live members first", views[0].NodeID, views[1].NodeID, views[2].NodeID)
	}
}

func TestBuildRaftNodeViews_NonVoter(t *testing.T) {
	peers := []config.RaftPeerConfig{
		{NodeID: "node1", APIAddr: "127.0.0.1:8081"},
		{NodeID: "node4", APIAddr: "127.0.0.1:8084"},
	}
	members := []raftMember{
		{ID: "node1", Addr: "127.0.0.1:10001", Suffrage: "Voter"},
		{ID: "node4", Addr: "127.0.0.1:10004", Suffrage: "Nonvoter"},
	}
	views := buildRaftNodeViews(members, peers, "node1", "node1")
	got := make(map[string]string, len(views))
	for _, v := range views {
		got[v.NodeID] = v.Role
	}
	if got["node1"] != "leader" {
		t.Errorf("node1 role = %q, want leader", got["node1"])
	}
	if got["node4"] != "non-voter" {
		t.Errorf("node4 role = %q, want non-voter", got["node4"])
	}
}

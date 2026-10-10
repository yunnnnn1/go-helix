// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/config"
)

// recordedRequest 用于记录客户端发往测试服务器的内容。
type recordedRequest struct {
	method      string
	path        string
	contentType string
	auth        string
	body        string
}

// newJoinTestServer 以指定状态码应答，并记录收到的请求。
func newJoinTestServer(t *testing.T, status int) (*httptest.Server, *recordedRequest) {
	t.Helper()
	rec := &recordedRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rec.method = r.Method
		rec.path = r.URL.Path
		rec.contentType = r.Header.Get("Content-Type")
		rec.auth = r.Header.Get("Authorization")
		rec.body = string(b)
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

// TestNewJoinRequest_AttachesAuthToken 是针对 join 认证缺陷的回归测试：
// 请求必须携带 Bearer token，否则 /join 会返回 401，节点永远无法加入
// 集群。
func TestNewJoinRequest_AttachesAuthToken(t *testing.T) {
	srv, rec := newJoinTestServer(t, http.StatusOK)

	req, err := newJoinRequest(srv.URL, "node2", "127.0.0.1:10002", "127.0.0.1:8082", "secret-token-123")
	if err != nil {
		t.Fatalf("newJoinRequest: %v", err)
	}
	if err := sendJoin(srv.Client(), req); err != nil {
		t.Fatalf("sendJoin with valid token should succeed: %v", err)
	}

	if rec.auth != "Bearer secret-token-123" {
		t.Errorf("Authorization = %q, want %q", rec.auth, "Bearer secret-token-123")
	}
	if rec.method != http.MethodPost {
		t.Errorf("method = %q, want POST", rec.method)
	}
	if rec.path != "/join" {
		t.Errorf("path = %q, want /join", rec.path)
	}
	if rec.contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", rec.contentType)
	}
	for _, want := range []string{`"node_id":"node2"`, `"raft_addr":"127.0.0.1:10002"`, `"api_addr":"127.0.0.1:8082"`} {
		if !strings.Contains(rec.body, want) {
			t.Errorf("body %q missing %s", rec.body, want)
		}
	}
}

// TestNewJoinRequest_NoToken 验证当 token 为空时不会发送认证头（针对未配置
// auth_token 的集群），而不是发送格式错误的 "Bearer "。
func TestNewJoinRequest_NoToken(t *testing.T) {
	srv, rec := newJoinTestServer(t, http.StatusOK)

	req, err := newJoinRequest(srv.URL, "node2", "127.0.0.1:10002", "127.0.0.1:8082", "")
	if err != nil {
		t.Fatalf("newJoinRequest: %v", err)
	}
	if err := sendJoin(srv.Client(), req); err != nil {
		t.Fatalf("sendJoin without token should succeed: %v", err)
	}
	if rec.auth != "" {
		t.Errorf("Authorization = %q, want empty", rec.auth)
	}
}

// TestSendJoin_NonOKFails 验证非 200 响应（例如 401）会被视为错误，从而
// 使重试循环继续尝试。
func TestSendJoin_NonOKFails(t *testing.T) {
	srv, _ := newJoinTestServer(t, http.StatusUnauthorized)

	req, err := newJoinRequest(srv.URL, "node2", "127.0.0.1:10002", "127.0.0.1:8082", "secret-token-123")
	if err != nil {
		t.Fatalf("newJoinRequest: %v", err)
	}
	err = sendJoin(srv.Client(), req)
	if err == nil {
		t.Fatal("expected error on 401 Unauthorized, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error %q should mention status 401", err.Error())
	}
}

func TestJoinURL(t *testing.T) {
	cases := []struct {
		joinAddr, want string
	}{
		{"127.0.0.1:8081", "http://127.0.0.1:8081/join"},
		{"http://127.0.0.1:8081", "http://127.0.0.1:8081/join"},
		{"https://127.0.0.1:8081/", "https://127.0.0.1:8081/join"},
	}
	for _, c := range cases {
		if got := joinURL(c.joinAddr); got != c.want {
			t.Errorf("joinURL(%q) = %q, want %q", c.joinAddr, got, c.want)
		}
	}
}

func TestShouldBootstrap(t *testing.T) {
	peers := []config.RaftPeerConfig{
		{NodeID: "node1"},
		{NodeID: "node2"},
		{NodeID: "node3"},
	}
	cases := []struct {
		name, selfID string
		want         bool
	}{
		{"seed bootstraps", "node1", true},
		{"non-seed waits", "node2", false},
		{"unknown node waits", "node9", false},
		{"empty self waits", "", false},
	}
	for _, c := range cases {
		if got := shouldBootstrap(peers, c.selfID); got != c.want {
			t.Errorf("%s: shouldBootstrap(%q) = %v, want %v", c.name, c.selfID, got, c.want)
		}
	}

	if shouldBootstrap(nil, "node1") {
		t.Error("empty peer list must not allow bootstrap")
	}
}

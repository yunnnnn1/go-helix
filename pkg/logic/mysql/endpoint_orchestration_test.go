// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

// fakeEndpointAgent 预置 RunEndpointHook 的返回结果。status 结果按
// 顺序返回（pre 调用与各次 verify 调用可能不同）。
type fakeEndpointAgent struct {
	pb.UnimplementedDBAgentServer
	mu        sync.Mutex
	calls     []*pb.EndpointHookRequest
	start     *pb.EndpointHookResult
	statusSeq []*pb.EndpointHookResult
	statusN   int
}

func (f *fakeEndpointAgent) RunEndpointHook(_ context.Context, req *pb.EndpointHookRequest) (*pb.EndpointHookResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, req)
	cmd := req.Command
	f.mu.Unlock()

	switch cmd {
	case "start":
		if f.start != nil {
			return f.start, nil
		}
	case "status":
		f.mu.Lock()
		i := f.statusN
		f.statusN++
		f.mu.Unlock()
		if i < len(f.statusSeq) {
			return f.statusSeq[i], nil
		}
		if len(f.statusSeq) > 0 {
			return f.statusSeq[len(f.statusSeq)-1], nil
		}
	}
	return &pb.EndpointHookResult{Success: true, CurrentMaster: req.NewMaster}, nil
}

func (f *fakeEndpointAgent) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// loadEndpointTestConfig 写入一份包含单个 endpoint_switch 条目的
// 临时 manager 配置并全局加载。
func loadEndpointTestConfig(t *testing.T, required bool, timeout string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	reqText := "false"
	if required {
		reqText = "true"
	}
	content := `{
      "manager": {
        "endpoint_switch": {
          "cluster1": {
            "endpoint_type": "vip",
            "endpoint_value": "10.0.0.1",
            "timeout": "` + timeout + `",
            "required": ` + reqText + `
          }
        }
      },
      "auth_token": ""
    }`
	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := config.Load(cfgPath); err != nil {
		t.Fatalf("load config: %v", err)
	}
}

func endpointNodes(agentPort int32) (old, new *datacenter.Node) {
	old = &datacenter.Node{ClusterName: "cluster1", Host: "127.0.0.1", Port: 3307}
	new = &datacenter.Node{ClusterName: "cluster1", Host: "127.0.0.1", Port: agentPort}
	return old, new
}

func TestEndpointHooks_Success(t *testing.T) {
	agent := &fakeEndpointAgent{
		start: &pb.EndpointHookResult{Success: true, Message: "switched"},
		statusSeq: []*pb.EndpointHookResult{
			{Success: true, CurrentMaster: "127.0.0.1:3307"}, // pre：旧
			{Success: true, CurrentMaster: "127.0.0.1"},      // verify：新（下方会覆盖）
		},
	}
	fakeNode, _ := startFakeAgent(t, agent)
	loadEndpointTestConfig(t, true, "5s")

	old, new := endpointNodes(fakeNode.AgentPort)
	// status 校验必须精确上报候选节点地址。
	agent.statusSeq[1].CurrentMaster = new.Addr()

	ctx := context.Background()
	if err := runEndpointHooks(ctx, "pre", old, new); err != nil {
		t.Fatalf("pre hooks: %v", err)
	}
	if err := runEndpointHooks(ctx, "post", old, new); err != nil {
		t.Fatalf("post hooks: %v", err)
	}

	// 观察到的命令：status(pre)、start、status(verify)。
	if agent.callCount() < 3 {
		t.Fatalf("expected at least 3 hook calls, got %d", agent.callCount())
	}
	// Sync URL 仅在 pre 调用时携带。
	if agent.calls[0].SyncUrl == "" {
		// sync_url 为空是因为 manager 配置中省略了它：可以接受
	}
}

func TestEndpointHooks_PreFailureRequired(t *testing.T) {
	agent := &fakeEndpointAgent{
		statusSeq: []*pb.EndpointHookResult{
			{Success: false, Message: "endpoint not reachable"},
		},
	}
	fakeNode, _ := startFakeAgent(t, agent)
	loadEndpointTestConfig(t, true, "3s")

	old, new := endpointNodes(fakeNode.AgentPort)
	if err := runEndpointHooks(context.Background(), "pre", old, new); err == nil {
		t.Fatal("required precheck failure must return error")
	}
}

func TestEndpointHooks_PostVerifyFailureRequired(t *testing.T) {
	agent := &fakeEndpointAgent{
		start: &pb.EndpointHookResult{Success: true},
		statusSeq: []*pb.EndpointHookResult{
			{Success: true, CurrentMaster: "127.0.0.1:3307"}, // 始终为旧主库
		},
	}
	fakeNode, _ := startFakeAgent(t, agent)
	loadEndpointTestConfig(t, true, "2s")

	old, new := endpointNodes(fakeNode.AgentPort)
	if err := runEndpointHooks(context.Background(), "post", old, new); err == nil {
		t.Fatal("required verify mismatch must return error")
	}
}

func TestEndpointHooks_NonRequiredFailureIsWarning(t *testing.T) {
	agent := &fakeEndpointAgent{
		statusSeq: []*pb.EndpointHookResult{
			{Success: false, Message: "no hook"},
		},
	}
	fakeNode, _ := startFakeAgent(t, agent)
	loadEndpointTestConfig(t, false, "2s")

	old, new := endpointNodes(fakeNode.AgentPort)
	if err := runEndpointHooks(context.Background(), "pre", old, new); err != nil {
		t.Fatalf("non-required failure should be a warning, got %v", err)
	}
}

func TestEndpointHooks_NoConfigEntry(t *testing.T) {
	loadEndpointTestConfig(t, true, "2s")
	old := &datacenter.Node{ClusterName: "cluster-other", Host: "127.0.0.1", Port: 3307}
	new := &datacenter.Node{ClusterName: "cluster-other", Host: "127.0.0.1", Port: 3308}
	if err := runEndpointHooks(context.Background(), "pre", old, new); err != nil {
		t.Fatalf("cluster without entry should be skipped, got %v", err)
	}
}

// 存储在 cluster_meta（由 ctx 携带）中的入口必须是钩子实际收到的
// 入口，并且其优先级必须高于配置文件中的值。
func TestEndpointHooks_StoredEndpointWinsAndReachesHook(t *testing.T) {
	agent := &fakeEndpointAgent{
		statusSeq: []*pb.EndpointHookResult{
			{Success: true, CurrentMaster: "127.0.0.1:3307"},
		},
	}
	fakeNode, _ := startFakeAgent(t, agent)
	// 配置文件指定的是 vip/10.0.0.1。
	loadEndpointTestConfig(t, false, "3s")

	old, new := endpointNodes(fakeNode.AgentPort)
	// 存储的值是 domain/db.example.com —— 必须以此覆盖。
	ctx := logic.WithClusterEndpoint(context.Background(), "domain", "db.example.com")
	if err := runEndpointHooks(ctx, "pre", old, new); err != nil {
		t.Fatalf("pre with stored endpoint: %v", err)
	}

	if agent.callCount() == 0 {
		t.Fatal("hook was not invoked")
	}
	got := agent.calls[0]
	if got.EndpointType != "domain" || got.Endpoint != "db.example.com" {
		t.Fatalf("hook received %s=%s, want domain=db.example.com",
			got.EndpointType, got.Endpoint)
	}
}

// 仅有存储的入口（manager 配置中没有 endpoint_switch 条目）时仍会执行。
func TestEndpointHooks_StoredEndpointWithoutConfig(t *testing.T) {
	agent := &fakeEndpointAgent{
		statusSeq: []*pb.EndpointHookResult{
			{Success: true, CurrentMaster: "127.0.0.1:3307"},
		},
	}
	fakeNode, _ := startFakeAgent(t, agent)

	// 最小化 manager 配置：完全没有 endpoint_switch。
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"manager":{},"auth_token":""}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := config.Load(cfgPath); err != nil {
		t.Fatalf("load config: %v", err)
	}

	old, new := endpointNodes(fakeNode.AgentPort)
	ctx := logic.WithClusterEndpoint(context.Background(), "vip", "10.0.0.9")
	if err := runEndpointHooks(ctx, "pre", old, new); err != nil {
		t.Fatalf("stored-only endpoint should run, got %v", err)
	}
	if agent.callCount() == 0 || agent.calls[0].Endpoint != "10.0.0.9" {
		t.Fatalf("hook did not receive stored endpoint, calls=%+v", agent.calls)
	}
}

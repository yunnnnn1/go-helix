// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

// fakeFenceAgent 预置围栏门禁所用的 Ping / SetReadOnly / GetStatus
// 行为。
type fakeFenceAgent struct {
	pb.UnimplementedDBAgentServer

	alive         bool   // Ping 结果
	pingErr       error  // 可选的 Ping 错误
	setROErr      error  // SetReadOnly 结果
	superReadOnly string // 在 status key 下返回的值
}

func (f *fakeFenceAgent) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	if f.pingErr != nil {
		return nil, f.pingErr
	}
	return &pb.PingResponse{Alive: f.alive}, nil
}

func (f *fakeFenceAgent) ExecuteOperation(_ context.Context, req *pb.OperationRequest) (*pb.OperationResponse, error) {
	if req.Operation == "SetReadOnly" && f.setROErr != nil {
		return nil, f.setROErr
	}
	return &pb.OperationResponse{Success: true}, nil
}

func (f *fakeFenceAgent) GetStatus(_ context.Context, _ *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	return &pb.GetStatusResponse{Status: map[string]string{
		"super_read_only": f.superReadOnly,
	}}, nil
}

// loadFenceTestConfig 加载一份使用指定围栏模式的 manager 配置。
// mode：""（默认安全）、"best_effort"、"disabled"。
func loadFenceTestConfig(t *testing.T, mode string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")

	fenceBlock := ""
	switch mode {
	case "best_effort":
		fenceBlock = `"fencing": {"enabled": true, "mode": "best_effort", "timeout": "2s"}`
	case "disabled":
		fenceBlock = `"fencing": {"enabled": false, "mode": "best_effort", "timeout": "2s"}`
	default:
		fenceBlock = `"fencing": {"enabled": true, "mode": "required", "timeout": "2s"}`
	}
	content := `{
      "manager": { ` + fenceBlock + ` },
      "auth_token": ""
    }`
	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := config.Load(cfgPath); err != nil {
		t.Fatalf("load config: %v", err)
	}
}

func TestGateFencing_ConfirmedFrozen(t *testing.T) {
	agent := &fakeFenceAgent{alive: true, superReadOnly: "ON"}
	node, _ := startFakeAgent(t, agent)
	loadFenceTestConfig(t, "required")

	if err := gateFencing(context.Background(), node); err != nil {
		t.Fatalf("expected confirmed fence to pass, got %v", err)
	}
}

func TestGateFencing_ConfirmedDeadDB(t *testing.T) {
	// Ping 显示 DB 不存活：门禁必须在不冻结的情况下通过。
	agent := &fakeFenceAgent{alive: false, superReadOnly: ""}
	node, _ := startFakeAgent(t, agent)
	loadFenceTestConfig(t, "required")

	if err := gateFencing(context.Background(), node); err != nil {
		t.Fatalf("confirmed dead DB should be safe, got %v", err)
	}
}

func TestGateFencing_ReadBackNotOn_Aborts(t *testing.T) {
	// DB 存活、SET 成功，但回读为 OFF -> required 中止。
	agent := &fakeFenceAgent{alive: true, superReadOnly: "OFF"}
	node, _ := startFakeAgent(t, agent)
	loadFenceTestConfig(t, "required")

	if err := gateFencing(context.Background(), node); err == nil {
		t.Fatal("required mode must abort when super_read_only read-back is not ON")
	}
}

func TestGateFencing_FreezeError_Aborts(t *testing.T) {
	agent := &fakeFenceAgent{alive: true, setROErr: context.DeadlineExceeded}
	node, _ := startFakeAgent(t, agent)
	loadFenceTestConfig(t, "required")

	if err := gateFencing(context.Background(), node); err == nil {
		t.Fatal("required mode must abort when the freeze SET fails")
	}
}

func TestGateFencing_UnreachableAgent_Aborts(t *testing.T) {
	loadFenceTestConfig(t, "required")
	// 先获取一个空闲端口再释放它，使节点指向一个没有任何
	// 监听的端口。
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()
	node := &datacenter.Node{Host: "127.0.0.1", AgentPort: int32(port)}

	if err := gateFencing(context.Background(), node); err == nil {
		t.Fatal("required mode must abort when the old master agent is unreachable")
	}
}

func TestGateFencing_BestEffort_Proceeds(t *testing.T) {
	agent := &fakeFenceAgent{alive: true, superReadOnly: "OFF"}
	node, _ := startFakeAgent(t, agent)
	loadFenceTestConfig(t, "best_effort")

	if err := gateFencing(context.Background(), node); err != nil {
		t.Fatalf("best_effort should proceed on uncertain fence, got %v", err)
	}
}

func TestGateFencing_Disabled_Proceeds(t *testing.T) {
	agent := &fakeFenceAgent{alive: true, superReadOnly: "OFF"}
	node, _ := startFakeAgent(t, agent)
	loadFenceTestConfig(t, "disabled")

	if err := gateFencing(context.Background(), node); err != nil {
		t.Fatalf("disabled gate should proceed, got %v", err)
	}
}

// ---- 多级证据链（L2 直连 / L3 对等）----

// fakeFenceProbers 是用于证据链测试的预置 logic.FenceProbers。
type fakeFenceProbers struct {
	directDB bool
	host     bool
	peer     bool
	hasPeer  bool
}

func (f *fakeFenceProbers) DirectDBReachable(_ *datacenter.Node) bool { return f.directDB }
func (f *fakeFenceProbers) HostReachable(_ *datacenter.Node) bool     { return f.host }
func (f *fakeFenceProbers) PeerAgentProbe(_ *datacenter.Node, _ int32) (bool, bool) {
	return f.peer, f.hasPeer
}

// deadAgentNode 返回一个自身 Agent 端口无监听的节点，使 L1
// 失败，证据链继续进入 L2/L3。
func deadAgentNode(t *testing.T) *datacenter.Node {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()
	return &datacenter.Node{ClusterName: "cluster1", Host: "127.0.0.1", Port: 3307, AgentPort: int32(port)}
}

func TestChain_L2_BothUnreachable_Confirmed(t *testing.T) {
	logic.SetFenceProbers(&fakeFenceProbers{directDB: false, host: false, peer: false, hasPeer: true})
	t.Cleanup(func() { logic.SetFenceProbers(nil) })
	loadFenceTestConfig(t, "required")

	node := deadAgentNode(t)
	if err := gateFencing(context.Background(), node); err != nil {
		t.Fatalf("host+DB unreachable should confirm, got %v", err)
	}
}

func TestChain_L2_HostUpDBUnreachable_Uncertain_Aborts(t *testing.T) {
	logic.SetFenceProbers(&fakeFenceProbers{directDB: false, host: true, hasPeer: false})
	t.Cleanup(func() { logic.SetFenceProbers(nil) })
	loadFenceTestConfig(t, "required")

	node := deadAgentNode(t)
	if err := gateFencing(context.Background(), node); err == nil {
		t.Fatal("host reachable but DB unconfirmed must abort (possible split-brain)")
	}
}

func TestChain_L2_DirectDBAlive_Aborts(t *testing.T) {
	logic.SetFenceProbers(&fakeFenceProbers{directDB: true, host: true})
	t.Cleanup(func() { logic.SetFenceProbers(nil) })
	loadFenceTestConfig(t, "required")

	node := deadAgentNode(t)
	if err := gateFencing(context.Background(), node); err == nil {
		t.Fatal("direct DB reachable proves old master is alive and not fenced; must abort")
	}
}

func TestChain_L3_PeerReportsDown_Confirmed(t *testing.T) {
	// L2 不确定（主机可达、DB 未确认），L3 对等节点存在并报告 DB 关闭。
	logic.SetFenceProbers(&fakeFenceProbers{directDB: false, host: true, peer: false, hasPeer: true})
	t.Cleanup(func() { logic.SetFenceProbers(nil) })
	loadFenceTestConfig(t, "required")

	node := deadAgentNode(t)
	if err := gateFencing(context.Background(), node); err != nil {
		t.Fatalf("peer reporting DB unreachable should confirm, got %v", err)
	}
}

func TestChain_L3_PeerReportsAlive_Aborts(t *testing.T) {
	// L2 不确定，L3 对等节点证实 DB 可达 -> 强制中止。
	logic.SetFenceProbers(&fakeFenceProbers{directDB: false, host: false, peer: true, hasPeer: true})
	t.Cleanup(func() { logic.SetFenceProbers(nil) })
	loadFenceTestConfig(t, "required")

	node := deadAgentNode(t)
	if err := gateFencing(context.Background(), node); err == nil {
		t.Fatal("peer reporting DB reachable is hard evidence of a live old master; must abort")
	}
}

func TestChain_NoProbers_FallsBackToL1_Aborts(t *testing.T) {
	logic.SetFenceProbers(nil)
	loadFenceTestConfig(t, "required")

	node := deadAgentNode(t)
	if err := gateFencing(context.Background(), node); err == nil {
		t.Fatal("with no probers and L1 agent down, required must abort")
	}
}

func TestChain_L2_BestEffort_Proceeds(t *testing.T) {
	// 在 best_effort 下，即使明显不确定的 L2/L3 也会被放行。
	logic.SetFenceProbers(&fakeFenceProbers{directDB: false, host: true})
	t.Cleanup(func() { logic.SetFenceProbers(nil) })
	loadFenceTestConfig(t, "best_effort")

	node := deadAgentNode(t)
	if err := gateFencing(context.Background(), node); err != nil {
		t.Fatalf("best_effort must proceed on uncertain evidence, got %v", err)
	}
}

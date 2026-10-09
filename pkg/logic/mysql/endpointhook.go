package mysql

import (
	"context"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/logic"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

// hookEndpointSwitcher 基于 Agent 的 RunEndpointHook RPC 实现
// logic.EndpointSwitcher。Agent 从其本地配置解析真正的可执行文件；
// 此 provider 只负责承载切换上下文。
type hookEndpointSwitcher struct{}

func init() {
	logic.RegisterEndpointSwitcher("mysql", &hookEndpointSwitcher{})
}

// RunHook 连接目标 agent 并执行一条入口钩子命令。
func (h *hookEndpointSwitcher) RunHook(ctx context.Context, req *logic.EndpointHookRequest) (*logic.EndpointHookResult, error) {
	// 默认执行主机 = 新主库主机（带其 agent 端口）。
	addr := req.ExecHost
	if addr == "" {
		addr = req.NewMaster
	}

	conn, client, err := dialAgentClient(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	timeoutSeconds := int32(timeout / time.Second)
	if timeoutSeconds <= 0 {
		timeoutSeconds = 15
	}

	resp, err := client.RunEndpointHook(ctx, &pb.EndpointHookRequest{
		Command:        req.Command,
		Cluster:        req.Cluster,
		EndpointType:   req.EndpointType,
		Endpoint:       req.Endpoint,
		OldMaster:      req.OldMaster,
		NewMaster:      req.NewMaster,
		SyncUrl:        req.SyncURL,
		Sha256:         req.Sha256,
		TimeoutSeconds: timeoutSeconds,
	})
	if err != nil {
		return nil, err
	}
	return &logic.EndpointHookResult{
		Success:       resp.Success,
		Message:       resp.Message,
		CurrentMaster: resp.CurrentMaster,
	}, nil
}

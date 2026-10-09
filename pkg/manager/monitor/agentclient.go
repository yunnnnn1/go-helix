package monitor

import (
	"context"
	"fmt"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// AgentClient 针对给定的 Agent gRPC 地址（host:agentPort）返回带缓存的
// DBAgent 客户端。连接以 grpcConns 为键，供调查 worker 与流式 HTTP
// 端点共同复用。
//
// 配置了鉴权 token 时，每个 RPC（包括服务端流式调用）都会附带该
// token（worker 的缓存拨号器只注入了 unary 拦截器；流式端点还需要
// stream 拦截器）。
func (m *Monitor) AgentClient(agentAddr string) (proto.DBAgentClient, error) {
	if conn, ok := m.grpcConns.Load(agentAddr); ok {
		return proto.NewDBAgentClient(conn.(*grpc.ClientConn)), nil
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	if cfg := config.Get(); cfg != nil && cfg.AuthToken != "" {
		token := "Bearer " + cfg.AuthToken
		authUnary := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", token)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}
		authStream := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", token)
			return streamer(ctx, desc, cc, method, callOpts...)
		}
		opts = append(opts,
			grpc.WithUnaryInterceptor(authUnary),
			grpc.WithStreamInterceptor(authStream))
	}

	newConn, err := grpc.NewClient(agentAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("create agent grpc client for %s: %w", agentAddr, err)
	}

	actualConn, loaded := m.grpcConns.LoadOrStore(agentAddr, newConn)
	if loaded {
		newConn.Close() // 另一个 goroutine 在缓存竞争中胜出
	}
	return proto.NewDBAgentClient(actualConn.(*grpc.ClientConn)), nil
}

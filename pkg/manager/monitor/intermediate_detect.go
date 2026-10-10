package monitor

import (
	"context"
	"fmt"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/db/ops"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/proto"
	"github.com/yunnnnn1/go-helix/pkg/tlsutil"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// handleIntermediateInvestigation 在恢复之前，确认疑似已死的中间主
// 确实已死。它复刻了主库流程的多角度取证（Agent + 数据库健康、直接
// 数据库探测、对等节点二次检查），并要求连续多次确认，使瞬时探测失败
// 不会触发中间主恢复。
func (m *Monitor) handleIntermediateInvestigation(job *investigationJob) {
	node := job.Node

	provider, err := ops.GetProvider(node.DbType)
	if err != nil {
		log.Printf("No provider for db type %s: %v", node.DbType, err)
		return
	}

	checkTimeout := 2 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Manager.CheckTimeout != "" {
		if d, pErr := time.ParseDuration(cfg.Manager.CheckTimeout); pErr == nil {
			checkTimeout = d
		}
	}

	agentClient, closeClient, err := m.dialAgentNode(node)
	if closeClient != nil {
		defer closeClient()
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()

	if err == nil {
		// Agent 可达：通过它获取数据库健康状况。
		healthy, hErr := provider.CheckHealth(ctx, node, agentClient)
		switch {
		case hErr != nil:
			log.Printf("[IntermediateDetect] %s agent reachable but health RPC error: %v", node.Addr(), hErr)
		case healthy:
			// 误报：该中间主仍然存活。
			log.Printf("[IntermediateDetect] %s agent reports DB healthy; not dead, resetting confirmations", node.Addr())
			m.resetFailoverConfirmations(node.Addr())
			return
		default:
			log.Printf("[IntermediateDetect] %s agent reachable but DB unhealthy", node.Addr())
		}
	} else {
		// Agent 不可达：区分“仅 Agent 宕机”与真实故障。
		log.Printf("[IntermediateDetect] %s agent unreachable (%v); probing DB directly", node.Addr(), err)

		if m.directDBCheck(node) {
			log.Printf("[IntermediateDetect] direct DB probe for %s succeeded; agent-only down, no recovery", node.Addr())
			m.resetFailoverConfirmations(node.Addr())
			return
		}

		// 避免因网络分区而采取动作：询问集群中的对等节点。
		if cfg := config.Get(); cfg != nil && cfg.Manager.SecondCheck {
			if m.secondaryCheckHost(node.Host, node.ClusterName) {
				log.Printf("[IntermediateDetect] peer can reach %s; network partition suspected, no recovery", node.Addr())
				m.resetFailoverConfirmations(node.Addr())
				return
			}
		}
	}

	// 上面所有未提前返回的路径，都找到了中间主已死的证据；
	// 在恢复之前仍要求连续多次确认。
	attempt := m.bumpFailoverConfirmation(node.Addr())
	if attempt < requiredFailoverConfirmations {
		log.Printf("[IntermediateDetect] %s suspected dead, unconfirmed (%d/%d)",
			node.Addr(), attempt, requiredFailoverConfirmations)
		return
	}
	m.resetFailoverConfirmations(node.Addr())

	log.Printf("[IntermediateDetect] %s confirmed dead intermediate master after %d checks; recovering intermediate slot",
		node.Addr(), requiredFailoverConfirmations)

	m.HandleConfirmedFailureWithCode(logic.DeadIntermediateMaster, node, nil)
}

// dialAgentNode 与节点的 agent 建立一条短生命周期的 gRPC 连接。返回的
// close 函数用于释放连接；err != nil 表示 agent 不可达。已配置时会附带
// Token。RPC 层超时由调用方的 context 提供，而不在这里设置
// （连接采用懒拨号方式）。
func (m *Monitor) dialAgentNode(node *datacenter.Node) (proto.DBAgentClient, func(), error) {
	agentPort, err := deriveAgentPort(node)
	if err != nil {
		return nil, nil, fmt.Errorf("derive agent port: %w", err)
	}
	addr := fmt.Sprintf("%s:%d", node.Host, agentPort)

	transport, err := tlsutil.ClientDialOption()
	if err != nil {
		return nil, nil, fmt.Errorf("build agent tls: %w", err)
	}
	opts := []grpc.DialOption{transport}
	if cfg := config.Get(); cfg != nil && cfg.AuthToken != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{},
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+cfg.AuthToken)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial agent %s: %w", addr, err)
	}
	// 连接是懒建立的；agent 已死会在调用方的首个 RPC（CheckHealth）
	// 中暴露出来，调用方会将 RPC 错误视为证据。
	return proto.NewDBAgentClient(conn), func() { conn.Close() }, nil
}

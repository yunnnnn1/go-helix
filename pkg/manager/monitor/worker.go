package monitor

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/db"
	"github.com/yunnnnn1/go-helix/pkg/db/ops"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/proto"
	"github.com/yunnnnn1/go-helix/pkg/tlsutil"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// requiredFailoverConfirmations 表示在执行紧急切换之前，必须连续多少个
// 节拍一致判定主库不可达 / 未按数据库协议通信。单次握手失败或探测到
// 异常字节都被视为瞬时现象；只有持续、可复现的证据才会触发动作。
const requiredFailoverConfirmations = 3

func (m *Monitor) worker(id int) {
	log.Printf("Monitor Worker %d started", id)
	for {
		select {
		case job, ok := <-m.jobCh:
			if !ok {
				return
			}
			m.handleNodeInvestigation(job)
		case <-m.stopCh:
			return
		}
	}
}

func (m *Monitor) handleNodeInvestigation(job *investigationJob) {
	node := job.Node
	code := job.InitialCode

	// 中间主采用专门的多角度确认流程；本函数其余部分是
	// 主库恢复流程。
	if code == logic.DeadIntermediateMaster {
		m.handleIntermediateInvestigation(job)
		return
	}

	// 获取该节点类型对应的 Provider
	provider, err := ops.GetProvider(node.DbType)
	if err != nil {
		log.Printf("No provider for db type %s: %v", node.DbType, err)
		return
	}

	// 1. 通过 gRPC 尝试连接 Agent
	agentPort, err := deriveAgentPort(node)
	if err != nil {
		log.Print(err)
		return
	}
	agentAddr := fmt.Sprintf("%s:%d", node.Host, agentPort)

	var conn *grpc.ClientConn
	if cachedConn, ok := m.grpcConns.Load(agentAddr); ok {
		conn = cachedConn.(*grpc.ClientConn)
	} else {
		// 添加鉴权 Token
		transport, tErr := tlsutil.ClientDialOption()
		if tErr != nil {
			log.Printf("Failed to build tls for %s: %v", agentAddr, tErr)
			return
		}
		opts := []grpc.DialOption{transport}

		if cfg := config.Get(); cfg != nil && cfg.AuthToken != "" {
			opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+cfg.AuthToken)
				return invoker(ctx, method, req, reply, cc, opts...)
			}))
		}

		newConn, err := grpc.NewClient(agentAddr, opts...)
		if err != nil {
			log.Printf("Failed to create grpc client for %s: %v", agentAddr, err)
			return
		}

		// 存入缓存
		actualConn, loaded := m.grpcConns.LoadOrStore(agentAddr, newConn)
		if loaded {
			newConn.Close()
		}
		conn = actualConn.(*grpc.ClientConn)
	}

	// 如果 Agent 可达，则检查数据库状态。
	agentClient := proto.NewDBAgentClient(conn)

	// 2. 依据 Provider 逻辑要求 Agent 检查数据库状态
	checkTimeout := 2 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Manager.CheckTimeout != "" {
		if d, err := time.ParseDuration(cfg.Manager.CheckTimeout); err == nil {
			checkTimeout = d
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()

	healthy, err := provider.CheckHealth(ctx, node, agentClient)
	if err != nil {
		log.Printf("Health check RPC error (or failed to connect) to %s: %v. Initiating Bypass Check...", agentAddr, err)

		// 条件 1：Agent 不可达。
		// Manager 直接检查数据库。若数据库存活，只是 Agent 宕机，不做故障切换。
		log.Printf("Agent connection failed. Manager attempting direct short-connection to DB at %s:%d...", node.Host, node.Port)
		if m.directDBCheck(node) {
			log.Printf("Direct DB check successful. Agent is down, but DB is ALIVE. No Failover triggered.")
			return
		}

		// 连接失败时重置冲突计数（状态已变化）
		m.resetConflict(node.Host)

		// 条件 2：数据库与 Agent 均宕机。
		// 绕过检查（Ping 主机）。优先通过其他节点尝试，以避免网络分区导致脑裂。
		cfg := config.Get()
		if cfg != nil && cfg.Manager.SecondCheck {
			log.Printf("Bypass Check: Initiating Secondary Check via Slaves for %s...", node.Host)
			if m.secondaryCheckHost(node.Host, node.ClusterName) {
				log.Printf("Secondary Check: Host %s is reachable from a Slave. Network Partition suspected. No Failover triggered.", node.Host)
				return
			}
		}

		// 从 Manager 直接 Ping/SSH 目标主机。SSH 端口可达只能证明主机
		// 在线，其本身并不能说明数据库进程已死：在完全健康的 MySQL 上，
		// directDB 探测也可能因瞬时的握手 / 代理原因（如异常的问候字节）
		// 而失败。将此视为未经确认的怀疑，要求连续多次确认后再切换，
		// 从而使不稳定的网络探测不会在每个节拍都触发故障切换。
		hostAlive := m.bypassCheckPing(node.Host)
		attempt := m.bumpFailoverConfirmation(node.Host)
		if attempt < requiredFailoverConfirmations {
			if hostAlive {
				log.Printf("Bypass Check: Host %s reachable but DB probe failed; unconfirmed (attempt %d/%d), not switching.",
					node.Host, attempt, requiredFailoverConfirmations)
			} else {
				log.Printf("Bypass Check: Host %s unreachable; unconfirmed (attempt %d/%d), not switching.",
					node.Host, attempt, requiredFailoverConfirmations)
			}
			return
		}

		if hostAlive {
			log.Printf("Bypass Check: Host %s ALIVE but DB probe failed %d times. Confirming Failover.",
				node.Host, requiredFailoverConfirmations)
		} else {
			log.Printf("Bypass & Secondary Check: Host %s unreachable %d times. Confirming Failover as UnreachableMaster.",
				node.Host, requiredFailoverConfirmations)
			if code == logic.DeadMaster {
				code = logic.UnreachableMaster
			}
		}
		m.resetFailoverConfirmations(node.Host)
		m.HandleConfirmedFailureWithCode(code, node, nil)
		return
	}

	// 条件 3：Agent 可达、数据库健康，但磁盘空间已满。
	// 若 Agent 可达且数据库健康，则检查磁盘
	diskStatus, err := agentClient.GetDiskStatus(ctx, &proto.DiskStatusRequest{Path: "/"})
	if err == nil && diskStatus.IsFull {
		log.Printf("Agent reports DB is HEALTHY, but Disk is FULL (%.2f%%). Triggering Failover.", diskStatus.UsagePercent)

		// 重置冲突计数
		m.resetConflict(node.Host)

		m.HandleConfirmedFailureWithCode(logic.MasterDiskFull, node, nil)
		return
	}

	if healthy {
		// 检查半同步锁：主库已启用半同步，但没有可回 ack 的从库
		statusResp, statusErr := agentClient.GetStatus(ctx, &proto.GetStatusRequest{})
		if statusErr == nil {
			st := statusResp.GetStatus()
			semiEnabled := st["semi_sync_master"] == "true" || st["rpl_semi_sync_master_enabled"] == "ON"
			semiClients := st["semi_sync_clients"]
			if semiClients == "" {
				semiClients = st["rpl_semi_sync_master_clients"]
			}
			if semiEnabled && semiClients == "0" {
				log.Printf("Agent reports semi-sync enabled but no ack-capable slaves on %s. Triggering LockedSemiSyncMaster.", node.Host)
				m.resetConflict(node.Host)
				m.HandleConfirmedFailureWithCode(logic.LockedSemiSyncMaster, node, nil)
				return
			}
		}

		// 条件 4：一切正常。DC 状态只是稍有延迟或发生了抖动。
		log.Printf("Agent reports DB is HEALTHY and Disk is OK. Conflict with DC status. Taking no action.")
		m.recordConflict(node.Host)
	} else {
		// 条件 5：Agent 可达，但上报数据库不健康（数据库进程崩溃）。
		log.Printf("Agent reports DB is UNHEALTHY. Manager attempting direct short-connection to DB at %s:%d to verify...", node.Host, node.Port)
		if m.directDBCheck(node) {
			log.Printf("Direct DB check successful. Agent DB check is a false positive. No Failover triggered.")
			return
		}

		// 确认故障后重置冲突计数
		m.resetConflict(node.Host)

		log.Printf("Manager short-connection also failed. Confirming DB is UNHEALTHY Failover.")
		m.HandleConfirmedFailureWithCode(code, node, nil)
	}
}

// bypassCheckPing 模拟对主机执行 ping 或 TCP 检查
func (m *Monitor) bypassCheckPing(host string) bool {
	timeout := 1 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Manager.CheckTimeout != "" {
		if d, err := time.ParseDuration(cfg.Manager.CheckTimeout); err == nil {
			timeout = d
		}
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, "22"), timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// deriveAgentPort 返回 n 上记录的显式 agent 端口；若缺失，则依据
// 数据库类型的方言推导得出。
func deriveAgentPort(n *datacenter.Node) (int32, error) {
	if n.AgentPort > 0 {
		return n.AgentPort, nil
	}
	dialect, err := db.GetDialect(n.DbType)
	if err != nil {
		return 0, fmt.Errorf("derive agent port for %s: %w", n.Host, err)
	}
	agentPort := dialect.DeriveAgentPort(n.Port)
	log.Printf("AgentPort missing for %s, derived port %d via %s dialect", n.Host, agentPort, n.DbType)
	return agentPort, nil
}

// directDBCheck 通过要求方言按数据库线协议通信（不进行鉴权），
// 验证数据库进程是否有响应。
func (m *Monitor) directDBCheck(node *datacenter.Node) bool {
	timeout := 1 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Manager.CheckTimeout != "" {
		if d, err := time.ParseDuration(cfg.Manager.CheckTimeout); err == nil {
			timeout = d
		}
	}
	dialect, err := db.GetDialect(node.DbType)
	if err != nil {
		log.Printf("[directDBCheck] no dialect registered for %s: %v", node.DbType, err)
		return false
	}
	addr := net.JoinHostPort(node.Host, strconv.Itoa(int(node.Port)))
	if err := dialect.Probe(context.Background(), addr, timeout); err != nil {
		log.Printf("[directDBCheck] %v", err)
		return false
	}
	return true
}

// secondaryCheckHost 请求集群中一个健康的对等 Agent 在 SSH/主机
// 端口（22）上对目标主机进行 TCP 探测。
func (m *Monitor) secondaryCheckHost(targetHost, clusterName string) bool {
	r, _ := m.secondaryProbePort(targetHost, clusterName, 22)
	return r
}

// secondaryProbePort 请求某个对等 Agent 在 targetPort（主机为 22，
// 数据库则为 DB 端口）上对目标主机进行 TCP 探测。reachable 表示是否
// 有应答；hasPeer 表示是否确实咨询到了一个可用的对等 Agent（false 表示
// 没有对等节点，但这不能证明目标已宕机）。
func (m *Monitor) secondaryProbePort(targetHost, clusterName string, targetPort int32) (reachable bool, hasPeer bool) {
	clusterNodes, err := m.dcClient.GetClusterNodes(clusterName)
	if err != nil || len(clusterNodes) == 0 {
		return false, false
	}

	for _, n := range clusterNodes {
		// 跳过故障节点自身
		if n.Host == targetHost {
			continue
		}

		// 尝试连接该从库的 Agent
		agentPort, portErr := deriveAgentPort(n)
		if portErr != nil {
			continue // 不知道该从库的 agent 端口，无法访问
		}
		agentAddr := net.JoinHostPort(n.Host, strconv.Itoa(int(agentPort)))

		transport, tErr := tlsutil.ClientDialOption()
		if tErr != nil {
			continue // TLS 配置无效，尝试下一个从库
		}
		opts := []grpc.DialOption{transport}

		if cfg := config.Get(); cfg.AuthToken != "" {
			opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+cfg.AuthToken)
				return invoker(ctx, method, req, reply, cc, opts...)
			}))
		}

		conn, err := grpc.NewClient(agentAddr, opts...)
		if err != nil {
			continue // 尝试下一个从库
		}

		client := proto.NewDBAgentClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.SSHCheck(ctx, &proto.SSHCheckRequest{
			TargetHost: targetHost,
			TargetPort: targetPort,
		})
		cancel()
		conn.Close()

		if err == nil {
			// 已成功咨询到该对等节点。
			return resp.Reachable, true
		}
	}

	return false, false
}

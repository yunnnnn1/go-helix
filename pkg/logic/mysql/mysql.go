package mysql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/db"
	"github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type MySQLSwitcher struct{}

func init() {
	logic.Register("mysql", &MySQLSwitcher{})
}

func getAgentClient(node *datacenter.Node) (*grpc.ClientConn, proto.DBAgentClient, error) {
	agentPort := node.AgentPort
	if agentPort <= 0 {
		dialect, err := db.GetDialect(node.DbType)
		if err != nil {
			return nil, nil, fmt.Errorf("derive agent port for %s: %w", node.Host, err)
		}
		agentPort = dialect.DeriveAgentPort(node.Port)
	}
	return dialAgentClient(fmt.Sprintf("%s:%d", node.Host, agentPort))
}

// dialAgentClient 以标准的传输和鉴权拦截器连接到位于 host:port 的
// DBAgent。
func dialAgentClient(addr string) (*grpc.ClientConn, proto.DBAgentClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if cfg := config.Get(); cfg != nil && cfg.AuthToken != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+cfg.AuthToken)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC client to %s: %w", addr, err)
	}
	return conn, proto.NewDBAgentClient(conn), nil
}

func (s *MySQLSwitcher) Elect(ctx context.Context, failedNode *datacenter.Node, clusterNodes []*datacenter.Node, preferredNode *datacenter.Node) (*logic.ElectionResult, error) {
	fl := logging.FailoverLogger(ctx)
	fl.Printf("Starting MySQL Election for Cluster %s (Failed Node: %s)", failedNode.ClusterName, failedNode.Addr())

	result := &logic.ElectionResult{}
	var slaveNode *datacenter.Node

	if preferredNode != nil {
		fl.Printf("Using Preferred Candidate: %s:%d", preferredNode.Host, preferredNode.Port)
		slaveNode = preferredNode
	} else {
		hostCount := make(map[string]int)
		for _, n := range clusterNodes {
			hostCount[n.Host]++
		}

		var candidates []*logic.Candidate
		probeTimeout := config.ParseDuration(config.Get().Manager.CheckTimeout, 2*time.Second)
		for _, n := range clusterNodes {
			if n.Role != "slave" {
				continue
			}

			candidate := &logic.Candidate{
				Node:          n,
				InstanceCount: hostCount[n.Host],
			}

			conn, client, err := getAgentClient(n)
			if err != nil {
				fl.Printf("[Elect] Cannot connect to agent on %s:%d: %v, marking as not alive", n.Host, n.AgentPort, err)
				candidates = append(candidates, candidate)
				continue
			}

			probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)

			pingResp, err := client.Ping(probeCtx, &proto.PingRequest{})
			if err != nil {
				fl.Printf("[Elect] Ping failed for %s:%d: %v", n.Host, n.Port, err)
			} else {
				candidate.IsAlive = pingResp.Alive
			}

			statusResp, err := client.GetStatus(probeCtx, &proto.GetStatusRequest{})
			if err != nil {
				fl.Printf("[Elect] GetStatus failed for %s:%d: %v", n.Host, n.Port, err)
			} else {
				st := statusResp.GetStatus()
				candidate.IsSemiSync = st["semi_sync_master"] == "true" || st["semi_sync_master"] == "ON"

				binlogFile := st["Relay_Master_Log_File"]
				if binlogFile == "" {
					binlogFile = st["Master_Log_File"]
				}
				var binlogPos int64
				if v, perr := strconv.ParseInt(st["Exec_Master_Log_Pos"], 10, 64); perr == nil {
					binlogPos = v
				}
				candidate.Position = newMySQLPosition(st["gtid_executed"], binlogFile, binlogPos)
			}

			cancel()
			conn.Close()

			candidates = append(candidates, candidate)
		}

		strategy := &logic.DefaultElectionStrategy{}
		winner, decisionTree := strategy.SelectCandidate(candidates, failedNode)
		fl.Println(decisionTree)

		if winner == nil {
			return nil, fmt.Errorf("no available slave found for failover (Election failed)")
		}
		slaveNode = winner.Node
		fl.Printf("Selected Slave %s:%d as Candidate (Priority: %d, Region: %s).", slaveNode.Host, slaveNode.Port, slaveNode.Priority, slaveNode.Region)
	}

	result.NewMaster = slaveNode
	// 比较完整标识（host:port）而非仅主机，这样同机部署但端口
	// 不同的实例会被视为不同节点。
	for _, n := range clusterNodes {
		if n.Role == "slave" && n.Addr() != slaveNode.Addr() {
			result.Slaves = append(result.Slaves, n)
		}
	}

	return result, nil
}

// --- 共享步骤 ---

// adminStepTimeout 限制单次管理类 RPC 的耗时。外层 context 没有
// 超时限制（见 monitor/failover.go），因此若无此限制，单个卡住的调用
// 就可能挂起切换 goroutine，导致 Completed/End 标记永远无法
// 写入。
const adminStepTimeout = 10 * time.Second

// execStep 以单次调用超时执行一个操作，使卡住的调用绝不会
// 阻塞整个流程。业务错误会被返回，交由调用方
// 记录日志/路由处理。
func execStep(parent context.Context, client proto.DBAgentClient, operation string, params map[string]string) error {
	ctx, cancel := context.WithTimeout(parent, adminStepTimeout)
	defer cancel()
	_, err := client.ExecuteOperation(ctx, &proto.OperationRequest{
		Operation: operation,
		Params:    params,
	})
	return err
}

// promoteToMaster 将从库提升为主库：StopSlave → ResetSlaveAll → EnableSemiSync → SetWritable。
func promoteToMaster(ctx context.Context, client proto.DBAgentClient, addr string) error {
	fl := logging.FailoverLogger(ctx)

	fl.Printf("[Promote] %s step 1/4 stopping replica threads", addr)
	if err := execStep(ctx, client, "StopSlave", nil); err != nil {
		return fmt.Errorf("StopSlave on %s: %w", addr, err)
	}

	fl.Printf("[Promote] %s step 2/4 resetting replica configuration (ALL)", addr)
	if err := execStep(ctx, client, "ResetSlave", map[string]string{"all": "true"}); err != nil {
		return fmt.Errorf("ResetSlave on %s: %w", addr, err)
	}

	fl.Printf("[Promote] %s step 3/4 enabling semi-sync source (best-effort)", addr)
	if err := execStep(ctx, client, "EnableSemiSyncMaster", nil); err != nil {
		fl.Printf("[Promote] Warning: EnableSemiSync on %s: %v", addr, err)
	}

	fl.Printf("[Promote] %s step 4/4 clearing read_only / super_read_only", addr)
	if err := execStep(ctx, client, "SetReadOnly", map[string]string{
		"read_only":       "false",
		"super_read_only": "false",
	}); err != nil {
		return fmt.Errorf("SetReadOnly=false on %s: %w", addr, err)
	}

	fl.Printf("[Promote] %s promotion finished, node is now a writable master", addr)
	return nil
}

// repointOne 将单个节点的复制源重新指向主库：
// StopSlave -> ChangeMaster -> StartSlave。每个 RPC 都有时间限制。
func repointOne(ctx context.Context, target, master *datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	sc, scl, err := getAgentClient(target)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer sc.Close()

	fl.Printf("[Repoint] %s -> %s: StopSlave", target.Addr(), master.Addr())
	if err := execStep(ctx, scl, "StopSlave", nil); err != nil {
		return fmt.Errorf("StopSlave: %w", err)
	}

	fl.Printf("[Repoint] %s ChangeMaster -> %s", target.Addr(), master.Addr())
	if err := execStep(ctx, scl, "ChangeMaster", map[string]string{
		"host": master.Host,
		"port": fmt.Sprintf("%d", master.Port),
	}); err != nil {
		return fmt.Errorf("ChangeMaster: %w", err)
	}

	fl.Printf("[Repoint] %s StartSlave", target.Addr())
	if err := execStep(ctx, scl, "StartSlave", nil); err != nil {
		return fmt.Errorf("StartSlave: %w", err)
	}
	return nil
}

// waitReplicaCatchup 让节点等待，直到其已执行 GTID 集合追平
// 其数据源，使用配置的（更长的）追平超时时间。
func waitReplicaCatchup(ctx context.Context, target *datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	sc, scl, err := getAgentClient(target)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer sc.Close()

	timeout := config.ParseDuration(config.Get().Manager.CatchupTimeout, 30*time.Second)
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fl.Printf("[Repoint] %s waiting GTID catchup (timeout %s)", target.Addr(), timeout)
	if _, err := scl.ExecuteOperation(wctx, &proto.OperationRequest{Operation: "WaitCatchup"}); err != nil {
		return err
	}
	fl.Printf("[Repoint] %s caught up", target.Addr())
	return nil
}

// repointSlaves 将一组节点重新指向新主库。失败节点的错误会被
// 返回。每个 RPC 都有时间限制，因此本函数总能返回，调用方
// 可以据此输出其 Completed/End 标记。
func repointSlaves(ctx context.Context, newMaster *datacenter.Node, slaves []*datacenter.Node) []string {
	fl := logging.FailoverLogger(ctx)
	var errs []string
	for _, slave := range slaves {
		if err := repointOne(ctx, slave, newMaster); err != nil {
			fl.Printf("[Repoint] %s failed to re-point: %v", slave.Addr(), err)
			errs = append(errs, fmt.Sprintf("%s: %v", slave.Addr(), err))
			continue
		}
		fl.Printf("[Repoint] %s re-pointed to new master %s", slave.Addr(), newMaster.Addr())
	}
	return errs
}

// gtidExecutedFrom 通过 Agent 的 GetStatus RPC 获取节点已执行的 GTID 集合。
func gtidExecutedFrom(ctx context.Context, client proto.DBAgentClient) (string, error) {
	resp, err := client.GetStatus(ctx, &proto.GetStatusRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetStatus()["gtid_executed"], nil
}

// --- 紧急故障切换 ---

func (s *MySQLSwitcher) Failover(ctx context.Context, failedNode *datacenter.Node, election *logic.ElectionResult) error {
	fl := logging.FailoverLogger(ctx)
	newMaster := election.NewMaster
	fl.Printf("[Failover] Starting: %s → %s", failedNode.Addr(), newMaster.Addr())

	// 步骤 1：执行提升前围栏。required 模式下，旧主库会被冻结并
	// 回读 super_read_only 为 ON（或确认其 DB 已宕机）；否则中止
	// 提升，以防止出现两个可写主库。best_effort / disabled 保持
	// 原有行为。
	fl.Printf("[Failover] Step 1/4 fencing failed master %s", failedNode.Addr())
	if err := gateFencing(ctx, failedNode); err != nil {
		return err
	}

	slaveConn, slaveClient, err := getAgentClient(newMaster)
	if err != nil {
		return fmt.Errorf("connect to candidate %s: %w", newMaster.Addr(), err)
	}
	defer slaveConn.Close()
	fl.Printf("[Failover] Connected to candidate %s", newMaster.Addr())

	// ---- 数据收敛：三级兜底 --------------------------
	catchupTimeout := config.ParseDuration(config.Get().Manager.CatchupTimeout, 30*time.Second)

	// 目标位点 = 故障主库已执行的 GTID（尽力读取）。
	targetGTID := readGTIDBestEffort(ctx, failedNode)
	// 回放前候选节点的快照（其最后应用的数据源位点）。
	candidateGTID, candidateStatus := readCandidateStatus(ctx, slaveClient)
	fl.Printf("[Failover] Data convergence: targetGTID=%q candidateGTID=%q", targetGTID, candidateGTID)

	reached := targetGTID != "" && gtidSetContains(candidateGTID, targetGTID)

	// Level 1：原生复制等待。
	if !reached {
		fl.Printf("[Failover] Level 1/3 waiting candidate GTID catchup via replication (timeout %s)", catchupTimeout)
		waitCtx, waitCancel := context.WithTimeout(ctx, catchupTimeout)
		_, waitErr := slaveClient.ExecuteOperation(waitCtx, &proto.OperationRequest{Operation: "WaitCatchup"})
		waitCancel()
		if waitErr != nil {
			fl.Printf("[Failover] Level 1 catchup did not finish: %v — trying next level", waitErr)
		} else {
			fl.Printf("[Failover] Level 1 candidate caught up")
		}
		reached = candidateReached(ctx, slaveClient, targetGTID)
	}

	// Level 2：通过复制协议进行 GTID 逻辑回放。
	if !reached {
		fl.Printf("[Failover] Level 2/3 GTID logical replay plugin from %s", failedNode.Addr())
		catchupCtx, catchupCancel := context.WithTimeout(ctx, catchupTimeout)
		resp, pErr := slaveClient.ExecutePlugin(catchupCtx, &proto.PluginRequest{
			PluginName: "gtid_sync",
			Action:     "CatchupFrom",
			Params: map[string]string{
				"host": failedNode.Host,
				"port": fmt.Sprintf("%d", failedNode.Port),
			},
		})
		catchupCancel()
		if pErr != nil || !resp.GetSuccess() {
			fl.Printf("[Failover] Level 2 logical replay did not complete: err=%v resp=%v — trying rescue", pErr, resp)
		}
		reached = candidateReached(ctx, slaveClient, targetGTID)
	}

	// Level 3：通过 agent 进行 MHA 风格的 binlog 尾部抢救（即使故障的
	// mysqld 已死亡也有效；其 agent 会从本地磁盘读取 binlog 文件）。
	if !reached {
		fl.Printf("[Failover] Level 3/3 tail-binlog rescue from %s agent", failedNode.Addr())
		if rErr := rescueBinlogTail(ctx, failedNode, newMaster, candidateStatus, targetGTID, slaveClient); rErr != nil {
			fl.Printf("[Failover] Level 3 rescue failed: %v", rErr)
		}
		reached = candidateReached(ctx, slaveClient, targetGTID)
	}

	switch {
	case targetGTID != "" && reached:
		fl.Printf("[Failover] Data convergence verified: candidate contains target GTID")
	case targetGTID != "":
		// 失败即关闭：任何路径都无法补齐的已知缺口不得被
		// 提升。这取代了此前仅告警的行为。
		return fmt.Errorf("candidate %s could not reach failed master GTID %s; aborting failover to avoid promoting incomplete data",
			newMaster.Addr(), targetGTID)
	default:
		// 故障主库完全不可达（无 status/agent）：双方都无法进行
		// 抢救；仍继续提升，依赖半同步（semi-sync）保障。
		fl.Printf("[Failover] Warning: failed master fully unreachable, target GTID unknown; proceeding with promotion — loss window depends on semi-sync protection")
	}

	// 构建复制拓扑图，以便保留层级：只有故障主库的
	// 直属副本需要重新指向候选节点；级联子树保持其
	// 形态，并继续经由各自（已重新指向的）
	// 中间主保持同步。
	graphNodes := make([]*datacenter.Node, 0, len(election.Slaves)+2)
	graphNodes = append(graphNodes, failedNode, newMaster)
	graphNodes = append(graphNodes, election.Slaves...)
	graph := logic.BuildTopology(graphNodes)

	// 解析作用域限定为本集群的策略（集群级覆盖 >
	// 配置文件 > 内置默认值），绝不使用全局 topology 配置段。
	topo := config.EffectiveTopology(failedNode.ClusterName)
	preserve := topo.Preserve()
	detach := topo.Detach()
	fl.Printf("[Failover] effective topology for cluster %s: detach_lost_replicas=%v preserve_hierarchy=%v",
		failedNode.ClusterName, detach, preserve)

	var targets []*datacenter.Node
	if preserve {
		// 故障主库的直属子节点，候选节点本身除外。
		for _, c := range graph.DirectChildren(failedNode) {
			if c.Addr() != newMaster.Addr() {
				targets = append(targets, c)
			}
		}
		fl.Printf("[Failover] Step 3/4 preserve-hierarchy: converging %d direct replica anchor(s) before promotion (sub-trees kept)", len(targets))
	} else {
		targets = election.Slaves
		fl.Printf("[Failover] Step 3/4 flat mode: converging %d replica(s) before promotion", len(targets))
	}

	var detached, hardFail []string
	for _, slave := range targets {
		if err := repointOne(ctx, slave, newMaster); err != nil {
			fl.Printf("[Failover] %s failed to re-point: %v", slave.Addr(), err)
			detail := fmt.Sprintf("%s: re-point: %v", slave.Addr(), err)
			if detach {
				detached = append(detached, detail)
			} else {
				hardFail = append(hardFail, detail)
			}
			continue
		}
		fl.Printf("[Failover] %s re-pointed, waiting catchup", slave.Addr())
		if err := waitReplicaCatchup(ctx, slave); err != nil {
			fl.Printf("[Failover] %s did not catch up: %v", slave.Addr(), err)
			detail := fmt.Sprintf("%s: catchup: %v", slave.Addr(), err)
			if detach {
				detached = append(detached, detail)
			} else {
				hardFail = append(hardFail, detail)
			}
			continue
		}
		fl.Printf("[Failover] %s converged with candidate", slave.Addr())
	}

	if len(hardFail) > 0 {
		// 强制收敛（detach 关闭）：不提升任何节点，以免不完整的
		// 候选节点被宣告为主库。
		return fmt.Errorf("failover: %d replica(s) not converged before promotion: %v",
			len(hardFail), hardFail)
	}
	if len(detached) > 0 {
		// detach 已启用：仍继续提升，并将这些副本置于活动拓扑
		// 之外，留待后续人工恢复。
		fl.Printf("[Failover] Note: %d replica(s) detached (not part of active topology), recover them later: %v",
			len(detached), detached)
	} else {
		fl.Printf("[Failover] All %d target replica(s) re-pointed and caught up", len(targets))
	}
	if preserve {
		fl.Printf("[Failover] Hierarchy preserved; deeper replicas continue through their intermediate anchors")
	}

	// 入口钩子预检（VIP/域名）：同步最新钩子并在提升前核验当前
	// 入口状态。Required（P0）失败将中止。
	if err := runEndpointHooks(ctx, "pre", failedNode, newMaster); err != nil {
		return fmt.Errorf("failover endpoint precheck: %w", err)
	}

	// 步骤 4/4：提升活动拓扑已与之匹配的候选节点。
	fl.Printf("[Failover] Step 4/4 promoting candidate %s to master", newMaster.Addr())
	if err := promoteToMaster(ctx, slaveClient, newMaster.Addr()); err != nil {
		return err
	}

	// 将业务入口（VIP/域名）切换到新主库并校验。
	if err := runEndpointHooks(ctx, "post", failedNode, newMaster); err != nil {
		return fmt.Errorf("failover endpoint switch: %w (new master %s is writable; manual endpoint recovery needed)",
			err, newMaster.Addr())
	}

	fl.Printf("[Failover] Completed. New Master: %s (detached=%d)", newMaster.Addr(), len(detached))
	return nil
}

// --- 平滑（计划内）主动切换 ---

func (s *MySQLSwitcher) GracefulMasterTakeover(ctx context.Context, oldMaster *datacenter.Node, election *logic.ElectionResult) error {
	fl := logging.FailoverLogger(ctx)
	newMaster := election.NewMaster
	fl.Printf("[GracefulTakeover] Starting: %s → %s", oldMaster.Addr(), newMaster.Addr())

	masterConn, masterClient, err := getAgentClient(oldMaster)
	if err != nil {
		return fmt.Errorf("old master %s must be reachable: %w", oldMaster.Addr(), err)
	}
	defer masterConn.Close()

	slaveConn, slaveClient, err := getAgentClient(newMaster)
	if err != nil {
		return fmt.Errorf("candidate %s must be reachable: %w", newMaster.Addr(), err)
	}
	defer slaveConn.Close()

	// 步骤 1：冻结旧主库（只读，不断开连接）
	fl.Printf("[GracefulTakeover] Step 1/4 freezing old master %s (read_only + super_read_only)", oldMaster.Addr())
	if err := execStep(ctx, masterClient, "SetReadOnly", map[string]string{
		"read_only":       "true",
		"super_read_only": "true",
	}); err != nil {
		return fmt.Errorf("set old master read-only: %w", err)
	}
	fl.Printf("[GracefulTakeover] Old master frozen; verifying super_read_only read-back")
	verifyCtx, verifyCancel := context.WithTimeout(ctx, config.ParseDuration(config.Get().Manager.CheckTimeout, 2*time.Second))
	st, vErr := masterClient.GetStatus(verifyCtx, &proto.GetStatusRequest{})
	verifyCancel()
	if vErr != nil || !strings.EqualFold(st.GetStatus()["super_read_only"], "ON") {
		got := ""
		if st != nil {
			got = st.GetStatus()["super_read_only"]
		}
		return fmt.Errorf("old master fence not confirmed (super_read_only read-back=%q): %w", got, vErr)
	}
	fl.Printf("[GracefulTakeover] Old master fence confirmed (super_read_only=ON); new writes are blocked")

	// 步骤 2：等待追平（失败时回滚）
	fl.Printf("[GracefulTakeover] Step 2/4 waiting for candidate %s GTID catchup", newMaster.Addr())
	catchupTimeout := config.ParseDuration(config.Get().Manager.CatchupTimeout, 30*time.Second)
	waitCtx, waitCancel := context.WithTimeout(ctx, catchupTimeout)
	_, err = slaveClient.ExecuteOperation(waitCtx, &proto.OperationRequest{Operation: "WaitCatchup"})
	waitCancel()
	if err != nil {
		fl.Printf("[GracefulTakeover] Candidate catchup failed: %v; rolling back old master to writable", err)
		if rbErr := execStep(ctx, masterClient, "SetReadOnly", map[string]string{
			"read_only":       "false",
			"super_read_only": "false",
		}); rbErr != nil {
			fl.Printf("[GracefulTakeover] Warning: rollback SetReadOnly on old master failed: %v", rbErr)
		} else {
			fl.Printf("[GracefulTakeover] Old master rolled back to writable")
		}
		return fmt.Errorf("WaitCatchup failed, rolled back old master: %w", err)
	}
	fl.Printf("[GracefulTakeover] Candidate caught up; data is consistent")

	// 提升前的入口预检（钩子同步在此进行）。
	if err := runEndpointHooks(ctx, "pre", oldMaster, newMaster); err != nil {
		// 返回前先将旧主库回滚为可写。
		_ = execStep(ctx, masterClient, "SetReadOnly", map[string]string{
			"read_only": "false", "super_read_only": "false",
		})
		return fmt.Errorf("switchover endpoint precheck: %w", err)
	}

	// 步骤 3：提升（共用步骤）
	fl.Printf("[GracefulTakeover] Step 3/4 promoting candidate %s", newMaster.Addr())
	if err := promoteToMaster(ctx, slaveClient, newMaster.Addr()); err != nil {
		return err
	}

	// 步骤 4：将旧主库降级 + 重新指向从库，并等待每个节点
	// 都追平，使切换返回时集群已完全收敛
	// （强保证，与紧急故障切换不同）。
	allSlaves := append([]*datacenter.Node{oldMaster}, election.Slaves...)
	fl.Printf("[GracefulTakeover] Step 4/4 demoting old master, re-pointing and converging %d node(s)", len(allSlaves))

	var convergeFail []string
	for _, slave := range allSlaves {
		if err := repointOne(ctx, slave, newMaster); err != nil {
			fl.Printf("[GracefulTakeover] %s failed to re-point: %v", slave.Addr(), err)
			convergeFail = append(convergeFail, fmt.Sprintf("%s: re-point: %v", slave.Addr(), err))
			continue
		}
		fl.Printf("[GracefulTakeover] %s re-pointed, waiting catchup", slave.Addr())
		if err := waitReplicaCatchup(ctx, slave); err != nil {
			fl.Printf("[GracefulTakeover] %s did not catch up: %v", slave.Addr(), err)
			convergeFail = append(convergeFail, fmt.Sprintf("%s: catchup: %v", slave.Addr(), err))
			continue
		}
		fl.Printf("[GracefulTakeover] %s converged with new master", slave.Addr())
	}
	if len(convergeFail) > 0 {
		return fmt.Errorf("graceful switchover: %d node(s) not fully converged: %v (new master %s holds the correct data)",
			len(convergeFail), convergeFail, newMaster.Addr())
	}
	fl.Printf("[GracefulTakeover] All %d node(s) re-pointed and caught up", len(allSlaves))

	// 在整个集群收敛后再切换业务入口。
	if err := runEndpointHooks(ctx, "post", oldMaster, newMaster); err != nil {
		return fmt.Errorf("switchover endpoint switch: %w", err)
	}

	fl.Printf("[GracefulTakeover] Completed. New Master: %s", newMaster.Addr())
	return nil
}

// --- 主从恢复 ---

// RecoverReplica 将一个副本重新指向当前主库，并等待其
// 追平（修复 IO/SQL 中断或失效的数据源配置）。
func (s *MySQLSwitcher) RecoverReplica(ctx context.Context, master, target *datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	if master == nil {
		return fmt.Errorf("no master known; cannot recover replica %s (try topology rebuild)", target.Addr())
	}
	if target.Addr() == master.Addr() {
		return fmt.Errorf("target %s is the current master, not a replica", target.Addr())
	}

	fl.Printf("[Recover] Repairing replica %s -> master %s", target.Addr(), master.Addr())
	if err := repointOne(ctx, target, master); err != nil {
		return fmt.Errorf("re-point %s: %w", target.Addr(), err)
	}
	if err := waitReplicaCatchup(ctx, target); err != nil {
		return fmt.Errorf("catchup %s: %w", target.Addr(), err)
	}
	fl.Printf("[Recover] Replica %s recovered and caught up with %s", target.Addr(), master.Addr())
	return nil
}

// RebuildTopology 使集群收敛到单一主库（desiredMaster）；
// 其他所有节点都重新指向它并等待追平。
func (s *MySQLSwitcher) RebuildTopology(ctx context.Context, desiredMaster *datacenter.Node, allNodes []*datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	fl.Printf("[Recover] Rebuilding topology: desired master %s among %d node(s)", desiredMaster.Addr(), len(allNodes))

	// 1. 使 desiredMaster 收敛为一个干净、可写的主库。
	sc, scl, err := getAgentClient(desiredMaster)
	if err != nil {
		return fmt.Errorf("connect desired master %s: %w", desiredMaster.Addr(), err)
	}
	if desiredMaster.Role == "slave" {
		fl.Printf("[Recover] Desired master %s is currently a slave; promoting it", desiredMaster.Addr())
		if err := promoteToMaster(ctx, scl, desiredMaster.Addr()); err != nil {
			sc.Close()
			return err
		}
	} else {
		// 按角色已是主库：尽力清除所有失效的数据源配置，
		// 然后确保其可写。当未配置复制时，Stop/Reset 可能
		// 报错，这属于预期情况，可忽略。
		fl.Printf("[Recover] %s already master; best-effort clearing stale source config, then read_only", desiredMaster.Addr())
		_ = execStep(ctx, scl, "StopSlave", nil)
		_ = execStep(ctx, scl, "ResetSlave", map[string]string{"all": "true"})
		if err := execStep(ctx, scl, "SetReadOnly", map[string]string{
			"read_only":       "false",
			"super_read_only": "false",
		}); err != nil {
			sc.Close()
			return fmt.Errorf("make %s writable: %w", desiredMaster.Addr(), err)
		}
	}
	sc.Close()

	// 2. 将其他所有节点重新指向 desiredMaster。
	others := make([]*datacenter.Node, 0, len(allNodes))
	for _, n := range allNodes {
		if n.Addr() != desiredMaster.Addr() {
			others = append(others, n)
		}
	}
	fl.Printf("[Recover] Re-pointing %d other node(s) to %s", len(others), desiredMaster.Addr())
	if errs := repointSlaves(ctx, desiredMaster, others); len(errs) > 0 {
		fl.Printf("[Recover] %d node(s) failed to re-point: %v", len(errs), errs)
	}

	// 3. 等待其他所有节点追平。
	var catchFail []string
	for _, n := range others {
		if err := waitReplicaCatchup(ctx, n); err != nil {
			fl.Printf("[Recover] Warning: catchup on %s failed: %v", n.Addr(), err)
			catchFail = append(catchFail, fmt.Sprintf("%s: %v", n.Addr(), err))
		}
	}
	if len(catchFail) > 0 {
		return fmt.Errorf("rebuild topology: %d node(s) not caught up: %v", len(catchFail), catchFail)
	}

	fl.Printf("[Recover] Topology rebuilt: master %s with %d replica(s)", desiredMaster.Addr(), len(others))
	return nil
}

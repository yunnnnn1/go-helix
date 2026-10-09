package mysql

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/proto"
)

// RecoverIntermediateMaster 在不影响可写主库的前提下修复已死亡的
// 中间主。提升一个直属下游副本，使其承接中间主位点：
//
//  1. 选取数据最新的直属下游副本（胜出者）；
//  2. 将胜出者重新指向死亡中间主的上游并等待追平；
//  3. 将同级的其他直属副本重新指向胜出者；更深层的子树保持不变。
func (s *MySQLSwitcher) RecoverIntermediateMaster(ctx context.Context, failedIM *datacenter.Node, allNodes []*datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	graph := logic.BuildTopology(allNodes)

	upstream := graph.Parent(failedIM)
	if upstream == nil {
		return fmt.Errorf("intermediate master %s has no resolvable upstream; cannot re-home replicas", failedIM.Addr())
	}
	children := graph.DirectChildren(failedIM)
	if len(children) == 0 {
		return fmt.Errorf("intermediate master %s has no downstream replicas; nothing to recover", failedIM.Addr())
	}

	fl.Printf("[IntermediateRecover] deadIM=%s upstream=%s directReplicas=%d",
		failedIM.Addr(), upstream.Addr(), len(children))

	// 1. 在直属副本中选择替代者。
	winner, err := selectReplicaReplacement(ctx, failedIM, children)
	if err != nil {
		return err
	}
	fl.Printf("[IntermediateRecover] replacement = %s", winner.Addr())

	// 摘除（detach）策略的作用域限定为本集群，而非全局配置。
	detach := config.EffectiveTopology(failedIM.ClusterName).Detach()

	// 2. 将胜出者重新指向死亡中间主的上游；它此时占据中间主
	// 位点（仍是副本，只读）。
	if err := repointOne(ctx, winner, upstream); err != nil {
		return fmt.Errorf("repoint winner %s to upstream %s: %w", winner.Addr(), upstream.Addr(), err)
	}
	if err := waitReplicaCatchup(ctx, winner); err != nil {
		return fmt.Errorf("winner %s catchup upstream %s: %w", winner.Addr(), upstream.Addr(), err)
	}
	fl.Printf("[IntermediateRecover] %s now replicates from upstream %s and holds the intermediate position",
		winner.Addr(), upstream.Addr())

	// 3. 将同级的其他直属副本重新指向胜出者。
	var detached, hardFail []string
	for _, c := range children {
		if c.Addr() == winner.Addr() {
			continue
		}
		if err := repointOne(ctx, c, winner); err != nil {
			detail := fmt.Sprintf("%s: repoint: %v", c.Addr(), err)
			if detach {
				detached = append(detached, detail)
			} else {
				hardFail = append(hardFail, detail)
			}
			continue
		}
		if err := waitReplicaCatchup(ctx, c); err != nil {
			detail := fmt.Sprintf("%s: catchup: %v", c.Addr(), err)
			if detach {
				detached = append(detached, detail)
			} else {
				hardFail = append(hardFail, detail)
			}
			continue
		}
		fl.Printf("[IntermediateRecover] %s re-pointed to new intermediate %s and caught up", c.Addr(), winner.Addr())
	}
	if len(hardFail) > 0 {
		return fmt.Errorf("intermediate recovery: %d sibling replica(s) not converged: %v", len(hardFail), hardFail)
	}
	if len(detached) > 0 {
		fl.Printf("[IntermediateRecover] Note: %d sibling replica(s) detached for later manual recover: %v", len(detached), detached)
	}

	fl.Printf("[IntermediateRecover] Completed. Intermediate %s replaced by %s (detached=%d)",
		failedIM.Addr(), winner.Addr(), len(detached))
	return nil
}

// selectReplicaReplacement 选取最合适的直属副本以替代死亡的中间主，
// 使用与主库选举相同的候选模型/决策树，但作用域限定为给定的
// 直属副本。
func selectReplicaReplacement(ctx context.Context, failedIM *datacenter.Node, replicas []*datacenter.Node) (*datacenter.Node, error) {
	probeTimeout := config.ParseDuration(config.Get().Manager.CheckTimeout, 2*time.Second)

	var candidates []*logic.Candidate
	for _, n := range replicas {
		candidate := &logic.Candidate{Node: n}

		conn, client, err := getAgentClient(n)
		if err != nil {
			candidates = append(candidates, candidate)
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		if pingResp, pErr := client.Ping(probeCtx, &proto.PingRequest{}); pErr == nil {
			candidate.IsAlive = pingResp.Alive
		}
		if stResp, sErr := client.GetStatus(probeCtx, &proto.GetStatusRequest{}); sErr == nil {
			st := stResp.GetStatus()
			file := st["Relay_Master_Log_File"]
			if file == "" {
				file = st["Master_Log_File"]
			}
			var pos int64
			if v, pErr := strconv.ParseInt(st["Exec_Master_Log_Pos"], 10, 64); pErr == nil {
				pos = v
			}
			candidate.Position = newMySQLPosition(st["gtid_executed"], file, pos)
		}
		cancel()
		conn.Close()
		candidates = append(candidates, candidate)
	}

	winner, decisionTree := (&logic.DefaultElectionStrategy{}).SelectCandidate(candidates, failedIM)
	logging.FailoverLogger(ctx).Println(decisionTree)
	if winner == nil {
		return nil, fmt.Errorf("no suitable downstream replica found to replace intermediate master %s", failedIM.Addr())
	}
	return winner.Node, nil
}

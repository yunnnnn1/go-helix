// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	"github.com/yunnnnn1/go-helix/pkg/proto"
)

// FenceOutcome 归类对旧主库提升前的核验结果。
type FenceOutcome int

const (
	// FenceConfirmed：旧主库无法接受写入。
	FenceConfirmed FenceOutcome = iota
	// FenceUncertain：没有经过交叉核验的确凿证据表明旧主库已被围栏
	// （或有证据表明其仍可访问/可写）。
	FenceUncertain
)

// ensureOldMasterFenced 运行证据链，并返回明确的“无法写入”保证：
//
//	L1：旧主库自身的 Agent（Ping 到 DB 已宕机，或冻结并回读为 ON）
//	L2+L3：带外佐证。Manager 直接探测旧主库
//	       （DB 线协议 + 主机/SSH），并让健康的对等
//	       Agent 探测它。“确认死亡”要求直连视角与对等
//	       视角一致认为 DB 不可达（这样单边网络分区无法伪造
//	       “DB 已死”）。若某个视角明确显示 DB 可达，
//	       则说明旧主库仍然存活：不确定。
//
// 任何表明旧主库存活/可写的确凿迹象都会以不确定终止证据链
// （required 模式的调用方必须中止）。所有步骤都有时间限制。
func ensureOldMasterFenced(ctx context.Context, oldMaster *datacenter.Node) (FenceOutcome, string) {
	if outcome, detail := fenceViaLocalAgent(ctx, oldMaster); outcome != FenceUncertain {
		return outcome, detail

	}
	return fenceOutOfBand(ctx, oldMaster)
}

// fenceViaLocalAgent 为 L1：旧主库自身的 Agent 可达。
func fenceViaLocalAgent(ctx context.Context, oldMaster *datacenter.Node) (FenceOutcome, string) {
	fl := logging.FailoverLogger(ctx)
	fc := config.EffectiveFencing()
	timeout := config.ParseDuration(fc.Timeout, 3*time.Second)

	conn, client, err := getAgentClient(oldMaster)
	if err != nil {
		fl.Printf("[Fence:L1] old master agent unreachable (%v) — trying L2", err)
		return FenceUncertain, fmt.Sprintf("old master agent unreachable: %v", err)
	}
	defer conn.Close()

	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 1. 确认 DB 进程已宕机 -> 无法写入。
	if pr, pErr := client.Ping(fctx, &proto.PingRequest{}); pErr == nil && pr != nil && !pr.Alive {
		fl.Printf("[Fence:L1] %s DB confirmed not alive", oldMaster.Addr())
		return FenceConfirmed, "old master DB confirmed down"
	} else if pErr != nil {
		fl.Printf("[Fence:L1] %s ping error: %v — attempting freeze", oldMaster.Addr(), pErr)
	}

	// 2. 冻结：super_read_only 会隐式设置 read_only。
	if _, e := client.ExecuteOperation(fctx, &proto.OperationRequest{
		Operation: "SetReadOnly",
		Params:    map[string]string{"read_only": "true", "super_read_only": "true"},
	}); e != nil {
		return FenceUncertain, fmt.Sprintf("freeze old master: %v", e)
	}
	if _, kErr := client.ExecuteOperation(fctx, &proto.OperationRequest{Operation: "KillConnections"}); kErr != nil {
		fl.Printf("[Fence:L1] KillConnections warning: %v", kErr)
	}

	// 3. 回读权威的 ON 状态。
	st, sErr := client.GetStatus(fctx, &proto.GetStatusRequest{})
	if sErr != nil {
		return FenceUncertain, fmt.Sprintf("read back old master status: %v", sErr)
	}
	if strings.EqualFold(st.GetStatus()["super_read_only"], "ON") {
		fl.Printf("[Fence:L1] %s confirmed super_read_only=ON", oldMaster.Addr())
		return FenceConfirmed, "old master frozen (super_read_only=ON)"
	}
	return FenceUncertain, fmt.Sprintf("old master super_read_only read-back not ON: %q",
		st.GetStatus()["super_read_only"])
}

// probeView 汇总针对旧主库的一个观察视角。
type probeView struct {
	available bool // 探测能够执行
	dbUp      bool // DB 端口有应答
	hostUp    bool // 主机/SSH 端口有应答（仅直连视角）
}

// directProbe 是 Manager 自身的观察视角（L2）。
func directProbe(oldMaster *datacenter.Node) probeView {
	p := logic.GetFenceProbers()
	if p == nil {
		return probeView{}
	}
	return probeView{
		available: true,
		dbUp:      p.DirectDBReachable(oldMaster),
		hostUp:    p.HostReachable(oldMaster),
	}
}

// peerProbe 是健康的对等 Agent 的观察视角（L3）。若缺少可用的
// 对等节点，则产生不可用视角（通过 hasPeer 区分“无对等节点”与
// “对等节点存在但 DB 关闭”）。
func peerProbe(oldMaster *datacenter.Node) probeView {
	p := logic.GetFenceProbers()
	if p == nil {
		return probeView{}
	}
	reachable, hasPeer := p.PeerAgentProbe(oldMaster, oldMaster.Port)
	return probeView{available: hasPeer, dbUp: reachable}
}

// fenceOutOfBand 综合直连视角与对等视角（L2 + L3）。
//
//   - 任一视角明确看到 DB 可达 -> 旧主库仍然存活且未被围栏：
//     不确定（required 必须中止）。
//   - 两个视角均可用且一致认为 DB 不可达 -> 确认
//     死亡（经过交叉核验，单边分区无法骗过我们）。
//   - 仅一个视角可用并报告 DB 宕机，或视角
//     缺失/冲突 -> 不确定（无确凿、经佐证的证据）。
func fenceOutOfBand(ctx context.Context, oldMaster *datacenter.Node) (FenceOutcome, string) {
	fl := logging.FailoverLogger(ctx)

	d := directProbe(oldMaster)
	q := peerProbe(oldMaster)

	// 任一可用视角给出的存活证据优先。
	if d.available && d.dbUp {
		return FenceUncertain, "direct probe: old master DB reachable (alive, not fenced)"
	}
	if q.available && q.dbUp {
		return FenceUncertain, "peer probe: old master DB reachable (alive, not fenced)"
	}

	// 需要佐证：两个视角均可用，且都看到 DB 宕机。
	if d.available && q.available {
		fl.Printf("[Fence:L2/L3] %s direct and peer agree DB unreachable", oldMaster.Addr())
		detail := "old master DB unreachable from manager and peer"
		// 直连视角若同时看到主机宕机，会进一步强化“完全死亡”的
		// 判断；主机可达但 DB 宕机的情况，仅在有对等节点
		// 佐证时才可接受。
		if d.hostUp {
			detail += " (host reachable, DB down on both views)"
		}
		return FenceConfirmed, detail
	}

	// 仅有一个视角（或均无）：交叉核验证据不足。
	if d.available || q.available {
		return FenceUncertain, "only one out-of-band view available; DB-down not corroborated"
	}
	return FenceUncertain, "no out-of-band probers available"
}

// gateFencing 执行配置的提升前策略。仅当旧主库被确认已围栏时
// 才返回 nil；required 模式下，不确定的围栏会成为硬错误
// （不得继续提升），而在 best_effort 模式下
// 则降级为警告。
func gateFencing(ctx context.Context, oldMaster *datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	fc := config.EffectiveFencing()

	outcome, detail := ensureOldMasterFenced(ctx, oldMaster)
	if outcome == FenceConfirmed {
		fl.Printf("[Fence] passed: %s", detail)
		return nil
	}

	if fc.Enabled != nil && !*fc.Enabled {
		fl.Printf("[Fence] gate disabled; uncertain fence ignored: %s", detail)
		return nil
	}
	if fc.Mode == config.FencingModeBestEffort {
		fl.Printf("[Fence] best_effort: uncertain fence (%s) — proceeding with promotion", detail)
		return nil
	}
	// required：失败即关闭（fail closed），以避免脑裂双主。
	return fmt.Errorf("fencing required but old master could not be confirmed fenced: %s — aborting promotion to prevent split-brain", detail)
}

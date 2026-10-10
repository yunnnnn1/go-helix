// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/proto"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// readGTIDBestEffort 在故障节点的 agent 有应答时返回其 gtid_executed。
// 空字符串表示完全无法连接节点/agent，因而没有可用的
// 目标位点。
func readGTIDBestEffort(ctx context.Context, node *datacenter.Node) string {
	conn, client, err := getAgentClient(node)
	if err != nil {
		return ""
	}
	defer conn.Close()

	timeout := config.ParseDuration(config.Get().Manager.CheckTimeout, 2*time.Second)
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	gtid, err := gtidExecutedFrom(tctx, client)
	if err != nil {
		return ""
	}
	return gtid
}

// readCandidateStatus 一次性读取候选节点的完整状态，返回其
// gtid_executed 和状态 map（用于确定 binlog 起始坐标）。
func readCandidateStatus(ctx context.Context, client proto.DBAgentClient) (string, map[string]string) {
	tctx, cancel := context.WithTimeout(ctx, adminStepTimeout)
	defer cancel()

	resp, err := client.GetStatus(tctx, &proto.GetStatusRequest{})
	if err != nil {
		return "", nil
	}
	st := resp.GetStatus()
	return st["gtid_executed"], st
}

// candidateReached 报告候选节点的 gtid_executed 现在是否已包含
// 目标 GTID 集合。
func candidateReached(ctx context.Context, client proto.DBAgentClient, target string) bool {
	tctx, cancel := context.WithTimeout(ctx, adminStepTimeout)
	defer cancel()

	current, err := gtidExecutedFrom(tctx, client)
	if err != nil {
		return false
	}
	return gtidSetContains(current, target)
}

// gtidSetContains 报告 set 是否包含 other 的每个区间。
func gtidSetContains(set, other string) bool {
	if set == "" || other == "" {
		return false
	}
	a, errA := gomysql.ParseGTIDSet("mysql", set)
	b, errB := gomysql.ParseGTIDSet("mysql", other)
	if errA != nil || errB != nil {
		return false
	}
	return a.Contain(b)
}

// rescueBinlogTail 执行 Level-3 的 MHA 风格恢复：它在故障节点的
// agent 上打开尾部流，将每个原始 binlog 数据块转发到候选
// agent 的 ApplyBinlogRescue 流，然后等待物理回放及其
// GTID 校验。
func rescueBinlogTail(
	ctx context.Context,
	failedNode *datacenter.Node,
	candidateNode *datacenter.Node,
	candidateStatus map[string]string,
	targetGTID string,
	candidateClient proto.DBAgentClient,
) error {
	// 节点级救援信息以节点表 meta_data 为唯一权威来源（路径由心跳上报，
	// rescue_enabled/access_roots/max_bytes 由运维配置）。
	var dcCfg config.DatabaseConfig
	if cfg := config.Get(); cfg != nil {
		dcCfg = cfg.Datacenter
	}
	meta := datacenter.NewClient(&dcCfg)

	failedRescue, fErr := meta.GetNodeBinlogRescue(failedNode.Host, int(failedNode.Port))
	if fErr != nil {
		return fmt.Errorf("读取故障节点 %s 的 binlog 救援配置失败：%w", failedNode.Addr(), fErr)
	}
	candidateRescue, cErr := meta.GetNodeBinlogRescue(candidateNode.Host, int(candidateNode.Port))
	if cErr != nil {
		return fmt.Errorf("读取候选节点 %s 的 binlog 救援配置失败：%w", candidateNode.Addr(), cErr)
	}
	if !failedRescue.Enabled {
		return fmt.Errorf("故障节点 %s 未启用 binlog 救援（meta_data.rescue_enabled）", failedNode.Addr())
	}
	if !candidateRescue.Enabled {
		return fmt.Errorf("候选节点 %s 未启用 binlog 救援（meta_data.rescue_enabled）", candidateNode.Addr())
	}

	return runRescueBinlogTail(ctx, failedNode, failedRescue, candidateRescue, candidateStatus, targetGTID, candidateClient)
}

// runRescueBinlogTail 在已读取节点级配置的前提下执行实际的尾部流转发、
// 物理回放与 GTID 校验。它不依赖元数据读取，便于单元测试。
func runRescueBinlogTail(
	ctx context.Context,
	failedNode *datacenter.Node,
	failedRescue, candidateRescue datacenter.NodeBinlogRescue,
	candidateStatus map[string]string,
	targetGTID string,
	candidateClient proto.DBAgentClient,
) error {
	masterConn, masterClient, err := getAgentClient(failedNode)
	if err != nil {
		return fmt.Errorf("connect failed node agent: %w", err)
	}
	defer masterConn.Close()

	startFile := ""
	var startPos int64
	if candidateStatus != nil {
		startFile = statusFirst(candidateStatus, "Relay_Master_Log_File", "Master_Log_File")
		if v, pErr := strconv.ParseInt(statusFirst(candidateStatus, "Exec_Master_Log_Pos"), 10, 64); pErr == nil {
			startPos = v
		}
	}

	tail, err := masterClient.StreamBinlogTail(ctx, &proto.BinlogTailRequest{
		StartFile: startFile,
		StartPos:  startPos,
		StartGtid: candidateStatus["gtid_executed"],

		// 故障节点的节点级救援配置（元数据）随请求下发。
		Enabled:        true,
		BinlogDir:      failedRescue.BinlogDir,
		BinlogBasename: failedRescue.BinlogBaseName,
		AccessRoots:    failedRescue.AccessRoots,
		MaxBytes:       failedRescue.MaxBytes,
	})
	if err != nil {
		return fmt.Errorf("open binlog tail stream: %w", err)
	}

	rescue, err := candidateClient.ApplyBinlogRescue(ctx)
	if err != nil {
		return fmt.Errorf("open rescue apply stream: %w", err)
	}

	// 先发送候选节点的授权头（节点级配置），agent 收到后才允许写片段。
	if err := rescue.Send(&proto.BinlogRescueChunk{
		Enabled:  true,
		MaxBytes: candidateRescue.MaxBytes,
	}); err != nil {
		return fmt.Errorf("send rescue config header: %w", err)
	}

	forwarded := false
	for {
		chunk, rErr := tail.Recv()
		if rErr != nil {
			if rErr == io.EOF {
				break
			}
			return fmt.Errorf("read binlog tail: %w", rErr)
		}
		if chunk.Done || len(chunk.Data) == 0 {
			continue
		}
		if err := rescue.Send(&proto.BinlogRescueChunk{Data: chunk.Data}); err != nil {
			return fmt.Errorf("send rescue chunk: %w", err)
		}
		forwarded = true
	}

	// 收尾：候选节点应用该片段并校验是否到达目标位点。
	sessionID := fmt.Sprintf("failover-%d", time.Now().UnixNano())
	if err := rescue.Send(&proto.BinlogRescueChunk{
		Done:       true,
		TargetGtid: targetGTID,
		SessionId:  sessionID,
	}); err != nil {
		return fmt.Errorf("send rescue final marker: %w", err)
	}

	res, err := rescue.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("rescue apply: %w", err)
	}
	if !res.GetSuccess() {
		return fmt.Errorf("rescue apply unsuccessful: %s", res.GetMessage())
	}
	if !forwarded {
		return fmt.Errorf("failed node agent produced no rescue bytes (binlog files missing or purged)")
	}
	return nil
}

// statusFirst 返回状态 map 中各个 key 里第一个非空值。
func statusFirst(st map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := st[k]; v != "" {
			return v
		}
	}
	return ""
}

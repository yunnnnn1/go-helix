// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package datacenter

import "fmt"

// ReplicaStatus 承载由 Agent 心跳上报的、与数据库无关的
// 运行时/复制状态。通用复制概念均有明确类型；任何特定于
// 数据库类型的内容（MySQL 的 GTID/binlog、PostgreSQL 的
// LSN/slot）原样放入 Extra，由
// 持久化层映射到存储。
type ReplicaStatus struct {
	// 同机部署的 Agent gRPC 端口。
	AgentPort int32

	// 复制拓扑边（本节点从哪个节点复制数据）。
	UpstreamHost string
	UpstreamPort int32

	// 通用复制状态（适用于任意数据库类型）：
	// Receiver 指接收变更流的线程/进程
	// （MySQL I/O 线程、PostgreSQL WAL receiver）；
	// Applier 指应用/回放变更的线程/进程
	// （MySQL SQL 线程、PostgreSQL startup/recovery）。
	ReceiverRunning bool
	ApplierRunning  bool
	SyncReplication bool // 同步 / 半同步保护
	LagSeconds      int64

	// 按类型上报的键（MySQL：gtid_executed/binlog_file/...；
	// PostgreSQL：lsn/replication_slot/...）。
	Extra map[string]string
}

func (s *ReplicaStatus) extra(key string) string {
	if s == nil || s.Extra == nil {
		return ""
	}
	return s.Extra[key]
}

// gtidExecuted / binlog 访问器为旧的物理列提供支撑。它们
// 仅从 Extra 读取（由 MySQL 后端填充）。
func (s *ReplicaStatus) gtidExecuted() string { return s.extra("gtid_executed") }
func (s *ReplicaStatus) binlogFile() string   { return s.extra("binlog_file") }

func (s *ReplicaStatus) binlogPos() int64 {
	var v int64
	fmt.Sscan(s.extra("binlog_pos"), &v)
	return v
}

// usingGTID 复刻此前的推导规则（上报了 GTID 的节点即
// 基于 GTID）。
func (s *ReplicaStatus) usingGTID() bool { return s.gtidExecuted() != "" }

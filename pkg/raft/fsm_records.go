// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package raft

import "time"

// Raft 日志条目的命令类型。为向后兼容，历史上的纯 KV 条目没有该字段
// （零值即为 KV），因此这里把 KV 的类型常量留为空字符串。
const (
	cmdTypeKV     = ""       // 键值写入（历史格式，type 缺省）
	cmdTypeSwitch = "switch" // 追加一条结构化切换摘要
	cmdTypeAudit  = "audit"  // 追加一条审计日志
)

// 审计日志的类别。与控制台“审计日志”页的过滤条件一一对应。
const (
	AuditLifecycle = "lifecycle" // 切换 / 恢复的生命周期
	AuditConfig    = "config"    // 各类配置编辑
	AuditCluster   = "cluster"   // 集群与成员管理
	AuditSecurity  = "security"  // 安全运维操作
)

// FSM 内两类追加集合的保留条数上限。超过上限时从最旧的一端环形截断，
// 避免 Raft 日志与快照随时间无限膨胀。
const (
	maxSwitchRecords = 1000
	maxAuditEntries  = 10000
)

// SwitchRecord 是经由 Raft 复制的一条结构化切换摘要，是元数据库
// switch_history 行的镜像，用于在 Manager 控制面内提供高可用的历史查询。
type SwitchRecord struct {
	Seq     int64     `json:"seq"` // 由 FSM 在应用时分配的集群内单调序号
	Time    time.Time `json:"time"`
	Cluster string    `json:"cluster"`
	// Operation 为 failover / switchover / recover；RecoverMode 仅在
	// operation=recover 时取值 replica / topology / rollback。
	Operation    string `json:"operation"`
	RecoverMode  string `json:"recover_mode,omitempty"`
	OldMaster    string `json:"old_master"` // 切换前 host:port
	NewMaster    string `json:"new_master"` // 切换后 host:port
	AnalysisCode string `json:"analysis_code,omitempty"`
	Result       string `json:"result"` // SUCCESS / FAILED，与元数据库口径一致
	Node         string `json:"node"`   // 执行该操作的 Manager 节点 RaftID
}

// AuditEntry 是一条结构化审计日志。所有敏感 / 变更类操作都在 Leader 上
// 追加一条，随 Raft 复制到全部 Manager 节点。
type AuditEntry struct {
	Seq      int64     `json:"seq"`
	Time     time.Time `json:"time"`
	Category string    `json:"category"` // AuditLifecycle / AuditConfig / AuditCluster / AuditSecurity
	Action   string    `json:"action"`   // 形如 failover.start、config/topology.save
	Cluster  string    `json:"cluster,omitempty"`
	Target   string    `json:"target,omitempty"` // 操作针对的节点 / 对象
	Result   string    `json:"result"`           // started / success / failed / aborted
	SourceIP string    `json:"source_ip,omitempty"`
	Node     string    `json:"node"`             // 执行该操作的 Manager 节点 RaftID
	Detail   string    `json:"detail,omitempty"` // 附加细节（如命令、源 URL）
}

// applySwitch 将一条切换摘要追加到集合，并由 FSM 分配单调序号。该序号在
// Raft 确定的相同应用顺序下于各节点上保持一致。调用时必须持有 f.mu。
func (f *StoreFSM) applySwitch(r *SwitchRecord) error {
	if r == nil {
		return errMissingPayload("switch")
	}
	f.nextSwitchSeq++
	r.Seq = f.nextSwitchSeq
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	f.switches = append(f.switches, *r)
	if len(f.switches) > maxSwitchRecords {
		// 丢弃最旧的条目，仅保留最近 maxSwitchRecords 条。
		f.switches = f.switches[len(f.switches)-maxSwitchRecords:]
	}
	return nil
}

// applyAudit 将一条审计日志追加到集合，规则同 applySwitch。调用时必须持有 f.mu。
func (f *StoreFSM) applyAudit(a *AuditEntry) error {
	if a == nil {
		return errMissingPayload("audit")
	}
	f.nextAuditSeq++
	a.Seq = f.nextAuditSeq
	if a.Time.IsZero() {
		a.Time = time.Now()
	}
	f.audits = append(f.audits, *a)
	if len(f.audits) > maxAuditEntries {
		f.audits = f.audits[len(f.audits)-maxAuditEntries:]
	}
	return nil
}

// Switches 返回切换摘要，按最新在前排列。cluster / operation 非空时按其
// 过滤；limit > 0 时最多返回 limit 条。返回的是副本，调用方可安全使用。
func (f *StoreFSM) Switches(cluster, operation string, limit int) []SwitchRecord {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]SwitchRecord, 0, len(f.switches))
	for i := len(f.switches) - 1; i >= 0; i-- {
		r := f.switches[i]
		if cluster != "" && r.Cluster != cluster {
			continue
		}
		if operation != "" && r.Operation != operation {
			continue
		}
		out = append(out, r)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// Audits 返回审计日志，按最新在前排列。category / cluster 非空时按其
// 过滤；limit > 0 时最多返回 limit 条。
func (f *StoreFSM) Audits(category, cluster string, limit int) []AuditEntry {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]AuditEntry, 0, len(f.audits))
	for i := len(f.audits) - 1; i >= 0; i-- {
		a := f.audits[i]
		if category != "" && a.Category != category {
			continue
		}
		if cluster != "" && a.Cluster != cluster {
			continue
		}
		out = append(out, a)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// fsmData 是版本化的快照 / 恢复信封。Version 用于区分历史上“裸 KV map”
// 的旧快照格式（见 StoreFSM.Restore）。
type fsmData struct {
	Version int `json:"version"`

	Store         map[string]string `json:"store"`
	Switches      []SwitchRecord    `json:"switches"`
	NextSwitchSeq int64             `json:"next_switch_seq"`
	Audits        []AuditEntry      `json:"audits"`
	NextAuditSeq  int64             `json:"next_audit_seq"`
}

// currentSnapshotVersion 是当前写出的快照格式版本。
const currentSnapshotVersion = 1

// errMissingPayload 在类型化命令缺少对应载荷时返回，避免静默地写入空记录。
func errMissingPayload(kind string) error {
	return &applyError{msg: kind + " command missing payload"}
}

// applyError 是 FSM Apply 返回的简单错误类型。
type applyError struct{ msg string }

func (e *applyError) Error() string { return e.msg }

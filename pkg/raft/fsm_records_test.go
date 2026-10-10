// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

// applyLog 通过完整的 Apply 路径应用一条已编码命令，并返回其响应错误。
func applyLog(t *testing.T, f *StoreFSM, c command) {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	resp := f.Apply(&raft.Log{Data: data})
	if e, ok := resp.(error); ok && e != nil {
		t.Fatalf("Apply returned error: %v", e)
	}
}

func TestApplySwitchAssignsSeq(t *testing.T) {
	f := NewFSM()
	for i := 0; i < 3; i++ {
		applyLog(t, f, command{Type: cmdTypeSwitch, Switch: &SwitchRecord{
			Cluster: "cluster1", Operation: "failover", Result: "SUCCESS",
		}})
	}

	got := f.Switches("", "", 0)
	if len(got) != 3 {
		t.Fatalf("want 3 records, got %d", len(got))
	}
	// Switches 最新在前：序号应为 3,2,1。
	if got[0].Seq != 3 || got[1].Seq != 2 || got[2].Seq != 1 {
		t.Fatalf("want seq order 3,2,1 got %d,%d,%d", got[0].Seq, got[1].Seq, got[2].Seq)
	}
	if got[0].Time.IsZero() {
		t.Fatal("FSM should fill a zero timestamp on apply")
	}
}

func TestApplyAuditIndependentSeq(t *testing.T) {
	f := NewFSM()
	applyLog(t, f, command{Type: cmdTypeAudit, Audit: &AuditEntry{
		Category: AuditConfig, Action: "config/topology.save", Result: "success",
	}})

	got := f.Audits("", "", 0)
	if len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("want single audit with seq=1, got %+v", got)
	}
	// Switch 与 Audit 序号各自独立，互不影响。
	if f.nextSwitchSeq != 0 {
		t.Fatalf("switch seq should stay 0, got %d", f.nextSwitchSeq)
	}
}

// TestLegacyKVCommandBackwardCompat 验证无 type 字段的历史 KV 条目仍按 KV 应用。
func TestLegacyKVCommandBackwardCompat(t *testing.T) {
	f := NewFSM()
	applyLog(t, f, command{Key: "topology_policy/cluster1", Value: `{"detach_lost_replicas":true}`})

	v, ok := f.Get("topology_policy/cluster1")
	if !ok || !strings.Contains(v, "detach_lost_replicas") {
		t.Fatalf("legacy kv command not applied, v=%q ok=%v", v, ok)
	}
}

func TestTypedCommandMissingPayload(t *testing.T) {
	f := NewFSM()

	cases := []command{
		{Type: cmdTypeSwitch},
		{Type: cmdTypeAudit},
	}
	for _, c := range cases {
		data, _ := json.Marshal(c)
		resp := f.Apply(&raft.Log{Data: data})
		e, ok := resp.(error)
		if !ok || e == nil || !strings.Contains(e.Error(), "missing payload") {
			t.Fatalf("want missing-payload error for type=%q, got %v", c.Type, resp)
		}
	}
}

func TestSwitchFiltersAndLimit(t *testing.T) {
	f := NewFSM()
	applyLog(t, f, command{Type: cmdTypeSwitch, Switch: &SwitchRecord{Cluster: "c1", Operation: "failover", Result: "SUCCESS"}})
	applyLog(t, f, command{Type: cmdTypeSwitch, Switch: &SwitchRecord{Cluster: "c1", Operation: "switchover", Result: "SUCCESS"}})
	applyLog(t, f, command{Type: cmdTypeSwitch, Switch: &SwitchRecord{Cluster: "c2", Operation: "failover", Result: "FAILED"}})

	if got := f.Switches("c1", "", 0); len(got) != 2 {
		t.Fatalf("want 2 c1 records, got %d", len(got))
	}
	if got := f.Switches("c1", "switchover", 0); len(got) != 1 || got[0].Operation != "switchover" {
		t.Fatalf("cluster+operation filter wrong: %+v", got)
	}
	// limit=1 只返回最新一条。
	if got := f.Switches("", "", 1); len(got) != 1 || got[0].Seq != 3 {
		t.Fatalf("limit should return only the newest, got %+v", got)
	}
}

func TestAuditFiltersAndLimit(t *testing.T) {
	f := NewFSM()
	applyLog(t, f, command{Type: cmdTypeAudit, Audit: &AuditEntry{Category: AuditLifecycle, Cluster: "c1", Action: "failover.start", Result: "started"}})
	applyLog(t, f, command{Type: cmdTypeAudit, Audit: &AuditEntry{Category: AuditSecurity, Cluster: "c1", Action: "security/agent.exec", Result: "started"}})
	applyLog(t, f, command{Type: cmdTypeAudit, Audit: &AuditEntry{Category: AuditSecurity, Cluster: "c2", Action: "security/agent.fetch", Result: "started"}})

	if got := f.Audits(AuditSecurity, "", 0); len(got) != 2 {
		t.Fatalf("want 2 security audits, got %d", len(got))
	}
	if got := f.Audits(AuditSecurity, "c2", 0); len(got) != 1 || got[0].Cluster != "c2" {
		t.Fatalf("category+cluster filter wrong: %+v", got)
	}
}

// TestSwitchRingTruncation 验证超过上限时从最旧端截断，但序号继续单调增长。
func TestSwitchRingTruncation(t *testing.T) {
	f := NewFSM()
	f.mu.Lock()
	for i := 0; i < maxSwitchRecords+50; i++ {
		if err := f.applySwitch(&SwitchRecord{Cluster: "c", Operation: "failover", Result: "SUCCESS"}); err != nil {
			f.mu.Unlock()
			t.Fatal(err)
		}
	}
	f.mu.Unlock()

	if len(f.switches) != maxSwitchRecords {
		t.Fatalf("want %d retained, got %d", maxSwitchRecords, len(f.switches))
	}
	// 前 50 条已丢弃：窗口为 seq 51..1050。
	if f.switches[0].Seq != 51 {
		t.Fatalf("oldest retained should be 51, got %d", f.switches[0].Seq)
	}
	if last := f.switches[len(f.switches)-1].Seq; last != maxSwitchRecords+50 {
		t.Fatalf("newest should be %d, got %d", maxSwitchRecords+50, last)
	}
	if f.nextSwitchSeq != maxSwitchRecords+50 {
		t.Fatalf("next seq should keep growing regardless of truncation, got %d", f.nextSwitchSeq)
	}
}

func TestAuditRingTruncation(t *testing.T) {
	f := NewFSM()
	f.mu.Lock()
	for i := 0; i < maxAuditEntries+10; i++ {
		if err := f.applyAudit(&AuditEntry{Category: AuditConfig, Action: "x", Result: "success"}); err != nil {
			f.mu.Unlock()
			t.Fatal(err)
		}
	}
	f.mu.Unlock()

	if len(f.audits) != maxAuditEntries {
		t.Fatalf("want %d retained, got %d", maxAuditEntries, len(f.audits))
	}
	if f.audits[0].Seq != 11 || f.audits[len(f.audits)-1].Seq != maxAuditEntries+10 {
		t.Fatalf("audit truncation window wrong: %d..%d",
			f.audits[0].Seq, f.audits[len(f.audits)-1].Seq)
	}
}

// TestSnapshotRestoreRoundTrip 验证版本化快照能完整携带 KV 与两类集合，
// 且恢复后状态一致（含单调序号的延续）。
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	f := NewFSM()
	f.mu.Lock()
	f.store["k1"] = "v1"
	if err := f.applySwitch(&SwitchRecord{Cluster: "c1", Operation: "failover", Result: "SUCCESS", NewMaster: "h:1"}); err != nil {
		f.mu.Unlock()
		t.Fatal(err)
	}
	if err := f.applyAudit(&AuditEntry{Category: AuditLifecycle, Action: "failover.start", Result: "started"}); err != nil {
		f.mu.Unlock()
		t.Fatal(err)
	}
	f.mu.Unlock()

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Persist 的核心就是把版本化信封 JSON 写出；这里直接编码后走 Restore。
	b, err := json.Marshal(snap.(*fsmSnapshot).data)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	g := NewFSM()
	if err := g.Restore(io.NopCloser(bytes.NewReader(b))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if g.store["k1"] != "v1" {
		t.Fatalf("kv not restored: %v", g.store)
	}
	if sw := g.Switches("", "", 0); len(sw) != 1 || sw[0].NewMaster != "h:1" || sw[0].Seq != 1 {
		t.Fatalf("switches not restored: %+v", sw)
	}
	if au := g.Audits("", "", 0); len(au) != 1 || au[0].Action != "failover.start" {
		t.Fatalf("audits not restored: %+v", au)
	}
	if g.nextSwitchSeq != 1 || g.nextAuditSeq != 1 {
		t.Fatalf("counters not restored: switch=%d audit=%d", g.nextSwitchSeq, g.nextAuditSeq)
	}
}

// TestRestoreLegacySnapshot 验证旧的“裸 KV map”快照仍可恢复，且不引入集合。
func TestRestoreLegacySnapshot(t *testing.T) {
	g := NewFSM()
	if err := g.Restore(io.NopCloser(strings.NewReader(`{"k1":"v1","k2":"v2"}`))); err != nil {
		t.Fatalf("Restore legacy: %v", err)
	}
	if g.store["k1"] != "v1" || g.store["k2"] != "v2" {
		t.Fatalf("legacy kv not restored: %v", g.store)
	}
	if len(g.switches) != 0 || len(g.audits) != 0 {
		t.Fatal("legacy restore must not introduce append collections")
	}
	if g.nextSwitchSeq != 0 || g.nextAuditSeq != 0 {
		t.Fatal("legacy restore must reset counters")
	}
}

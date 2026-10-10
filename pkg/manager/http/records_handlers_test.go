// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	hraft "github.com/hashicorp/raft"

	"github.com/yunnnnn1/go-helix/pkg/raft"
)

// newRecordsServer 构造一个仅装配 FSM 的节点（不启动真实 Raft），用于测试
// 两个只读端点；FSM.Apply / List 路径都不会访问底层 Raft。
func newRecordsServer(t *testing.T) *Server {
	t.Helper()
	return NewServer(nil, &raft.Node{FSM: raft.NewFSM(), RaftID: "node-test"})
}

// seedApply 直接经 FSM.Apply 写入一条已编码命令。
func seedApply(t *testing.T, node *raft.Node, payload map[string]interface{}) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	if resp := node.FSM.Apply(&hraft.Log{Data: data}); resp != nil {
		if e, ok := resp.(error); ok && e != nil {
			t.Fatalf("seed Apply error: %v", e)
		}
	}
}

func TestHandleSwitchRecordsListAndFilter(t *testing.T) {
	s := newRecordsServer(t)
	node := s.raftNode

	seedApply(t, node, map[string]interface{}{
		"type":   "switch",
		"switch": &raft.SwitchRecord{Cluster: "c1", Operation: "failover", Result: "SUCCESS", NewMaster: "h1:1"},
	})
	seedApply(t, node, map[string]interface{}{
		"type":   "switch",
		"switch": &raft.SwitchRecord{Cluster: "c1", Operation: "switchover", Result: "SUCCESS"},
	})
	seedApply(t, node, map[string]interface{}{
		"type":   "switch",
		"switch": &raft.SwitchRecord{Cluster: "c2", Operation: "failover", Result: "FAILED"},
	})

	get := func(target string) (int, *struct {
		Records []raft.SwitchRecord `json:"records"`
		Count   int                 `json:"count"`
	}) {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		w := httptest.NewRecorder()
		s.handleSwitchRecords(w, req)

		var out struct {
			Records []raft.SwitchRecord `json:"records"`
			Count   int                 `json:"count"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatalf("invalid json (%d): %v body=%s", w.Code, err, w.Body.String())
		}
		return w.Code, &out
	}

	// 全量：3 条，最新在前（seq=3 属 c2）。
	code, all := get("/switch-records")
	if code != http.StatusOK || all.Count != 3 {
		t.Fatalf("want 200/3, got %d/%d", code, all.Count)
	}
	if all.Records[0].Cluster != "c2" || all.Records[0].Seq != 3 {
		t.Fatalf("newest-first order wrong: %+v", all.Records[0])
	}

	// 集群 + 操作过滤。
	if _, one := get("/switch-records?cluster=c1&operation=switchover"); one.Count != 1 ||
		one.Records[0].Operation != "switchover" {
		t.Fatalf("cluster+operation filter wrong: %+v", one.Records)
	}

	// limit 钳制为 1 条（最新）。
	if _, limited := get("/switch-records?limit=1"); limited.Count != 1 || limited.Records[0].Seq != 3 {
		t.Fatalf("limit wrong: %+v", limited.Records)
	}
}

func TestHandleAuditLogsListAndFilter(t *testing.T) {
	s := newRecordsServer(t)
	node := s.raftNode

	seedApply(t, node, map[string]interface{}{
		"type":  "audit",
		"audit": &raft.AuditEntry{Category: raft.AuditLifecycle, Cluster: "c1", Action: "failover.start", Result: "started"},
	})
	seedApply(t, node, map[string]interface{}{
		"type":  "audit",
		"audit": &raft.AuditEntry{Category: raft.AuditSecurity, Cluster: "c1", Action: "security/agent.exec", Result: "started"},
	})
	seedApply(t, node, map[string]interface{}{
		"type":  "audit",
		"audit": &raft.AuditEntry{Category: raft.AuditSecurity, Cluster: "c2", Action: "security/agent.fetch", Result: "started"},
	})

	req := httptest.NewRequest(http.MethodGet, "/audit-logs?category=security&cluster=c2", nil)
	w := httptest.NewRecorder()
	s.handleAuditLogs(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var out struct {
		AuditLogs []raft.AuditEntry `json:"audit_logs"`
		Count     int               `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if out.Count != 1 || out.AuditLogs[0].Cluster != "c2" {
		t.Fatalf("category+cluster filter wrong: %+v", out.AuditLogs)
	}
}

// TestRecordsEndpointsEmptyAreArrays 验证无数据时返回空数组而不是 null。
func TestRecordsEndpointsEmptyAreArrays(t *testing.T) {
	s := newRecordsServer(t)

	for path, h := range map[string]http.HandlerFunc{
		"/switch-records": s.handleSwitchRecords,
		"/audit-logs":     s.handleAuditLogs,
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		h(w, req)

		body := w.Body.String()
		if strings.Contains(body, ":null") {
			t.Fatalf("%s empty response must use [], not null: %s", path, body)
		}
	}
}

func TestRecordsEndpointsMethodNotAllowed(t *testing.T) {
	s := newRecordsServer(t)

	for path, h := range map[string]http.HandlerFunc{
		"/switch-records": s.handleSwitchRecords,
		"/audit-logs":     s.handleAuditLogs,
	} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		w := httptest.NewRecorder()
		h(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s POST want 405, got %d", path, w.Code)
		}
	}
}

// TestRecordsEndpointsRegisteredAsAuthAPI 保证两个新路径纳入 Bearer 鉴权集合。
func TestRecordsEndpointsRegisteredAsAuthAPI(t *testing.T) {
	if !apiPaths["/switch-records"] || !apiPaths["/audit-logs"] {
		t.Fatalf("new record paths must require auth: switch=%v audit=%v",
			apiPaths["/switch-records"], apiPaths["/audit-logs"])
	}
}

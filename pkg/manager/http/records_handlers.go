package http

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/yunnnnn1/go-helix/pkg/raft"
)

const (
	defaultRecordsLimit = 100
	maxRecordsLimit     = 1000
)

// parseRecordsLimit 解析 limit 查询参数：缺省 / 非法时用默认值，并对最大值
// 做钳制，避免一次请求拉取过多记录。
func parseRecordsLimit(v string) int {
	if v == "" {
		return defaultRecordsLimit
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultRecordsLimit
	}
	if n > maxRecordsLimit {
		return maxRecordsLimit
	}
	return n
}

// handleSwitchRecords 返回经 Raft 复制的结构化切换摘要。数据已复制到每个
// Manager 节点，因此任意节点都可直接返回、无需转发给 Leader。
// 可选过滤：cluster、operation；limit 控制返回条数。
func (s *Server) handleSwitchRecords(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	records := s.raftNode.ListSwitches(q.Get("cluster"), q.Get("operation"),
		parseRecordsLimit(q.Get("limit")))
	if records == nil {
		records = []raft.SwitchRecord{}
	}

	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"records": records,
		"count":   len(records),
	})
}

// handleAuditLogs 返回经 Raft 复制的审计日志。任意节点可直接返回。
// 可选过滤：category、cluster；limit 控制返回条数。
func (s *Server) handleAuditLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	entries := s.raftNode.ListAudits(q.Get("category"), q.Get("cluster"),
		parseRecordsLimit(q.Get("limit")))
	if entries == nil {
		entries = []raft.AuditEntry{}
	}

	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"audit_logs": entries,
		"count":      len(entries),
	})
}

package http

import (
	"encoding/json"
	"net/http"

	"github.com/yunnnnn1/go-helix/pkg/proto"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

// ExecStreamRequest 是 /agent/exec-stream 的请求体。它在节点上运行一个已
// 加入白名单的命令，并以换行分隔的 JSON（NDJSON）流式输出结果。
type ExecStreamRequest struct {
	Cluster        string   `json:"cluster"`
	Node           string   `json:"node"` // host 或 host:port
	Command        string   `json:"command"`
	Args           []string `json:"args"`
	TimeoutSeconds int32    `json:"timeout_seconds"`
}

// handleAgentExecStream 将服务端流式 Agent RPC（ExecStream）以 NDJSON 形式
// 代理给浏览器。每一行都是一个 ExecStreamLine 对象。
func (s *Server) handleAgentExecStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 敏感操作统一在 Leader 上执行并审计；Follower 将流式请求转发给 Leader。
	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var req ExecStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Cluster == "" || req.Node == "" || req.Command == "" {
		http.Error(w, "cluster, node and command are required", http.StatusBadRequest)
		return
	}

	agentAddr, _, err := s.monitor.AgentAddrForNode(req.Cluster, req.Node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	client, err := s.monitor.AgentClient(agentAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	// 审计：命令执行已授权并准备发起（记录命令，便于事后追溯）。
	ae := s.newAudit(r, raft.AuditSecurity, "security/agent.exec", "started")
	ae.Cluster = req.Cluster
	ae.Target = req.Node
	ae.Detail = "command=" + req.Command
	s.raftNode.EmitAudit(ae)

	stream, err := client.ExecStream(r.Context(), &proto.ExecStreamRequest{
		Command:        req.Command,
		Args:           req.Args,
		TimeoutSeconds: req.TimeoutSeconds,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	writeNoStore(w)
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	enc := json.NewEncoder(w)

	for {
		line, err := stream.Recv()
		if err != nil {
			// io.EOF 是正常的结束标志；对于传输错误，则输出最后一个对象呈现给客户端。
			if err.Error() != "EOF" {
				_ = enc.Encode(map[string]interface{}{"final": true, "error": err.Error()})
				flusher.Flush()
			}
			return
		}
		if err := enc.Encode(line); err != nil {
			return // 客户端已断开
		}
		flusher.Flush()
	}
}

// FetchFileRequest 是 /agent/fetch-file 的请求体。
type FetchFileRequest struct {
	Cluster   string `json:"cluster"`
	Node      string `json:"node"`
	SourceURL string `json:"source_url"`
	TargetDir string `json:"target_dir"`
	Filename  string `json:"filename"`
	SHA256    string `json:"sha256"`
	MaxBytes  int64  `json:"max_bytes"`
	LimitKbps int64  `json:"limit_kbps"`
}

// handleAgentFetchFile 将 Agent 的 FetchFile 服务端流以 NDJSON 形式代理给
// 浏览器。
func (s *Server) handleAgentFetchFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 敏感操作统一在 Leader 上执行并审计；Follower 将流式请求转发给 Leader。
	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var req FetchFileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Cluster == "" || req.Node == "" || req.SourceURL == "" ||
		req.TargetDir == "" || req.Filename == "" {
		http.Error(w, "cluster, node, source_url, target_dir and filename are required", http.StatusBadRequest)
		return
	}

	agentAddr, _, err := s.monitor.AgentAddrForNode(req.Cluster, req.Node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	client, err := s.monitor.AgentClient(agentAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	// 审计：安全文件拉取已授权并准备发起（记录源 URL）。
	ae := s.newAudit(r, raft.AuditSecurity, "security/agent.fetch", "started")
	ae.Cluster = req.Cluster
	ae.Target = req.Node
	ae.Detail = "url=" + req.SourceURL
	s.raftNode.EmitAudit(ae)

	stream, err := client.FetchFile(r.Context(), &proto.FetchFileRequest{
		SourceUrl: req.SourceURL,
		TargetDir: req.TargetDir,
		Filename:  req.Filename,
		Sha256:    req.SHA256,
		MaxBytes:  req.MaxBytes,
		LimitKbps: req.LimitKbps,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	writeNoStore(w)
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	enc := json.NewEncoder(w)

	for {
		prog, err := stream.Recv()
		if err != nil {
			if err.Error() != "EOF" {
				_ = enc.Encode(map[string]interface{}{"phase": "error", "message": err.Error()})
				flusher.Flush()
			}
			return
		}
		if err := enc.Encode(prog); err != nil {
			return
		}
		flusher.Flush()
	}
}

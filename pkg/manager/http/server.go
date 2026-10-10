package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/manager/monitor"
	"github.com/yunnnnn1/go-helix/pkg/manager/switchlog"
	"github.com/yunnnnn1/go-helix/pkg/manager/web"
	"github.com/yunnnnn1/go-helix/pkg/notify"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

type Server struct {
	monitor  *monitor.Monitor
	raftNode *raft.Node

	httpServer *http.Server

	// tlsCert/tlsKey 非空时以 HTTPS 提供服务（双向校验由内部 gRPC 侧
	// 负责；HTTP 控制面默认明文，启用 manager.tls 后切到 TLS）。
	tlsCert string
	tlsKey  string

	// startTime 作为 uptime（运行时长）指标的锚点。
	startTime time.Time
}

// SetTLS 在启用 manager.tls 时配置 HTTPS 证书，Start 将据此使用
// ListenAndServeTLS。
func (s *Server) SetTLS(c config.TLSConfig) error {
	if err := c.Validate(); err != nil {
		return err
	}
	s.tlsCert, s.tlsKey = c.CertFile, c.KeyFile
	return nil
}

func NewServer(m *monitor.Monitor, r *raft.Node) *Server {
	return &Server{
		monitor:   m,
		raftNode:  r,
		startTime: time.Now(),
	}
}

// noAuthPaths 是无需 Bearer 令牌即可访问的可观测性端点（负载均衡器的健康
// 探针和 Prometheus 的抓取器都不会携带令牌）。这些端点仅暴露存活/就绪状态
// 以及非敏感指标。
var noAuthPaths = map[string]bool{
	"/health":  true,
	"/ready":   true,
	"/metrics": true,
}

// apiPaths 是需要 API 鉴权的确切路由集合。其余所有路径都属于内嵌的 Web
// 控制台，无需令牌即可提供服务（控制台在调用这些 API 时会在页面内附带
// Bearer 令牌）。
var apiPaths = map[string]bool{
	"/failover":             true,
	"/switchover":           true,
	"/recover":              true,
	"/join":                 true,
	"/leave":                true,
	"/status":               true,
	"/leader":               true,
	"/clusters":             true,
	"/raft/nodes":           true,
	"/switchlogs":           true,
	"/switchlogs/aggregate": true,
	"/switchlog/content":    true,
	"/recover/preflight":    true,
	"/config/topology":      true,
	"/config/priority":      true,
	"/config/notify":        true,
	"/cluster/register":     true,
	"/raft/priority":        true,
	"/switch-records":       true,
	"/audit-logs":           true,
	"/agent/exec-stream":    true,
	"/agent/fetch-file":     true,
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()

	// 可观测性端点（无需令牌；在鉴权包装器之前注册）。
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)

	// 鉴权中间件
	authMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg := config.Get(); cfg != nil && cfg.AuthToken != "" {
			// 去掉可能存在的 "Bearer " 前缀；无该前缀时 TrimPrefix 原样返回。
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(token), []byte(cfg.AuthToken)) != 1 {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})

	mux.HandleFunc("/failover", s.handleFailover)
	mux.HandleFunc("/switchover", s.handleSwitchover)
	mux.HandleFunc("/recover", s.handleRecover)
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/leave", s.handleLeave)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/leader", s.handleLeader)
	mux.HandleFunc("/clusters", s.handleClusters)
	mux.HandleFunc("/raft/nodes", s.handleRaftNodes)
	mux.HandleFunc("/switchlogs", s.handleSwitchLogs)
	mux.HandleFunc("/switchlogs/aggregate", s.handleAggregateSwitchLogs)
	mux.HandleFunc("/switchlog/content", s.handleSwitchLogContent)
	mux.HandleFunc("/recover/preflight", s.handleRecoverPreflight)
	mux.HandleFunc("/config/topology", s.handleConfigTopology)
	mux.HandleFunc("/config/priority", s.handleConfigPriority)
	mux.HandleFunc("/config/notify", s.handleConfigNotify)
	mux.HandleFunc("/cluster/register", s.handleClusterRegister)
	mux.HandleFunc("/raft/priority", s.handleRaftPriority)

	// 复制到 FSM 的结构化切换摘要 / 审计日志（任意节点可读）。
	mux.HandleFunc("/switch-records", s.handleSwitchRecords)
	mux.HandleFunc("/audit-logs", s.handleAuditLogs)

	// 流式主机操作（代理转发到节点 Agent）。
	mux.HandleFunc("/agent/exec-stream", s.handleAgentExecStream)
	mux.HandleFunc("/agent/fetch-file", s.handleAgentFetchFile)

	// 内嵌的 Web 控制台（无需 API 鉴权即可提供服务）。
	staticFS, err := web.StaticFS()
	if err != nil {
		return err
	}
	fileServer := http.FileServer(http.FS(staticFS))
	staticHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 内嵌静态资源会随每次二进制发布重新构建，但既没有带版本号的 URL，
		// 也没有校验机制（内嵌文件的 ModTime 为零值），因此若不设置此响应头，
		// 浏览器会展示陈旧的缓存控制台（例如升级后工具栏上缺少某个开关）。
		writeNoStore(w)
		// 若资源存在则直接返回；否则回退到 index.html。
		if p := strings.TrimPrefix(r.URL.Path, "/"); p != "" {
			if f, oErr := staticFS.Open(p); oErr == nil {
				f.Close()
				fileServer.ServeHTTP(w, r)
				return
			}
		}
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})

	// 顶层分发器：可观测性路由无需鉴权，API 路由需要鉴权，其余请求则返回
	// 控制台。
	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if noAuthPaths[r.URL.Path] {
			mux.ServeHTTP(w, r)
			return
		}
		if apiPaths[r.URL.Path] {
			authMux.ServeHTTP(w, r)
			return
		}
		staticHandler.ServeHTTP(w, r)
	})

	// 超时设置：ReadHeaderTimeout 防 Slowloris；ReadTimeout 限制读取完整
	// 请求；IdleTimeout 限制空闲连接。不设 WriteTimeout，以免掐断
	// NDJSON 流式响应。
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           recoveryHandler(rootHandler),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	if s.tlsCert != "" {
		log.Printf("API Server + Web console listening on %s (TLS)", addr)
		if err := s.httpServer.ListenAndServeTLS(s.tlsCert, s.tlsKey); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}

	log.Printf("API Server + Web console listening on %s", addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// recoveryHandler 捕获下游 handler 的 panic，记录日志并返回 500，避免
// 单个意外 panic 导致整个 Manager 进程崩溃。
func recoveryHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[Recovery] %s panic: %v", r.URL.Path, rec)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// Shutdown 优雅地停止 API 服务器，在 ctx 超时之前等待所有在途请求处理
// 完成。
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clusterName := r.URL.Query().Get("cluster")
	if clusterName == "" {
		http.Error(w, "cluster parameter is required", http.StatusBadRequest)
		return
	}

	nodes, err := s.monitor.GetClusterNodes(clusterName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get cluster nodes: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cluster": clusterName,
		"nodes":   nodes,
	})
}

func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	isLeader := s.raftNode.IsLeader()
	leaderAddr := s.raftNode.LeaderAddr()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"is_leader":   isLeader,
		"leader_addr": leaderAddr,
	})
}

// handleClusters 返回所有受管集群的聚合概览。
func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clusters, err := s.monitor.GetClusters()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list clusters: %v", err), http.StatusInternalServerError)
		return
	}
	if clusters == nil {
		clusters = []*datacenter.ClusterSummary{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"clusters": clusters,
		"count":    len(clusters),
	})
}

type ClusterRequest struct {
	ClusterName   string `json:"cluster_name"`
	PreferredNode string `json:"preferred_node"`
}

// handleFailover 触发紧急故障切换。AnalysisCode 由系统自动判定。
func (s *Server) handleFailover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 集群状态变更必须经由 Leader 处理。Follower 会把请求代理转发给当前
	// Leader，这样调用方可以固定使用同一个地址。
	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var req ClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.ClusterName == "" {
		http.Error(w, "cluster_name is required", http.StatusBadRequest)
		return
	}

	log.Printf("[API] Emergency failover requested for cluster %s (preferred: %s)", req.ClusterName, req.PreferredNode)

	err := s.monitor.TriggerManualFailover(req.ClusterName, req.PreferredNode, false)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failover failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Emergency failover initiated",
	})
}

// handleSwitchover 触发优雅的、计划内的主从切换（GracefulMasterTakeover）。
func (s *Server) handleSwitchover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 当本节点为 Follower 时，代理转发给 Leader（参见 handleFailover）。
	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var req ClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.ClusterName == "" {
		http.Error(w, "cluster_name is required", http.StatusBadRequest)
		return
	}

	log.Printf("[API] Graceful switchover requested for cluster %s (preferred: %s)", req.ClusterName, req.PreferredNode)

	err := s.monitor.TriggerManualFailover(req.ClusterName, req.PreferredNode, true)
	if err != nil {
		http.Error(w, fmt.Sprintf("Switchover failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Graceful switchover initiated",
	})
}

// RecoverRequest 用于触发一次主从恢复。
type RecoverRequest struct {
	ClusterName string `json:"cluster_name"`
	Mode        string `json:"mode"`        // replica / topology / rollback
	TargetNode  string `json:"target_node"` // 目标副本（replica 模式）或期望的主库（topology 模式）
}

// handleRecover 触发一次主从恢复。恢复操作在 Leader 上执行；Follower 会
// 代理转发该请求。
func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var req RecoverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.ClusterName == "" {
		http.Error(w, "cluster_name is required", http.StatusBadRequest)
		return
	}

	log.Printf("[API] Recover requested for cluster %s mode=%s target=%s", req.ClusterName, req.Mode, req.TargetNode)

	if err := s.monitor.TriggerRecover(req.ClusterName, req.Mode, req.TargetNode); err != nil {
		http.Error(w, fmt.Sprintf("Recover failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Recovery initiated",
	})
}

// handleRecoverPreflight 返回只读的回滚预检（preflight）预览：需要逆向的
// 切换记录、当前/恢复后的节点快照，以及具体的前置条件检查结果。该接口不
// 执行任何状态变更。
func (s *Server) handleRecoverPreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cluster := r.URL.Query().Get("cluster")
	if cluster == "" {
		http.Error(w, "cluster parameter is required", http.StatusBadRequest)
		return
	}

	preflight, err := s.monitor.PreflightRollback(cluster)
	if err != nil {
		http.Error(w, fmt.Sprintf("Preflight failed: %v", err), http.StatusInternalServerError)
		return
	}

	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(preflight)
}

// handleConfigTopology 根据必填查询参数 `cluster`，返回（GET）或更新
// （POST）单个集群当前生效的拓扑策略。GET 反映该集群解析后的策略（集群级
// 覆盖 > 配置文件值 > 内置默认值 true）。POST 必须在 Leader 上执行
// （Follower 请求会被代理转发）；该值通过 Raft 提交，因此在整个集群范围内
// 保持一致，并且在 Leader 变更/重启后依然持久有效。
func (s *Server) handleConfigTopology(w http.ResponseWriter, r *http.Request) {
	cluster := r.URL.Query().Get("cluster")
	if cluster == "" {
		http.Error(w, "Missing required query parameter: cluster", http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodPost {
		if !s.raftNode.IsLeader() {
			s.forwardToLeader(w, r)
			return
		}

		var req struct {
			DetachLostReplicas         bool `json:"detach_lost_replicas"`
			PreserveHierarchy          bool `json:"preserve_hierarchy"`
			IntermediateMasterRecovery bool `json:"intermediate_master_recovery"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		payload, err := json.Marshal(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("Encode policy failed: %v", err), http.StatusInternalServerError)
			return
		}
		if err := s.raftNode.PutKV(raft.TopologyPolicyKey(cluster), string(payload)); err != nil {
			http.Error(w, fmt.Sprintf("Persist topology policy failed: %v", err), http.StatusInternalServerError)
			return
		}
		log.Printf("[API] Topology policy for cluster %s updated and committed: %s", cluster, string(payload))

		// 审计：拓扑策略已保存（由 Leader 提交）。
		ae := s.newAudit(r, raft.AuditConfig, "config/topology.save", "success")
		ae.Cluster = cluster
		ae.Detail = string(payload)
		s.raftNode.EmitAudit(ae)

		writeNoStore(w)
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topo := config.EffectiveTopology(cluster)

	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{
		"detach_lost_replicas":         topo.Detach(),
		"preserve_hierarchy":           topo.Preserve(),
		"intermediate_master_recovery": topo.IntermediateRecoverEnabled(),
	})
}

type JoinRequest struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	APIAddr  string `json:"api_addr"`
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.NodeID == "" || req.RaftAddr == "" {
		http.Error(w, "node_id and raft_addr are required", http.StatusBadRequest)
		return
	}

	log.Printf("[API] Received join request from %s (%s, api %s)", req.NodeID, req.RaftAddr, req.APIAddr)

	if err := s.monitor.JoinNode(req.NodeID, req.RaftAddr); err != nil {
		http.Error(w, fmt.Sprintf("Failed to join node: %v", err), http.StatusInternalServerError)
		return
	}

	// 审计：新节点加入集群。
	ae := s.newAudit(r, raft.AuditCluster, "cluster/join", "success")
	ae.Target = req.NodeID
	ae.Detail = "raft_addr=" + req.RaftAddr
	s.raftNode.EmitAudit(ae)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Node joined successfully",
	})
}

type LeaveRequest struct {
	NodeID string `json:"node_id"`
}

// handleLeave 将一个成员从 Raft 集群中永久移除。此后 Leader 将不再与该节点
// 通信。这是一个运维操作：只有在确认该节点已永久下线后才可调用。
func (s *Server) handleLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LeaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.NodeID == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}

	log.Printf("[API] Received leave request for %s", req.NodeID)

	if err := s.monitor.RemoveNode(req.NodeID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to remove node: %v", err), http.StatusInternalServerError)
		return
	}

	// 审计：成员被永久移除。
	ae := s.newAudit(r, raft.AuditCluster, "cluster/leave", "success")
	ae.Target = req.NodeID
	s.raftNode.EmitAudit(ae)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Node removed from cluster",
	})
}

// leaderAPITarget 从已配置的节点列表中解析出指定 Leader 的 API 地址。当
// Leader 未知或缺少其 API 地址时，返回 "" 和 false。
func leaderAPITarget(peers []config.RaftPeerConfig, leaderID string) (string, bool) {
	if leaderID == "" {
		return "", false
	}
	for _, p := range peers {
		if p.NodeID == leaderID && p.APIAddr != "" {
			return p.APIAddr, true
		}
	}
	return "", false
}

// forwardToLeader 将传入请求代理转发给当前 Raft Leader 的 API 服务器，保留
// 请求方法、路径、请求体和请求头。当当前没有已知或可达的 Leader 时，返回
// 503/502，以便调用方重试。
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request) {
	leaderID := s.raftNode.LeaderID()

	var peers []config.RaftPeerConfig
	if cfg := config.Get(); cfg != nil {
		peers = cfg.Manager.RaftPeers
	}

	target, ok := leaderAPITarget(peers, leaderID)
	if !ok {
		log.Printf("[API] Cannot forward %s %s: no leader currently known", r.Method, r.URL.Path)
		http.Error(w, "no leader available", http.StatusServiceUnavailable)
		return
	}

	base, err := url.Parse("http://" + target)
	if err != nil {
		http.Error(w, "invalid leader address", http.StatusBadGateway)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(base)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, pErr error) {
		log.Printf("[API] Forward to leader %s (%s) failed: %v", leaderID, target, pErr)
		http.Error(w, fmt.Sprintf("forward to leader failed: %v", pErr), http.StatusBadGateway)
	}

	log.Printf("[API] Forwarding %s %s to leader %s (%s)", r.Method, r.URL.Path, leaderID, target)
	proxy.ServeHTTP(w, r)
}

// peersConfig 返回已配置的 Raft 节点列表。
func (s *Server) peersConfig() []config.RaftPeerConfig {
	if cfg := config.Get(); cfg != nil {
		return cfg.Manager.RaftPeers
	}
	return nil
}

// selfAPIAddr 返回本节点自身配置的 API 地址；若节点列表中没有本节点的
// 条目，则返回 ""。
func (s *Server) selfAPIAddr() string {
	for _, p := range s.peersConfig() {
		if p.NodeID == s.raftNode.RaftID && p.APIAddr != "" {
			return p.APIAddr
		}
	}
	return ""
}

// validatePeerTarget 校验 target 是否与某个已配置节点的 API 地址完全匹配。
// 这可以防止 node 参数使该端点沦为可访问任意内部主机的开放代理。
func validatePeerTarget(peers []config.RaftPeerConfig, target string) bool {
	for _, p := range peers {
		if p.APIAddr != "" && p.APIAddr == target {
			return true
		}
	}
	return false
}

// forwardToNode 将请求代理转发给某个指定节点的 API 地址。调用方必须传入
// 已经过节点列表校验的 target。
func (s *Server) forwardToNode(target string, w http.ResponseWriter, r *http.Request) {
	base, err := url.Parse("http://" + target)
	if err != nil {
		http.Error(w, "invalid target address", http.StatusBadGateway)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(base)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, pErr error) {
		log.Printf("[API] Forward to node %s failed: %v", target, pErr)
		http.Error(w, fmt.Sprintf("forward to node failed: %v", pErr), http.StatusBadGateway)
	}

	log.Printf("[API] Forwarding %s %s to node %s", r.Method, r.URL.Path, target)
	proxy.ServeHTTP(w, r)
}

// raftMember 是视图构建器所需的最小运行时成员数据，与 hashicorp/raft 的
// 类型解耦，以便可以直接对其进行单元测试。
type raftMember struct {
	ID       string
	Addr     string
	Suffrage string // "Voter" / "Nonvoter" / "Staging"
}

// RaftNodeView 描述 Raft 集群中的一个节点及其状态，供 API 使用。
type RaftNodeView struct {
	NodeID    string `json:"node_id"`
	RaftAddr  string `json:"raft_addr"`
	APIAddr   string `json:"api_addr"`
	Role      string `json:"role"`     // leader / follower / non-voter / pending
	Suffrage  string `json:"suffrage"` // Voter / Nonvoter / Staging / ""（pending 时为空）
	IsSelf    bool   `json:"is_self"`
	InCluster bool   `json:"in_cluster"` // 是否存在于当前生效的 Raft 配置中
}

// buildRaftNodeViews 将当前生效的 Raft 配置（实际成员及其投票情况）与已配置
// 的期望节点列表合并（后者提供每个节点的 API 地址，并暴露出那些已配置但
// 尚未加入的节点）。
//
// 角色推导规则：
//   - 当前生效且为现任 Leader 的 Voter      -> "leader"
//   - 当前生效但不是 Leader 的 Voter        -> "follower"
//   - 当前生效的 non-voter / staging 节点   -> "non-voter"
//   - 在期望列表中但尚未生效                -> "pending"
//   - 当前生效但不在期望列表中的成员        -> 按 suffrage 确定角色（不受管）
func buildRaftNodeViews(members []raftMember, peers []config.RaftPeerConfig, leaderID, selfID string) []RaftNodeView {
	apiByID := make(map[string]string, len(peers))
	desired := make(map[string]bool, len(peers))
	order := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.NodeID == "" {
			continue
		}
		apiByID[p.NodeID] = p.APIAddr
		if !desired[p.NodeID] {
			desired[p.NodeID] = true
			order = append(order, p.NodeID)
		}
	}

	seen := make(map[string]bool)
	views := make([]RaftNodeView, 0, len(members)+len(peers))

	add := func(id, raftAddr, suffrage string, inCluster bool) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true

		role := "pending"
		if inCluster {
			switch {
			case id == leaderID:
				role = "leader"
			case suffrage == "Nonvoter" || suffrage == "Staging":
				role = "non-voter"
			default:
				role = "follower"
			}
		}

		views = append(views, RaftNodeView{
			NodeID:    id,
			RaftAddr:  raftAddr,
			APIAddr:   apiByID[id],
			Role:      role,
			Suffrage:  suffrage,
			IsSelf:    id == selfID,
			InCluster: inCluster,
		})
	}

	// 先处理当前生效的成员（具有权威性），保持运行时顺序。
	for _, m := range members {
		add(m.ID, m.Addr, m.Suffrage, true)
	}
	// 再处理已期望加入但尚未加入的节点，按配置顺序排列。
	for _, id := range order {
		add(id, "", "", false)
	}
	return views
}

// handleRaftNodes 返回当前 Raft 集群成员及其状态。任何节点都可以响应此请求
// （Raft 配置是多副本复制的），因此不会代理转发给 Leader。
func (s *Server) handleRaftNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	conf, err := s.raftNode.Configuration()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read cluster configuration: %v", err), http.StatusInternalServerError)
		return
	}

	members := make([]raftMember, 0, len(conf.Servers))
	for _, srv := range conf.Servers {
		members = append(members, raftMember{
			ID:       string(srv.ID),
			Addr:     string(srv.Address),
			Suffrage: srv.Suffrage.String(),
		})
	}

	var peers []config.RaftPeerConfig
	if cfg := config.Get(); cfg != nil {
		peers = cfg.Manager.RaftPeers
	}

	views := buildRaftNodeViews(members, peers, s.raftNode.LeaderID(), s.raftNode.RaftID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"leader_id": s.raftNode.LeaderID(),
		"self_id":   s.raftNode.RaftID,
		"tier":      s.monitor.Tier(),
		"nodes":     views,
	})
}

// handleConfigPriority 返回或更新单个集群的优先级。
//
// GET  ?cluster=<name>：读取该集群当前生效的优先级（本地读取，
//
//	任意节点均可响应）。
//
// POST ?cluster=<name>：请求体为 {"cluster_priority":"P0"}；仅限 Leader，
//
//	Follower 会被代理转发给 Leader。
func (s *Server) handleConfigPriority(w http.ResponseWriter, r *http.Request) {
	cluster := r.URL.Query().Get("cluster")
	if cluster == "" {
		http.Error(w, "Missing required query parameter: cluster", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		meta, err := s.monitor.GetClusterMeta(cluster)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read cluster meta: %v", err), http.StatusInternalServerError)
			return
		}
		writeNoStore(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"cluster_name":     meta.ClusterName,
			"cluster_priority": meta.ClusterPriority,
			"registered":       meta.Registered,
			"endpoint_type":    meta.EndpointType,
			"endpoint_value":   meta.EndpointValue,
		})

	case http.MethodPost:
		if !s.raftNode.IsLeader() {
			s.forwardToLeader(w, r)
			return
		}
		var body struct {
			ClusterPriority string `json:"cluster_priority"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON body: %v", err), http.StatusBadRequest)
			return
		}
		p, err := datacenter.ParseClusterPriority(body.ClusterPriority)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.monitor.SetClusterPriority(cluster, p); err != nil {
			http.Error(w, fmt.Sprintf("Failed to set cluster priority: %v", err), http.StatusInternalServerError)
			return
		}
		log.Printf("[API] Cluster %s priority set to %s", cluster, p)

		// 审计：集群优先级已变更。
		ae := s.newAudit(r, raft.AuditConfig, "config/priority.set", "success")
		ae.Cluster = cluster
		ae.Target = fmt.Sprintf("%v", p)
		s.raftNode.EmitAudit(ae)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"cluster_name":     cluster,
			"cluster_priority": p,
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleConfigNotify 暴露当前生效的告警通知配置（GET，其中密码/令牌会被
// 掩码处理），并发送一条测试通知（POST）。这两种方法都可由任意节点响应，
// 因为它只读取本地配置并由自身完成对外发送，因此永远不会代理转发给
// Leader。
func (s *Server) handleConfigNotify(w http.ResponseWriter, r *http.Request) {
	// PUT 用于持久化编辑后的通知配置（仅限 Leader；Follower 会被代理
	// 转发）。下方的 GET/POST 分别用于查看 / 测试。
	if r.Method == http.MethodPut {
		s.saveConfigNotify(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeNoStore(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(notify.EffectiveView())

	case http.MethodPost:
		// 请求体是可选的；channel 为空时会测试所有已启用的通知渠道。
		var body struct {
			Channel string `json:"channel"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		channel := strings.ToLower(strings.TrimSpace(body.Channel))
		if channel != "" {
			switch channel {
			case notify.ChannelEmail, notify.ChannelWeChat, notify.ChannelDingTalk, notify.ChannelFeishu:
			default:
				http.Error(w, "invalid channel (want email|wechat|dingtalk|feishu or empty)", http.StatusBadRequest)
				return
			}
		}

		ev := &notify.Event{
			Title:    "go-helix 测试通知",
			Severity: notify.SeverityInfo,
			Message: fmt.Sprintf("这是来自 go-helix Manager（节点 %s）的测试通知，时间 %s。收到即说明该通知渠道配置正确。",
				s.raftNode.RaftID, time.Now().Format("2006-01-02 15:04:05")),
		}

		ctx, cancel := context.WithTimeout(r.Context(), 18*time.Second)
		defer cancel()
		results, err := notify.Dispatch(ctx, ev, channel)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeNoStore(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"results": results})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// notifySaveRequest 是 PUT 请求的请求体。涉密字段（邮箱密码、Webhook
// URL/签名密钥）使用 *string 类型，以便控制台可以发送"保持不变"：nil
// 表示保留当前生效的密钥（GET 会将这些值掩码，因此若回发掩码值反而会
// 破坏它们），非 nil 值则替换原密钥（空字符串表示清空）。所有非涉密字段
// 都会由 GET 原样返回并回发。
type notifySaveRequest struct {
	Enabled  bool              `json:"enabled"`
	Email    notifyEmailSave   `json:"email"`
	WeChat   notifyWebhookSave `json:"wechat"`
	DingTalk notifyWebhookSave `json:"dingtalk"`
	Feishu   notifyWebhookSave `json:"feishu"`
}

type notifyEmailSave struct {
	Enabled  bool     `json:"enabled"`
	Host     string   `json:"host"`
	Port     int      `json:"port"`
	Username string   `json:"username"`
	Password *string  `json:"password"`
	From     string   `json:"from"`
	To       []string `json:"to"`
	UseTLS   bool     `json:"use_tls"`
}

type notifyWebhookSave struct {
	Enabled bool    `json:"enabled"`
	URL     *string `json:"url"`
	Secret  *string `json:"secret"`
}

// saveConfigNotify 将提交的通知配置合并到当前生效的配置之上（保留未变更的
// 密钥），通过 Raft 提交以使其在整个集群范围内生效且持久保存，并返回新的
// 掩码视图。该方法必须在 Leader 上执行；Follower 会把 PUT 请求代理转发
// 给它。
func (s *Server) saveConfigNotify(w http.ResponseWriter, r *http.Request) {
	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var req notifySaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 从当前生效的配置（Raft 覆盖或配置文件）开始处理，以保留现有密钥和
	// 其他通知渠道。
	cur := config.EffectiveNotify()
	cur.Enabled = req.Enabled

	// 邮件：覆盖所有非涉密字段；仅当控制台显式发送密码时才替换密码。
	cur.Email.Enabled = req.Email.Enabled
	cur.Email.Host = strings.TrimSpace(req.Email.Host)
	cur.Email.Port = req.Email.Port
	cur.Email.Username = strings.TrimSpace(req.Email.Username)
	cur.Email.From = strings.TrimSpace(req.Email.From)
	cur.Email.To = req.Email.To
	cur.Email.UseTLS = req.Email.UseTLS
	if req.Email.Password != nil {
		cur.Email.Password = *req.Email.Password
	}

	mergeWebhook := func(dst *config.WebhookNotifyConfig, src notifyWebhookSave) error {
		dst.Enabled = src.Enabled
		if src.URL != nil {
			u := strings.TrimSpace(*src.URL)
			if u != "" && !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
				return fmt.Errorf("webhook url must start with http:// or https://")
			}
			dst.URL = u
		}
		if src.Secret != nil {
			dst.Secret = *src.Secret
		}
		return nil
	}
	if err := mergeWebhook(&cur.WeChat, req.WeChat); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := mergeWebhook(&cur.DingTalk, req.DingTalk); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := mergeWebhook(&cur.Feishu, req.Feishu); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	payload, err := json.Marshal(cur)
	if err != nil {
		http.Error(w, fmt.Sprintf("Encode notify config failed: %v", err), http.StatusInternalServerError)
		return
	}
	if err := s.raftNode.PutKV(raft.KVNotifyConfigKey, string(payload)); err != nil {
		http.Error(w, fmt.Sprintf("Persist notify config failed: %v", err), http.StatusInternalServerError)
		return
	}
	log.Printf("[API] Notify config updated and committed (enabled=%v)", cur.Enabled)

	// 审计：通知配置已保存（GET 查看、POST 测试发送不审计）。
	ae := s.newAudit(r, raft.AuditConfig, "config/notify.save", "success")
	ae.Detail = fmt.Sprintf("enabled=%v", cur.Enabled)
	s.raftNode.EmitAudit(ae)

	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(notify.EffectiveView())
}

// handleClusterRegister 注册一个集群（准入操作；P0 Manager 集群在管理某个
// 集群之前必须先完成注册）。仅限 Leader；Follower 会被代理转发。
func (s *Server) handleClusterRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var meta datacenter.ClusterMeta
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON body: %v", err), http.StatusBadRequest)
		return
	}
	if meta.ClusterName == "" {
		http.Error(w, "cluster_name is required", http.StatusBadRequest)
		return
	}
	p, err := datacenter.ParseClusterPriority(string(meta.ClusterPriority))
	if err != nil {
		// 注册时若优先级为空，默认为 P3。
		if strings.TrimSpace(string(meta.ClusterPriority)) == "" {
			p = datacenter.PriorityP3
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	meta.ClusterPriority = p
	endpointType, err := datacenter.NormalizeEndpointType(meta.EndpointType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	meta.EndpointType = endpointType
	meta.Registered = true

	if err := s.monitor.RegisterCluster(meta); err != nil {
		http.Error(w, fmt.Sprintf("Failed to register cluster: %v", err), http.StatusInternalServerError)
		return
	}
	log.Printf("[API] Cluster %s registered (priority=%s endpoint=%s:%s)",
		meta.ClusterName, meta.ClusterPriority, meta.EndpointType, meta.EndpointValue)

	// 审计：集群注册（同一入口也用于设置接入端点）。
	ae := s.newAudit(r, raft.AuditCluster, "cluster/register", "success")
	ae.Cluster = meta.ClusterName
	ae.Target = string(meta.ClusterPriority)
	ae.Detail = fmt.Sprintf("endpoint=%s:%s", meta.EndpointType, meta.EndpointValue)
	s.raftNode.EmitAudit(ae)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}

// handleRaftPriority 返回本 Manager 集群的服务等级（tier）。该值在启动时即
// 固定（更改等级需要修改每个节点的配置并重启），因此为只读。
func (s *Server) handleRaftPriority(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cluster_priority": s.monitor.Tier(),
	})
}

// writeNoStore 将响应标记为不可缓存。切换日志内容和实时列表会在同一个 URL
// 上被反复轮询；若不设置此标记，浏览器或中间代理可能一直返回最初的快照，
// 从而导致实时视图看起来像冻结了一样，尽管磁盘上的文件仍在持续增长。
func writeNoStore(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Cache-Control", "no-store, no-cache, must-revalidate")
	h.Set("Pragma", "no-cache")
}

// requestSourceIP 从 RemoteAddr 中剥离端口，返回调用方 IP，用于审计留痕。
func requestSourceIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// newAudit 以当前 HTTP 请求构造一条审计条目（自动带上源 IP 与本节点 ID）；
// cluster / target / detail 由调用方按具体操作补充。
func (s *Server) newAudit(r *http.Request, category, action, result string) *raft.AuditEntry {
	return &raft.AuditEntry{
		Category: category,
		Action:   action,
		Result:   result,
		SourceIP: requestSourceIP(r),
		Node:     s.raftNode.RaftID,
	}
}

// logDir 返回已配置的 Manager 日志根目录（切换日志存放在其下）。
func (s *Server) logDir() string {
	if cfg := config.Get(); cfg != nil {
		return cfg.Manager.LogDir
	}
	return ""
}

// handleSwitchLogs 列出历史切换日志。可选查询参数：kind
// （failover/switchover）和 cluster。
func (s *Server) handleSwitchLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 切换日志由执行切换的节点（即 Leader）写入。Follower 的本地日志目录中
	// 并没有这些文件，因此从 Leader 读取，以保持单一一致的数据来源。
	if !s.raftNode.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	q := r.URL.Query()
	logs, err := switchlog.List(s.logDir(), q.Get("kind"), q.Get("cluster"))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list switch logs: %v", err), http.StatusInternalServerError)
		return
	}
	if logs == nil {
		logs = []*switchlog.Meta{}
	}

	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"logs":  logs,
		"count": len(logs),
	})
}

// handleSwitchLogContent 返回某个切换日志文件的原始内容。
func (s *Server) handleSwitchLogContent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	id := q.Get("id")
	if id == "" {
		http.Error(w, "id parameter is required", http.StatusBadRequest)
		return
	}

	// 聚合列表会为每条记录标注其来源节点。node 参数可将读取请求直接路由到
	// 该节点（已经过节点列表校验），从而使前任 Leader 产生的文件依然可以
	// 被获取。
	if node := q.Get("node"); node != "" {
		peers := s.peersConfig()
		if !validatePeerTarget(peers, node) {
			http.Error(w, "node is not a configured Raft peer", http.StatusBadRequest)
			return
		}
		if node != s.selfAPIAddr() {
			s.forwardToNode(node, w, r)
			return
		}
		// node == 本节点：继续向下执行本地读取。
	} else if !s.raftNode.IsLeader() {
		// 未显式指定 node：文件位于执行切换的 Leader 上。
		s.forwardToLeader(w, r)
		return
	}

	content, err := switchlog.Read(s.logDir(), id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read switch log: %v", err), http.StatusNotFound)
		return
	}

	// 使用纯文本，以便控制台可以原样渲染。
	writeNoStore(w)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(content)
}

// aggregateTimeout 限定整个扇出过程以及每个节点的抓取耗时上限。
const aggregateTimeout = 6 * time.Second

// handleAggregateSwitchLogs 并发地从每个已配置的 Raft 节点收集切换日志并
// 合并。切换日志是仅由执行切换的节点写入的本地文件，因此单个节点的目录
// 并非完整历史；该端点提供跨节点的完整列表。每条记录都带有其来源节点，
// 以便控制台可以直接读取其内容。
func (s *Server) handleAggregateSwitchLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	kind, cluster := q.Get("kind"), q.Get("cluster")
	peers := s.peersConfig()

	type nodeResult struct {
		source string
		logs   []*switchlog.Meta
		err    error
	}

	ctx, cancel := context.WithTimeout(r.Context(), aggregateTimeout)
	defer cancel()

	var wg sync.WaitGroup
	results := make(chan nodeResult, len(peers)+1)

	fetchHTTP := func(source string) {
		defer wg.Done()

		params := url.Values{}
		if kind != "" {
			params.Set("kind", kind)
		}
		if cluster != "" {
			params.Set("cluster", cluster)
		}
		reqURL := "http://" + source + "/switchlogs?" + params.Encode()

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			results <- nodeResult{source: source, err: err}
			return
		}
		if cfg := config.Get(); cfg != nil && cfg.AuthToken != "" {
			httpReq.Header.Set("Authorization", "Bearer "+cfg.AuthToken)
		}

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			results <- nodeResult{source: source, err: err}
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			results <- nodeResult{source: source, err: fmt.Errorf("HTTP %d", resp.StatusCode)}
			return
		}

		var payload struct {
			Logs []*switchlog.Meta `json:"logs"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			results <- nodeResult{source: source, err: err}
			return
		}
		results <- nodeResult{source: source, logs: payload.Logs}
	}

	for _, p := range peers {
		if p.APIAddr == "" {
			continue
		}
		wg.Add(1)
		if p.APIAddr == s.selfAPIAddr() {
			// 本地节点：直接读取其目录，省去一次 HTTP 往返。
			go func() {
				defer wg.Done()
				logs, err := switchlog.List(s.logDir(), kind, cluster)
				results <- nodeResult{source: p.APIAddr, logs: logs, err: err}
			}()
			continue
		}
		go fetchHTTP(p.APIAddr)
	}

	wg.Wait()
	close(results)

	var merged []*switchlog.Meta
	var unreachable []string
	for res := range results {
		if res.err != nil {
			log.Printf("[API] Aggregate: node %s unreachable: %v", res.source, res.err)
			unreachable = append(unreachable, res.source)
			continue
		}
		for _, m := range res.logs {
			m.Source = res.source
			merged = append(merged, m)
		}
	}

	// 与本地列表保持相同的排序：最新的在前。
	sort.SliceStable(merged, func(i, j int) bool {
		if !merged[i].StartTime.Equal(merged[j].StartTime) {
			return merged[i].StartTime.After(merged[j].StartTime)
		}
		if merged[i].ID != merged[j].ID {
			return merged[i].ID > merged[j].ID
		}
		return merged[i].Source > merged[j].Source
	})
	if merged == nil {
		merged = []*switchlog.Meta{}
	}

	writeNoStore(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"logs":        merged,
		"count":       len(merged),
		"unreachable": unreachable,
	})
}

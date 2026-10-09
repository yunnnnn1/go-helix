package http

import (
	"encoding/json"
	"net/http"
	"runtime"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/metrics"
)

// 指标处理器在默认注册表中只注册一次。Counter（计数器）由 monitor/notify
// 包递增；Gauge（仪表）在抓取时刷新。
var (
	metricUp          = metrics.Gauge("helix_up", "1 if the manager process is up")
	metricIsLeader    = metrics.Gauge("helix_is_leader", "1 if this node is the Raft leader")
	metricAppliedIdx  = metrics.Gauge("helix_raft_applied_index", "Highest applied Raft log index")
	metricClustersTot = metrics.Gauge("helix_clusters_total", "Number of managed clusters")
	metricGoroutines  = metrics.Gauge("helix_goroutines", "Number of running goroutines")
	metricUptime      = metrics.Gauge("helix_uptime_seconds", "Process uptime in seconds")
)

// handleHealth 上报存活状态：只要 HTTP 服务器在运行，就返回 200。
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleReady 上报本节点是否能够处理 API 流量。当存在 Raft Leader（本节点
// 自身，或一个已知、可供其代理的 Leader）时即为就绪；若尚无 Leader，则
// 返回 503，以便负载均衡器暂时摘除该节点。
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	isLeader := s.raftNode.IsLeader()
	leaderID := s.raftNode.LeaderID()
	ready := isLeader || leaderID != ""

	w.Header().Set("Content-Type", "application/json")
	if ready {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"ready":     ready,
		"is_leader": isLeader,
		"leader_id": leaderID,
	})
}

// metricsEnabled 解析可选的 manager.metrics_enabled 标志，未设置时默认为
// true。
func metricsEnabled() bool {
	if cfg := config.Get(); cfg != nil && cfg.Manager.MetricsEnabled != nil {
		return *cfg.Manager.MetricsEnabled
	}
	return true
}

// handleMetrics 输出 Prometheus 文本格式的指标数据。Gauge 在抓取时刷新；
// Counter 在事件发生处累加。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !metricsEnabled() {
		http.Error(w, "metrics disabled", http.StatusNotFound)
		return
	}

	// 刷新各个 Gauge。
	metricUp.Set(1)
	leader := int64(0)
	if s.raftNode.IsLeader() {
		leader = 1
	}
	metricIsLeader.Set(leader)
	metricAppliedIdx.Set(int64(s.raftNode.AppliedIndex()))
	metricGoroutines.Set(int64(runtime.NumGoroutine()))
	metricUptime.Set(int64(time.Since(s.startTime).Seconds()))

	clusters, err := s.monitor.GetClusters()
	if err == nil {
		metricClustersTot.Set(int64(len(clusters)))
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := metrics.Default.Gather(w); err != nil {
		http.Error(w, "gather metrics: "+err.Error(), http.StatusInternalServerError)
	}
}

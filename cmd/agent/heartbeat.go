package main

import (
	"context"
	"github.com/yunnnnn1/go-helix/pkg/agent/backend"
	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Heartbeat 已改为接收显式参数，以支持多实例。

func startHeartbeat(ctx context.Context, dbType, agentBind, host string, port int, clusterName, user, pass string, reportIP string) {
	log.Printf("[Agent] Starting Heartbeat Reporter for %s (DB:%d) Cluster:%s...", host, port, clusterName)

	interval := config.ParseDuration(config.Get().Agent.HeartbeatInterval, 5*time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 1. 根据数据库类型动态配置 Provider
	provider, err := backend.GetBackend(dbType)
	if err != nil {
		log.Printf("[Agent] Failed to get heartbeat provider: %v", err)
		return
	}

	// 2. 数据中心客户端
	var dbConfig *config.DatabaseConfig
	if cfg := config.Get(); cfg != nil {
		dbConfig = &cfg.Datacenter
	}
	dcClient := datacenter.NewClient(dbConfig)

	// 仅解析一次 Agent gRPC 端口 —— 在本实例的整个生命周期内它是固定的。
	_, portStr, err := net.SplitHostPort(agentBind)
	if err != nil {
		log.Printf("[Agent] Invalid agent bind address %q: %v, skipping heartbeat", agentBind, err)
		return
	}
	agentPort, _ := strconv.Atoi(portStr)
	if agentPort <= 0 {
		log.Printf("[Agent] Could not determine agent port from %q, skipping heartbeat", agentBind)
		return
	}

	// 解析本节点上报时使用的 IP（若调用方未设置则使用回退值）。
	if reportIP == "" {
		if h, _, splitErr := net.SplitHostPort(agentBind); splitErr == nil && h != "" && h != ":" && h != "0.0.0.0" {
			reportIP = h
		} else {
			reportIP = "127.0.0.1"
		}
	}
	log.Printf("[Agent] Heartbeat will report %s as agent port %d (report_ip=%s)", host, agentPort, reportIP)

	// 在真实环境中，需要找到数据库对应的正确挂载点。
	dbPath := "/"

	// reportOnce 执行一次：探测业务数据库 -> 读取实时状态（含 binlog 物理
	// 路径、GTID、复制位点、版本）-> 写入元数据库。抽成闭包是为了在启动时
	// 立即执行一次，不必等待第一个心跳周期，做到“初始化即带入”。
	reportOnce := func() {
		now := time.Now()

		// 是否处于重连退避窗口。退避窗口内不再发起 Ping 握手，避免
		// 对不可达 DB 每个周期产生一次失败握手、累计触发 MySQL Error
		// 1129。长连接池保持不动，窗口过后下一次 Ping 即自动重连。
		// 首次启动恒为 false。
		inBackoff := false
		if bo, ok := provider.(backend.ReconnectBackoff); ok {
			inBackoff = !bo.ShouldReconnect(now)
		}

		// 1. Ping 数据库（复用持久连接池；退避窗口内跳过握手）
		alive := false
		if !inBackoff {
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			alive, _ = provider.Ping(pingCtx)
			pingCancel()
		}

		// 2. 获取机器指标
		diskPct := getDiskUsage(dbPath)
		sysLoad := getSystemLoad()

		// 3. DB 探活失败时确保连接可用。Configure 复用现有长连接池
		// （不会因瞬时错误 Close/重建）；失败则进入指数退避。
		if !alive && !inBackoff {
			if err := provider.Configure(host, port, user, pass); err != nil {
				if bo, ok := provider.(backend.ReconnectBackoff); ok {
					d := bo.NoteConnectFailure()
					log.Printf("[Agent] %s:%d probe reconnect failed, backing off %s: %v", host, port, d, err)
				} else {
					log.Printf("[Agent] %s:%d probe reconnect failed: %v", host, port, err)
				}
			}
		}

		// 4. 实际从业务数据库读取实时状态：log_bin_basename（binlog 物理
		// 路径前缀）、gtid_executed、binlog_file/binlog_pos、版本等。
		version := ""
		status, gErr := provider.GetStatus(ctx)
		if gErr == nil {
			version = status["version"]
		} else {
			log.Printf("[Agent] 初始化读取业务数据库状态失败（%s:%d）：%v", host, port, gErr)
		}

		// 角色从中性的复制边键推导而来：有上游的节点是 slave，
		// 没有上游的节点是 master。
		role := "master"
		if status["upstream_host"] != "" {
			role = "slave"
		}

		// 构建数据库中立的副本状态（强类型的公共字段 + 特定类型的
		// Extra），用于上报。
		replicaStatus := buildReplicaStatus(status, int32(agentPort))

		// 5. 立即写入元数据库（binlog 物理路径随 Extra 一并持久化）。
		if err := dcClient.RegisterOrUpdateHeartbeat(reportIP, port, dbType, clusterName, role, version, diskPct, sysLoad, replicaStatus); err != nil {
			log.Printf("[Agent] Failed to update heartbeat: %v", err)
		}
	}

	// 初始化即执行一次：启动当下就从业务数据库取出 binlog 信息并写入元数据库。
	log.Printf("[Agent] Reporting initial binlog state to metadata DB immediately...")
	reportOnce()

	for {
		select {
		case <-ctx.Done():
			log.Println("[Agent] Stopping heartbeat reporter...")
			return
		case <-ticker.C:
			reportOnce()
		}
	}
}

// getDiskUsage 返回给定路径的磁盘使用百分比
func getDiskUsage(path string) float64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}

	all := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)
	if all == 0 {
		return 0
	}

	used := all - free
	usagePct := (float64(used) / float64(all)) * 100.0
	return usagePct
}

// getSystemLoad 使用原生的操作系统/系统信息获取 1 分钟平均负载
func getSystemLoad() float64 {
	// 这里不通过 shell 执行 'uptime'，因为那样可能依赖具体操作系统，且处理
	// 不当会带来潜在的注入风险；我们使用原生的 Go 方式获取系统负载。由于 Go
	// 没有直接的跨平台内置接口，可以在 Linux 上安全地解析 /proc/loadavg，
	// 若不支持则直接返回 0。为了跨平台兼容，这里直接执行 `uptime`，不经过
	// shell 包装，因此是安全的。

	out, err := exec.Command("uptime").Output()
	if err != nil {
		return 0
	}

	// 示例输出：
	// MacOS:  14:32  up 10 days,  3:14, 2 users, load averages: 1.23 2.34 3.45
	// Linux:  14:32:00 up 10 days,  3:14,  2 users,  load average: 1.23, 2.34, 3.45
	s := string(out)

	// 简单的手工解析，以避免使用 awk/sed 等 shell 命令
	idx := strings.LastIndex(s, "load average")
	if idx == -1 {
		idx = strings.LastIndex(s, "load averages")
	}
	if idx != -1 {
		parts := strings.Split(s[idx:], ":")
		if len(parts) >= 2 {
			loads := strings.Split(strings.TrimSpace(parts[1]), ",")
			if len(loads) > 0 {
				loads = strings.Fields(loads[0])
				if len(loads) > 0 {
					val, err := strconv.ParseFloat(loads[0], 64)
					if err == nil {
						return val
					}
				}
			}
		}
	}
	return 0
}

// commonReplicaStatusKeys 是会被读取到强类型、数据库中立的 ReplicaStatus
// 字段中的状态条目。其他所有上报的键都会原样复制到 Extra 中，因此本心跳
// 代码保持中立，绝不会罗列某种特定数据库类型的键（GTID/binlog/LSN/...）。
var commonReplicaStatusKeys = map[string]bool{
	"upstream_host":           true,
	"upstream_port":           true,
	"receiver_running":        true,
	"applier_running":         true,
	"replication_lag_seconds": true,
	"semi_sync_enabled":       true,
	"semi_sync_master":        true,
	"version":                 true,
	"read_only":               true,
}

// buildReplicaStatus 将 Agent 后端的原始状态 map 转换为数据库中立的
// ReplicaStatus：通用的复制概念成为强类型字段，所有特定于类型的内容则放入
// Extra。
func buildReplicaStatus(status map[string]string, agentPort int32) *datacenter.ReplicaStatus {
	if status == nil {
		return nil
	}

	rs := &datacenter.ReplicaStatus{
		AgentPort:       agentPort,
		UpstreamHost:    status["upstream_host"],
		Extra:           map[string]string{},
		ReceiverRunning: status["receiver_running"] == "true",
		ApplierRunning:  status["applier_running"] == "true",
	}

	if rs.UpstreamHost != "" {
		if v, err := strconv.ParseInt(status["upstream_port"], 10, 32); err == nil {
			rs.UpstreamPort = int32(v)
		}
	}

	// 同步 / 半同步保护：优先使用规范化的键，回退使用旧版输出的键。
	if v, ok := status["semi_sync_enabled"]; ok {
		rs.SyncReplication = v == "true"
	} else {
		rs.SyncReplication = status["semi_sync_master"] == "true"
	}

	if v, err := strconv.ParseInt(status["replication_lag_seconds"], 10, 64); err == nil {
		rs.LagSeconds = v
	}

	// 每一个非公共键都是特定于类型的 -> Extra（MySQL 的 GTID/binlog，
	// PostgreSQL 的 LSN/slot，...）。
	for k, v := range status {
		if !commonReplicaStatusKeys[k] {
			rs.Extra[k] = v
		}
	}
	return rs
}

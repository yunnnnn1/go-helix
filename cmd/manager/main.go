// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	_ "github.com/yunnnnn1/go-helix/pkg/db/drivers/mysql"  // 注册 MySQL 驱动与方言
	_ "github.com/yunnnnn1/go-helix/pkg/db/drivers/sqlite" // 注册 SQLite 驱动与方言
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
	managerapi "github.com/yunnnnn1/go-helix/pkg/manager/http"
	"github.com/yunnnnn1/go-helix/pkg/manager/monitor"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

func main() {
	configPath := flag.String("config", "config_manager.json", "Path to configuration file")
	flag.Parse()

	// 0. 加载配置
	if err := config.Load(*configPath); err != nil {
		log.Fatalf("Fatal: Failed to load config from %s: %v", *configPath, err)
	}
	config.Watch(5 * time.Second)

	cfg := config.Get()
	if cfg == nil {
		log.Fatalf("Fatal: Configuration is nil")
	}

	// 初始化结构化日志
	log.Init(cfg.Manager.LogDir, "manager")

	// 1. 初始化数据中心客户端
	dcClient := datacenter.NewClient(&cfg.Datacenter)

	// 从配置中确定各地址，并严格校验
	raftBindAddr := cfg.Manager.RaftAddr
	if raftBindAddr == "" {
		log.Fatalf("Fatal: Manager RaftAddr is not configured")
	}

	apiAddr := cfg.Manager.APIAddr
	if apiAddr == "" {
		log.Fatalf("Fatal: Manager APIAddr is not configured")
	}

	// HTTP 监听地址：优先使用专用的 bind_addr（例如 "0.0.0.0:8081"），
	// 这样 API 可以在所有网络接口上暴露，而 api_addr 仍作为对外通告的地址，
	// 用于节点间转发。当未配置 bind_addr 时回退使用 api_addr。
	listenAddr := cfg.Manager.BindAddr
	if listenAddr == "" {
		log.Printf("bind_addr not configured, falling back to api_addr %s for listening", apiAddr)
		listenAddr = apiAddr
	}

	raftID := cfg.Manager.RaftID
	if raftID == "" {
		log.Fatalf("Fatal: Manager RaftID is not configured")
	}

	raftDir := cfg.Manager.RaftDir
	if raftDir == "" {
		log.Fatalf("Fatal: Manager RaftDir is not configured")
	}

	// 2. 启动 Raft 节点
	r, err := raft.NewNode(raftID, raftDir, raftBindAddr)
	if err != nil {
		log.Fatalf("Failed to create raft node: %v", err)
	}
	defer r.Shutdown()
	log.Printf("Raft node %s started on %s", raftID, raftBindAddr)

	// 将各集群的拓扑策略镜像到配置覆盖层。已应用的变更通过 Raft 日志回调
	// 到达每个节点；而已有条目（重启 / 快照恢复场景）则在启动时通过前缀扫描
	// 一次性加载，因为回调不会对历史条目重新回放。
	r.FSM.SetKVChangeObserver(func(key, value string) {
		if strings.HasPrefix(key, raft.KVTopologyPolicyPrefix) {
			cluster := strings.TrimPrefix(key, raft.KVTopologyPolicyPrefix)
			applyTopologyOverride(cluster, value)
			return
		}
		if key == raft.KVNotifyConfigKey {
			applyNotifyOverride(value)
		}
	})
	for k, v := range r.GetKVsByPrefix(raft.KVTopologyPolicyPrefix) {
		applyTopologyOverride(strings.TrimPrefix(k, raft.KVTopologyPolicyPrefix), v)
	}
	// 在启动时一次性加载集群范围的通知配置（在重启 / 快照恢复时，观察者
	// 不会对历史条目重新回放）。
	if v, ok := r.GetKV(raft.KVNotifyConfigKey); ok {
		applyNotifyOverride(v)
	}

	// 校验本 Manager 集群的所有节点采用一致的服务层级（Raft KV
	// manager/tier）。若不一致则启动失败，而不是运行一个混合层级的集群。
	if err := enforceClusterTier(r); err != nil {
		log.Fatalf("Fatal: %v", err)
	}

	// 3. 启动 Manager 监控器
	m := monitor.NewMonitor(r, dcClient)
	m.Start()
	defer m.Stop()

	// 将监控器的直连/主机/对等节点探测接入故障切换层，使其能够带外验证
	// 不可达的旧 master（通过 FenceProbers 接口打破 logic<->monitor 之间的
	// 导入循环）。
	logic.SetFenceProbers(monitor.NewFenceProbers(m))

	// 4. 启动 HTTP API 服务器
	apiServer := managerapi.NewServer(m, r)
	if cfg.Manager.TLS.Enabled {
		if err := apiServer.SetTLS(cfg.Manager.TLS); err != nil {
			log.Fatalf("Fatal: configure API TLS: %v", err)
		}
	}
	go func() {
		log.Printf("API server listening on %s (advertised as %s)", listenAddr, apiAddr)
		if err := apiServer.Start(listenAddr); err != nil {
			log.Printf("Fatal: API server failed: %v", err)
			// 发送信号以触发优雅关闭
			p, _ := os.FindProcess(os.Getpid())
			p.Signal(syscall.SIGTERM)
		}
	}()

	// 5. 自动加入 / 引导 —— 无需提供 --join 或 --bootstrap 命令行参数。
	// 节点通过配置的 raft_peers 加入已有集群；固定种子节点（raft_peers[0]）
	// 在尚不存在集群时进行引导。
	self := config.RaftPeerConfig{
		NodeID:   raftID,
		RaftAddr: raftBindAddr,
		APIAddr:  apiAddr,
	}
	go autoReconcile(r, self, cfg.Manager.RaftPeers, cfg.AuthToken)

	// 等待信号
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("Shutting down manager...")

	// 优雅停止 API 服务器，使在途请求得以完成。
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("API server shutdown error: %v", err)
	}
}

// topologyPolicyJSON 是 API 与 Raft KV 之间共享的线上传输格式。
type topologyPolicyJSON struct {
	DetachLostReplicas         bool `json:"detach_lost_replicas"`
	PreserveHierarchy          bool `json:"preserve_hierarchy"`
	IntermediateMasterRecovery bool `json:"intermediate_master_recovery"`
}

// applyTopologyOverride 解码某个集群的、存储在 Raft 中的策略 JSON，并将其
// 安装为该集群的运行时覆盖项。空值会清除覆盖项；格式错误的条目会被记录
// 并忽略，以避免错误值导致控制面缺少可用策略。
func applyTopologyOverride(cluster, value string) {
	if strings.TrimSpace(value) == "" {
		config.SetTopologyOverride(cluster, nil)
		log.Printf("Topology policy: override cleared for cluster %s", cluster)
		return
	}

	var p topologyPolicyJSON
	if err := json.Unmarshal([]byte(value), &p); err != nil {
		log.Printf("Topology policy: ignored unparsable override for %s: %v", cluster, err)
		return
	}

	detach, preserve, im := p.DetachLostReplicas, p.PreserveHierarchy, p.IntermediateMasterRecovery
	config.SetTopologyOverride(cluster, &config.TopologyConfig{
		DetachLostReplicas:   &detach,
		PreserveHierarchy:    &preserve,
		IntermediateRecovery: &im,
	})
	log.Printf("Topology policy: runtime override applied to cluster %s: %s", cluster, value)
}

// applyNotifyOverride 解码存储在 Raft 中的通知配置，并将其安装为运行时
// 覆盖项（其优先级高于配置文件中的 notify 段）。空值会清除覆盖项；格式
// 错误的条目会被记录并忽略，以避免错误值导致所有通知静默或失效。
func applyNotifyOverride(value string) {
	if strings.TrimSpace(value) == "" {
		config.SetNotifyOverride(nil)
		log.Printf("Notify config: override cleared, using config-file notify section")
		return
	}

	var n config.NotifyConfig
	if err := json.Unmarshal([]byte(value), &n); err != nil {
		log.Printf("Notify config: ignored unparsable override: %v", err)
		return
	}
	config.SetNotifyOverride(&n)
	log.Printf("Notify config: runtime override applied (enabled=%v)", n.Enabled)
}

// enforceClusterTier 确保所有节点运行相同的、在启动时确定的服务层级。第一个
// 能够成为 Leader 的节点会将其层级发布到 Raft KV；一旦发布，任何配置了不同
// 层级的节点都会快速失败。
func enforceClusterTier(r *raft.Node) error {
	tier, err := config.ManagerTier()
	if err != nil {
		return err
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		if existing, ok := r.GetKV(raft.KVManagerTierKey); ok && existing != "" {
			if existing != tier {
				return fmt.Errorf("manager.tier mismatch: local config=%q but cluster already established tier=%q — align manager.tier on every node and restart",
					tier, existing)
			}
			log.Printf("Manager cluster tier confirmed: %s", tier)
			return nil
		}

		// 层级尚未发布：由 Leader 进行发布。
		if r.IsLeader() {
			if err := r.PutKV(raft.KVManagerTierKey, tier); err != nil {
				return fmt.Errorf("failed to publish manager tier: %w", err)
			}
			log.Printf("Manager cluster tier published: %s", tier)
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("manager cluster tier could not be established (no leader available within 20s)")
		}
		time.Sleep(time.Second)
	}
}

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

// joinURL 为 joinAddr 渲染 /join 端点，joinAddr 可带或不带 http(s)
// 协议前缀。
func joinURL(joinAddr string) string {
	if strings.HasPrefix(joinAddr, "http://") || strings.HasPrefix(joinAddr, "https://") {
		return strings.TrimRight(joinAddr, "/") + "/join"
	}
	return "http://" + joinAddr + "/join"
}

// newJoinRequest 构造携带本节点身份信息的 POST /join 请求。当配置了
// auth_token 时，集群的 /join 端点要求 Bearer 认证，因此必须在此处附带
// token —— 若遗漏会导致每次加入尝试都返回 401，节点永远无法加入。
func newJoinRequest(joinAddr, raftID, raftBindAddr, apiAddr, authToken string) (*http.Request, error) {
	body, err := json.Marshal(map[string]string{
		"node_id":   raftID,
		"raft_addr": raftBindAddr,
		"api_addr":  apiAddr,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal join body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, joinURL(joinAddr), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build join request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	return req, nil
}

// sendJoin 执行一次加入尝试，仅在 HTTP 200 时返回 nil。
func sendJoin(client *http.Client, req *http.Request) error {
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("join request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return fmt.Errorf("join returned status: %s", resp.Status)
}

// shouldBootstrap 报告本节点是否为固定的引导种子节点（RaftPeers[0]）。
// 只有种子节点可以创建全新集群；这可以防止多个同时启动的节点各自组建
// 自己的集群。
func shouldBootstrap(peers []config.RaftPeerConfig, selfID string) bool {
	if len(peers) == 0 || selfID == "" {
		return false
	}
	return peers[0].NodeID == selfID
}

// autoReconcile 确保节点在无需 join 或 bootstrap 命令行参数的情况下成为
// 集群的一部分。
//   - 已有成员（存在于磁盘上的 Raft 配置中）不做任何操作，直接从本地
//     状态恢复。
//   - 全新节点会不断尝试向其他所有已知对等节点 POST /join。
//   - 若没有可达的集群，固定种子节点会引导一个新集群；其他节点则等待
//     并不断重试，直到种子节点出现。
func autoReconcile(node *raft.Node, self config.RaftPeerConfig, peers []config.RaftPeerConfig, authToken string) {
	if node.IsConfigured() {
		log.Printf("[Raft] %s is already a cluster member; recovering from local state", self.NodeID)
		return
	}

	for {
		joined := false
		var lastErr error

		// 1. 尝试通过任一已知对等节点加入已经在运行的集群。
		for _, p := range peers {
			if p.NodeID == "" || p.NodeID == self.NodeID || p.APIAddr == "" {
				continue
			}
			req, err := newJoinRequest(p.APIAddr, self.NodeID, self.RaftAddr, self.APIAddr, authToken)
			if err != nil {
				lastErr = err
				continue
			}
			if err := sendJoin(http.DefaultClient, req); err != nil {
				lastErr = err
				continue
			}
			log.Printf("[Raft] %s joined cluster via %s", self.NodeID, p.NodeID)
			joined = true
			break
		}
		if joined {
			return
		}

		// 2. 没有可达的集群。只有固定种子节点可以初始化一个集群。
		if shouldBootstrap(peers, self.NodeID) {
			if err := node.Bootstrap(); err != nil {
				log.Printf("[Raft] %s bootstrap failed: %v; retrying", self.NodeID, err)
			} else {
				log.Printf("[Raft] %s bootstrapped a new single-node cluster", self.NodeID)
				return
			}
		} else {
			log.Printf("[Raft] %s waiting for the cluster or bootstrap seed (last error: %v)", self.NodeID, lastErr)
		}

		time.Sleep(2 * time.Second)
	}
}

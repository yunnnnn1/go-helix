package monitor

import (
	"fmt"
	"net"
	"strconv"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// AgentAddrForNode 依据节点的数据库地址解析其 Agent gRPC 地址。
// nodeAddr 可以是 "host"、"host:port" 或一个 IP。省略端口时，
// 仅按 host 匹配节点（必须唯一）。
func (m *Monitor) AgentAddrForNode(cluster, nodeAddr string) (string, *datacenter.Node, error) {
	host, portStr, err := net.SplitHostPort(nodeAddr)
	if err != nil {
		// 不带端口：将整个值视为 host。
		host, portStr = nodeAddr, ""
	}

	nodes, err := m.dcClient.GetClusterNodes(cluster)
	if err != nil {
		return "", nil, fmt.Errorf("get nodes for %s: %w", cluster, err)
	}

	var matched *datacenter.Node
	for _, n := range nodes {
		if n.Host != host {
			continue
		}
		if portStr != "" {
			p, perr := strconv.Atoi(portStr)
			if perr != nil || int(n.Port) != p {
				continue
			}
		}
		if matched != nil {
			return "", nil, fmt.Errorf("multiple nodes match %s; specify host:port", nodeAddr)
		}
		matched = n
	}

	if matched == nil {
		return "", nil, fmt.Errorf("node %s not found in cluster %s", nodeAddr, cluster)
	}

	agentPort, err := deriveAgentPort(matched)
	if err != nil {
		return "", nil, err
	}
	return net.JoinHostPort(matched.Host, strconv.Itoa(int(agentPort))), matched, nil
}

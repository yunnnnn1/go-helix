package mysql

import (
	"context"
	"strconv"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/db/ops"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/proto"
)

type MySQLProvider struct{}

func init() {
	ops.Register("mysql", &MySQLProvider{})
}

func (p *MySQLProvider) CheckHealth(ctx context.Context, node *datacenter.Node, agentClient proto.DBAgentClient) (bool, error) {
	pingResp, err := agentClient.Ping(ctx, &proto.PingRequest{})
	if err != nil {
		return false, err
	}
	if !pingResp.Alive {
		return false, nil
	}

	statusResp, err := agentClient.GetStatus(ctx, &proto.GetStatusRequest{})
	if err != nil {
		return false, err
	}
	st := statusResp.GetStatus()

	if node.Role == "slave" {
		receiverRunning, _ := strconv.ParseBool(st["receiver_running"])
		applierRunning, _ := strconv.ParseBool(st["applier_running"])
		if !receiverRunning || !applierRunning {
			log.Printf("[CheckHealth] %s:%d replication threads down (receiver=%v, applier=%v)", node.Host, node.Port, receiverRunning, applierRunning)
			return false, nil
		}

		if lagStr := st["Seconds_Behind_Master"]; lagStr != "" {
			if lag, err := strconv.ParseInt(lagStr, 10, 64); err == nil && lag > 3600 {
				log.Printf("[CheckHealth] %s:%d replication lag too high: %ds", node.Host, node.Port, lag)
				return false, nil
			}
		} else if lagStr := st["Seconds_Behind_Source"]; lagStr != "" {
			if lag, err := strconv.ParseInt(lagStr, 10, 64); err == nil && lag > 3600 {
				log.Printf("[CheckHealth] %s:%d replication lag too high: %ds", node.Host, node.Port, lag)
				return false, nil
			}
		}
	}

	log.Printf("[CheckHealth] %s:%d healthy (role=%s)", node.Host, node.Port, node.Role)
	return true, nil
}

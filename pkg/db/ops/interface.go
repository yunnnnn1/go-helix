package ops

import (
	"context"
	"fmt"
	"sync"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/proto"
)

// Provider 定义数据库特定逻辑的接口
type Provider interface {
	// CheckHealth 验证数据库是否健康
	CheckHealth(ctx context.Context, node *datacenter.Node, agentClient proto.DBAgentClient) (bool, error)
}

// Factory 管理已注册的 provider
type Factory struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

var GlobalFactory = &Factory{
	providers: make(map[string]Provider),
}

func Register(dbType string, provider Provider) {
	GlobalFactory.mu.Lock()
	defer GlobalFactory.mu.Unlock()
	GlobalFactory.providers[dbType] = provider
}

func GetProvider(dbType string) (Provider, error) {
	GlobalFactory.mu.RLock()
	defer GlobalFactory.mu.RUnlock()
	provider, ok := GlobalFactory.providers[dbType]
	if !ok {
		return nil, fmt.Errorf("provider not found for db type: %s", dbType)
	}
	return provider, nil
}

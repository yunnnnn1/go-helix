package logic

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// EndpointHookRequest 承载业务访问端点（VIP/域名）的切换
// 上下文。输入严格以命令行参数的形式传递给 hook。
type EndpointHookRequest struct {
	Cluster      string
	Command      string // status|stop|start
	EndpointType string // none|vip|domain
	Endpoint     string // VIP 或域名的值
	OldMaster    string // host:port
	NewMaster    string // host:port

	// ExecHost 选择执行 hook 的 agent（"host:port" 形式的 agent
	// 地址）；留空表示使用新主库所在主机。
	ExecHost string
	// SyncURL 可在运行前先同步最新的 hook。
	SyncURL string
	Sha256  string
	Timeout time.Duration
}

// EndpointHookResult 是 hook 的结构化执行结果。
type EndpointHookResult struct {
	Success       bool
	Message       string
	CurrentMaster string // host:port，端点实际指向的地址
}

// EndpointSwitcher 抽象 VIP/域名端点切换。故障切换
// 只依赖该接口，因此可以在不改动状态机的前提下新增具体实现
// （外部可执行 hook，以及后续的内部 DNS/SLB API）。
type EndpointSwitcher interface {
	// RunHook 在目标 agent 上执行一条 hook 命令。
	RunHook(ctx context.Context, req *EndpointHookRequest) (*EndpointHookResult, error)
}

// --- 注册表 ---

var (
	endpointRegistryMu sync.RWMutex
	endpointRegistry   = map[string]EndpointSwitcher{}
)

// RegisterEndpointSwitcher 为某一数据库类型注册 EndpointSwitcher。
func RegisterEndpointSwitcher(dbType string, p EndpointSwitcher) {
	endpointRegistryMu.Lock()
	defer endpointRegistryMu.Unlock()
	endpointRegistry[dbType] = p
}

// GetEndpointSwitcher 解析某一数据库类型对应的 EndpointSwitcher。
func GetEndpointSwitcher(dbType string) (EndpointSwitcher, error) {
	endpointRegistryMu.RLock()
	defer endpointRegistryMu.RUnlock()
	p, ok := endpointRegistry[dbType]
	if !ok {
		return nil, fmt.Errorf("no endpoint switcher registered for db type %q", dbType)
	}
	return p, nil
}

// --- 通过 context 携带已存储的端点（cluster_meta -> hook） ---

// clusterEndpointCtxKey 承载从 cluster_meta 表读取的权威 VIP/
// 域名，从而保证交给 hook 的是已持久化/注册的同一份值，而不是
// 配置文件中的重复配置。
type clusterEndpointCtxKey struct{}

// WithClusterEndpoint 将已存储的端点（类型、取值）附加到 context 中。
func WithClusterEndpoint(ctx context.Context, endpointType, endpointValue string) context.Context {
	return context.WithValue(ctx, clusterEndpointCtxKey{}, [2]string{endpointType, endpointValue})
}

// ClusterEndpointFromContext 返回已存储的端点。不存在时 ok 为 false。
func ClusterEndpointFromContext(ctx context.Context) (endpointType, endpointValue string, ok bool) {
	v, ok := ctx.Value(clusterEndpointCtxKey{}).([2]string)
	if !ok {
		return "", "", false
	}
	return v[0], v[1], true
}

// HasRealEndpoint 判断端点类型/取值是否表示一个真实的
// VIP/域名（既不为空也不是 "none"）。
func HasRealEndpoint(endpointType, _ string) bool {
	return endpointType != "" && endpointType != "none"
}

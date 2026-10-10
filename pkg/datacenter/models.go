// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package datacenter

import (
	"fmt"
	"strings"
	"time"
)

// NodeStatus 表示集群中数据库节点的状态
type NodeStatus string

const (
	StatusPending NodeStatus = "PENDING" // 已插入但从未有 Agent 心跳注册
	StatusOnline  NodeStatus = "ONLINE"
	StatusOffline NodeStatus = "OFFLINE"
	StatusFailed  NodeStatus = "FAILED" // 由 DC 监控标记为失败
)

// Node 表示由 meta_data 表映射而来的通用数据库节点
type Node struct {
	ID          uint64    `json:"id"`
	ClusterName string    `json:"cluster_name"`
	Role        string    `json:"role"` // master/slave
	Host        string    `json:"ip"`   // 由 'ip' 映射
	Port        int32     `json:"port"`
	AgentPort   int32     `json:"agent_port"`  // Agent gRPC 端口
	MasterHost  string    `json:"master_host"` // 复制上游（仅从节点）
	MasterPort  int32     `json:"master_port"` // 复制上游端口
	DbType      string    `json:"dbtype"`
	DbVersion   string    `json:"db_version"`
	Zone        string    `json:"zone"`
	Region      string    `json:"region"`
	IDC         string    `json:"idc"`
	Vip         string    `json:"vip"`
	Priority    int       `json:"priority"`
	IsDeleted   bool      `json:"is_deleted"`
	Env         string    `json:"env"`
	CreateTime  time.Time `json:"create_time"`
	UpdateTime  time.Time `json:"update_time"`

	// 运行时状态（不在 DB 中，而是推导得出或用于逻辑处理）
	Status       NodeStatus `json:"status"`
	IsBad        bool       `json:"is_bad"`
	DiskUsagePct float64    `json:"disk_usage_pct"`
	Load         float64    `json:"load"`
	DC           string     `json:"dc"`

	// 复制状态（由 Agent 心跳上报）。GTID/binlog 字段由
	// MySQL 填充；receiver/applier 使用与数据库无关的命名。
	GtidExecuted string `json:"gtid_executed"`
	BinlogFile   string `json:"binlog_file"`
	BinlogPos    int64  `json:"binlog_pos"`
	// 本地 binlog 物理路径（心跳上报 @@log_bin_basename 解析）。
	BinlogDir      string `json:"binlog_dir"`
	BinlogBaseName string `json:"binlog_basename"`
	// MHA 风格尾部救援策略（运维配置，默认关闭）。
	RescueEnabled     bool     `json:"rescue_enabled"`
	RescueAccessRoots []string `json:"rescue_access_roots"`
	RescueMaxBytes    int64    `json:"rescue_max_bytes"`
	ReceiverRunning   bool     `json:"receiver_running"` // 变更流接收端（MySQL I/O 线程 / PG WAL receiver）
	ApplierRunning    bool     `json:"applier_running"`  // 变更应用端（MySQL SQL 线程 / PG recovery）
	SemiSyncEnabled   bool     `json:"semi_sync_enabled"`
	SecondsBehind     int64    `json:"seconds_behind_master"`
	UsingGTID         bool     `json:"using_gtid"`
}

// Addr 返回节点的数据库地址 host:port，用于日志。
func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

// ClusterPriority 是集群级优先级（P0..P3）。它不同于
// Node.Priority，后者是节点各自的选举权重。
type ClusterPriority string

const (
	PriorityP0 ClusterPriority = "P0"
	PriorityP1 ClusterPriority = "P1"
	PriorityP2 ClusterPriority = "P2"
	PriorityP3 ClusterPriority = "P3" // 默认
)

// ParseClusterPriority 对取值做规范化（去空白 + 转大写）并校验。
// 空值解析为默认优先级 P3。
func ParseClusterPriority(s string) (ClusterPriority, error) {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return PriorityP3, nil
	}
	p := ClusterPriority(strings.ToUpper(raw))
	if !p.Valid() {
		return "", fmt.Errorf("invalid cluster priority %q (want one of P0..P3)", s)
	}
	return p, nil
}

func (p ClusterPriority) Valid() bool {
	switch p {
	case PriorityP0, PriorityP1, PriorityP2, PriorityP3:
		return true
	}
	return false
}

// Numeric 返回 0..3（P0..P3），用于比较/排序。
func (p ClusterPriority) Numeric() int {
	switch p {
	case PriorityP0:
		return 0
	case PriorityP1:
		return 1
	case PriorityP2:
		return 2
	}
	return 3
}

// 业务访问端点的端点类型取值。
const (
	EndpointTypeNone   = "none"
	EndpointTypeVIP    = "vip"
	EndpointTypeDomain = "domain"
)

// NormalizeEndpointType 校验端点类型（空值 → none）。
func NormalizeEndpointType(s string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	if t == "" {
		t = EndpointTypeNone
	}
	switch t {
	case EndpointTypeNone, EndpointTypeVIP, EndpointTypeDomain:
		return t, nil
	}
	return "", fmt.Errorf("invalid endpoint type %q (want none|vip|domain)", s)
}

// ClusterMeta 保存持久化在 cluster_meta 表中的集群级属性。
type ClusterMeta struct {
	ClusterName     string          `json:"cluster_name"`
	ClusterPriority ClusterPriority `json:"cluster_priority"`
	// Registered 在集群通过准入后为真（P0 的必要条件）。
	Registered    bool      `json:"registered"`
	EndpointType  string    `json:"endpoint_type"` // none|vip|domain
	EndpointValue string    `json:"endpoint_value"`
	Remark        string    `json:"remark"`
	CreateTime    time.Time `json:"create_time"`
	UpdateTime    time.Time `json:"update_time"`
}

// DefaultClusterMeta 返回集群没有 cluster_meta 记录时使用的隐式属性：
// P3、未注册、无端点。
func DefaultClusterMeta(cluster string) ClusterMeta {
	return ClusterMeta{
		ClusterName:     cluster,
		ClusterPriority: PriorityP3,
		Registered:      false,
		EndpointType:    EndpointTypeNone,
		EndpointValue:   "",
	}
}

// NodeBinlogRescue 是节点（host:port）的 binlog 尾部救援信息视图，
// 读取自节点表 meta_data（不再有独立表）：节点上报的物理路径
// binlog_dir/binlog_basename + 运维策略 rescue_enabled/access_roots/
// max_bytes。Manager 读取本视图并随故障切换 RPC 下发给对应 agent，
// agent 本地不再配置 binlog_rescue。
type NodeBinlogRescue struct {
	Host           string    `json:"host"`
	Port           int       `json:"port"`
	Enabled        bool      `json:"enabled"`
	BinlogDir      string    `json:"binlog_dir"`
	BinlogBaseName string    `json:"binlog_basename"`
	AccessRoots    []string  `json:"access_roots"`
	MaxBytes       int64     `json:"max_bytes"`
	UpdateTime     time.Time `json:"update_time"`
}

// DefaultNodeBinlogRescue 返回节点在 meta_data 中无对应行时的
// 隐式属性：默认关闭（enabled=false）。因此“无记录”即“禁用救援”，
// 符合失败即关闭（fail-closed）的默认安全语义。
func DefaultNodeBinlogRescue(host string, port int) NodeBinlogRescue {
	return NodeBinlogRescue{
		Host:        host,
		Port:        port,
		Enabled:     false,
		AccessRoots: []string{},
	}
}

// ClusterSummary 是一个受管数据库集群的聚合概览。
type ClusterSummary struct {
	ClusterName string `json:"cluster_name"`
	NodeCount   int    `json:"node_count"`
	MasterCount int    `json:"master_count"`
	SlaveCount  int    `json:"slave_count"`
	OnlineCount int    `json:"online_count"`
	// Healthy 在集群恰有一个主节点且每个节点均为 ONLINE 时为真。
	Healthy       bool      `json:"healthy"`
	LastHeartbeat time.Time `json:"last_heartbeat"`

	// 由 cluster_meta 合并而来的集群级属性（默认值：P3/none）。
	ClusterPriority ClusterPriority `json:"cluster_priority"`
	Registered      bool            `json:"registered"`
	EndpointType    string          `json:"endpoint_type"`
	EndpointValue   string          `json:"endpoint_value"`
}

// mergeClusterMeta 用 cluster_meta 属性丰富摘要内容；没有
// 记录的集群保留隐式默认值（P3 / 未注册 / none）。
func mergeClusterMeta(summaries []*ClusterSummary, metas map[string]ClusterMeta) {
	for _, s := range summaries {
		m, ok := metas[s.ClusterName]
		if !ok {
			m = DefaultClusterMeta(s.ClusterName)
		}
		s.ClusterPriority = m.ClusterPriority
		s.Registered = m.Registered
		s.EndpointType = m.EndpointType
		s.EndpointValue = m.EndpointValue
	}
}

// SwitchRecord 是 switch_history 表中的一条结构化记录。它记录
// 切换前/后的主节点身份，使回滚时无需
// 解析自由格式的日志文本。
type SwitchRecord struct {
	ID           uint64    `json:"id"`
	ClusterName  string    `json:"cluster_name"`
	Operation    string    `json:"operation"`     // failover / switchover / recover
	RecoverMode  string    `json:"recover_mode"`  // replica / topology / rollback（仅 recover）
	OldMaster    string    `json:"old_master"`    // 切换前的 host:port
	NewMaster    string    `json:"new_master"`    // 切换后的 host:port
	AnalysisCode string    `json:"analysis_code"` // AnalysisCode 以字符串表示（如有）
	Result       string    `json:"result"`        // SUCCESS / FAILED
	CreateTime   time.Time `json:"create_time"`
}

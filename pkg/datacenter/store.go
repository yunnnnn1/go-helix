package datacenter

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// MetaStore 是元数据存储的语义化持久化契约。
//
// 业务层（Client）只依赖该接口，不直接写 SQL、不持有具体连接。每种元数据
// 库类型（MySQL / SQLite）提供各自实现，自行消化方言差异。
//
// P1：接口先由 MySQL 与内存（降级）两种后端实现，对外行为与重构前一致。
type MetaStore interface {
	// Close 释放底层资源。
	Close() error

	// EnsureSchema 确保所需的表已存在（幂等）。基于文件的后端在此处建表；
	// MySQL 的 schema 由外部管理（空操作 no-op）。
	EnsureSchema(ctx context.Context) error

	// ResolveClusterName 完全依据复制边确定集群归属。仅在心跳未显式指定
	// 集群时调用：自身已知 → 加入上游 → 加入指向我的下游 →
	// 分配一个新的编号集群。
	ResolveClusterName(ctx context.Context, hb Heartbeat) (string, error)

	// UpsertHeartbeat 插入或更新一条节点心跳。
	UpsertHeartbeat(ctx context.Context, hb Heartbeat) error

	// GetClusterNodes 返回指定集群的全部存活节点。
	GetClusterNodes(ctx context.Context, cluster string) ([]*Node, error)

	// GetStaleNodes 返回指定角色（"" 表示任意角色）中最近一次心跳早于
	// olderThan 的节点。从未注册过的节点（PENDING）会被排除，以免尚未启动
	// 的种子行被误判为故障。
	GetStaleNodes(ctx context.Context, role string, olderThan time.Duration) ([]*Node, error)

	// ListClusters 返回所有受管集群的聚合概览。
	ListClusters(ctx context.Context) ([]*ClusterSummary, error)

	// GetClusterMeta 返回单个集群的集群级属性。
	// 记录缺失时返回默认值（P3/未注册/none），不报错。
	GetClusterMeta(ctx context.Context, cluster string) (ClusterMeta, error)

	// ListClusterMetas 返回所有拥有 cluster_meta 记录的集群属性。
	ListClusterMetas(ctx context.Context) (map[string]ClusterMeta, error)

	// SetClusterPriority 更新或写入优先级（新记录：未注册/none）。
	SetClusterPriority(ctx context.Context, cluster string, p ClusterPriority) error

	// RegisterCluster 更新或写入一条 registered=true 的 cluster_meta 记录。
	RegisterCluster(ctx context.Context, m ClusterMeta) error

	// GetNodeBinlogRescue 返回单个节点（host:port）的 binlog 尾部救援配置。
	// 记录缺失时返回默认值（enabled=false），不报错。
	GetNodeBinlogRescue(ctx context.Context, host string, port int) (NodeBinlogRescue, error)

	// UpsertNodeBinlogRescue 更新或写入节点级 binlog 救援配置。
	UpsertNodeBinlogRescue(ctx context.Context, n NodeBinlogRescue) error

	// InsertSwitchRecord 持久化一条切换/恢复记录。此处的失败
	// 不得阻塞运维操作路径。
	InsertSwitchRecord(ctx context.Context, rec *SwitchRecord) error

	// GetLatestSwitch 返回某集群最近一次成功且改变拓扑的切换
	// （recover(replica) 已排除）。
	GetLatestSwitch(ctx context.Context, cluster string) (*SwitchRecord, error)

	// InsertTopologySnapshot 在改变拓扑的切换前持久化完整复制边集合（JSON），
	// 以便回滚时精确恢复。
	InsertTopologySnapshot(ctx context.Context, cluster string, edgesJSON string) error

	// GetLatestTopologySnapshot 返回最近一次保存的边集合 JSON。
	GetLatestTopologySnapshot(ctx context.Context, cluster string) (string, error)
}

// Heartbeat 是节点心跳的输入 DTO。它收拢了此前
// RegisterOrUpdateHeartbeat 分散的参数以及 ReplicaStatus。
type Heartbeat struct {
	Host string // 节点 IP
	Port int    // 数据库端口

	DBType  string // 受管数据库类型（mysql/...）
	Cluster string // 已解析出的集群名；为空则由存储解析
	Role    string // 主节点 / 从节点

	Version string // 数据库版本

	DiskUsage float64 // 磁盘使用率百分比
	Load      float64 // 系统负载

	Replica *ReplicaStatus // 复制/运行时状态（可能为 nil）
}

// replica 返回复制状态的值拷贝（nil 安全）。此前的
// RegisterOrUpdateHeartbeat 在使用前进行值拷贝，此处保持该做法。
func (hb Heartbeat) replica() ReplicaStatus {
	if hb.Replica == nil {
		return ReplicaStatus{}
	}
	return *hb.Replica
}

// nodeSelectCols 是回读节点时使用的标准列清单。
const nodeSelectCols = `id, cluster_name, role, ip, port, agent_port, master_host, master_port,
	dbtype, db_version, zone, region, idc, vip, priority, is_deleted, env,
	create_time, update_time, status`

// switchRecordCols 是读取 switch_history 时使用的标准列清单。
const switchRecordCols = `id, cluster_name, operation, recover_mode, old_master, new_master, analysis_code, result, create_time`

// clusterNRe 匹配编号形式的集群名（如 "cluster12"）。读取时出于向后兼容，
// 也接受旧的带下划线形式（"cluster_12"），但新分配的名称不带下划线。
// 末尾编号采用全局单一序列，因此新自动发现的组绝不会复用
// 已被现有 clusterN 占用的编号。
var clusterNRe = regexp.MustCompile(`^cluster_?(\d+)$`)

func matchClusterN(name string) int {
	if m := clusterNRe.FindStringSubmatch(name); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil {
			return n
		}
	}
	return 0
}

func nodeKey(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

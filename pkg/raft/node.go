package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	log "github.com/yunnnnn1/go-helix/pkg/logging"
)

type Node struct {
	Raft     *raft.Raft
	FSM      *StoreFSM
	RaftDir  string
	RaftBind string
	RaftID   string

	// 各类存储
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	snapshots   *raft.FileSnapshotStore
	transport   *raft.NetworkTransport
}

func NewNode(raftID, raftDir, raftBind string) (*Node, error) {
	// 所有 Raft/hclog 的输出都经由这个行缓冲 writer 汇入项目日志，而不是直接
	// 写入 stderr。该 sink 会为每一行打上节点 ID 和 Raft 绑定地址（端口）标签。
	logSink := newLogSink(raftID, raftBind, nil)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(raftID)
	// 缩短超时时间，以便演示时更快完成故障切换
	config.HeartbeatTimeout = 1000 * time.Millisecond
	config.ElectionTimeout = 1000 * time.Millisecond
	config.LeaderLeaseTimeout = 500 * time.Millisecond

	// sink 已携带节点身份信息，因此将 hclog 名称留空，避免节点 ID 被打印两次。
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "",
		Level:  hclog.Info,
		Output: logSink,
	})
	config.Logger = logger

	addr, err := net.ResolveTCPAddr("tcp", raftBind)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(raftBind, addr, 3, 10*time.Second, logSink)
	if err != nil {
		return nil, err
	}

	// 创建快照存储，使 Raft 能够对日志进行截断。
	snapshots, err := raft.NewFileSnapshotStore(raftDir, 2, logSink)
	if err != nil {
		return nil, err
	}

	// 创建日志存储和稳定存储。
	// 两者均使用 BoltDB。
	if err := os.MkdirAll(raftDir, 0700); err != nil {
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}

	fsm := NewFSM()

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	return &Node{
		Raft:        r,
		FSM:         fsm,
		RaftDir:     raftDir,
		RaftBind:    raftBind,
		RaftID:      raftID,
		logStore:    logStore,
		stableStore: stableStore,
		snapshots:   snapshots,
		transport:   transport,
	}, nil
}

// Shutdown 停止 Raft 节点并释放相关资源
func (n *Node) Shutdown() error {
	// 关闭 Raft
	if n.Raft != nil {
		future := n.Raft.Shutdown()
		if err := future.Error(); err != nil {
			return err
		}
	}

	// 关闭各类存储
	if n.logStore != nil {
		n.logStore.Close()
	}
	if n.stableStore != nil {
		n.stableStore.Close()
	}
	if n.transport != nil {
		n.transport.Close()
	}

	return nil
}

func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

// LeaderAddr 返回当前 Leader 的地址
func (n *Node) LeaderAddr() string {
	addr, _ := n.Raft.LeaderWithID()
	return string(addr)
}

func (n *Node) LeaderCh() <-chan bool {
	return n.Raft.LeaderCh()
}

// Join 将一个新节点加入集群
func (n *Node) Join(nodeID, addr string) error {
	future := n.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

// Remove 将某个成员从集群配置中永久移除。一旦移除，Leader 会立即停止向其
// 发送心跳和日志条目。
func (n *Node) Remove(nodeID string) error {
	future := n.Raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
}

// LeaderID 返回当前 Leader 的服务器 ID（未知时返回 ""）。
func (n *Node) LeaderID() string {
	_, id := n.Raft.LeaderWithID()
	return string(id)
}

// AppliedIndex 返回已知已应用的最高 Raft 日志索引。
func (n *Node) AppliedIndex() uint64 {
	return n.Raft.AppliedIndex()
}

// Configuration 返回当前集群配置。
func (n *Node) Configuration() (raft.Configuration, error) {
	f := n.Raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return f.Configuration(), nil
}

// IsConfigured 报告本节点是否已存在于 Raft 配置中。已有成员重启时属于已配置
// 状态；全新节点（配置为空）则未配置，必须先加入集群或进行引导。
func (n *Node) IsConfigured() bool {
	cfg, err := n.Configuration()
	if err != nil {
		return false
	}
	for _, s := range cfg.Servers {
		if s.ID == raft.ServerID(n.RaftID) {
			return true
		}
	}
	return false
}

// Bootstrap 创建一个仅包含本节点的全新单节点集群。ErrCantBootstrap 被视为
// 成功：这意味着该节点已是成员（例如正在重启），因此无需再做初始化。
func (n *Node) Bootstrap() error {
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.RaftID),
				Address: raft.ServerAddress(n.RaftBind),
			},
		},
	}
	if err := n.Raft.BootstrapCluster(cfg).Error(); err != nil && err != raft.ErrCantBootstrap {
		return err
	}
	return nil
}

// KVTopologyPolicyPrefix 是 Raft KV 的键前缀，每个集群的运行时拓扑策略都存储
// 在其下。完整的键为 前缀+集群名。
const KVTopologyPolicyPrefix = "topology_policy/"

// TopologyPolicyKey 返回某个集群策略对应的 Raft KV 键。
func TopologyPolicyKey(cluster string) string {
	return KVTopologyPolicyPrefix + cluster
}

// KVManagerTierKey 存储本 Manager 集群在启动时即固定的服务层级，使所有节点
// 都能校验彼此是否以相同的层级进行配置。
const KVManagerTierKey = "manager/tier"

// KVNotifyConfigKey 存储集群范围的告警通知配置（一个序列化后的
// config.NotifyConfig）。控制台的编辑会提交到此处并在每个节点上应用，因此其
// 优先级高于各配置文件中的 notify 段落，并且能在重启 / Leader 变更后保留。
const KVNotifyConfigKey = "notify/config"

// GetKV 返回一个本地复制的 KV 值。一旦 Raft 应用了对应条目，该值在各节点间
// 保持一致。
func (n *Node) GetKV(key string) (string, bool) {
	return n.FSM.Get(key)
}

// GetKVsByPrefix 返回所有键以 prefix 开头的复制 KV 条目。用于在启动时加载
// 整套按集群划分的策略集合。
func (n *Node) GetKVsByPrefix(prefix string) map[string]string {
	return n.FSM.GetPrefix(prefix)
}

// PutKV 通过 Raft 提交一个 KV 条目。该方法必须在 Leader 上运行；调用方应检查
// IsLeader，并将非 Leader 请求转发给 Leader。应用过程受一个较短的超时限制，
// 以免停滞的集群导致请求挂起。
func (n *Node) PutKV(key, value string) error {
	if !n.IsLeader() {
		return fmt.Errorf("not raft leader")
	}
	data, err := json.Marshal(command{Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("encode kv command: %w", err)
	}
	future := n.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
	}
	if resp := future.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// AppendSwitch 通过 Raft 追加一条结构化切换摘要。必须在 Leader 上调用。
func (n *Node) AppendSwitch(r *SwitchRecord) error {
	if !n.IsLeader() {
		return fmt.Errorf("not raft leader")
	}
	data, err := json.Marshal(command{Type: cmdTypeSwitch, Switch: r})
	if err != nil {
		return fmt.Errorf("encode switch command: %w", err)
	}
	return n.applyTyped(data)
}

// AppendAudit 通过 Raft 追加一条审计日志。必须在 Leader 上调用。
func (n *Node) AppendAudit(a *AuditEntry) error {
	if !n.IsLeader() {
		return fmt.Errorf("not raft leader")
	}
	data, err := json.Marshal(command{Type: cmdTypeAudit, Audit: a})
	if err != nil {
		return fmt.Errorf("encode audit command: %w", err)
	}
	return n.applyTyped(data)
}

// applyTyped 提交已编码的类型化命令并校验应用结果。
func (n *Node) applyTyped(data []byte) error {
	future := n.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply: %w", err)
	}
	if resp := future.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// ListSwitches 返回本地复制的切换摘要（最新在前）。数据已随 Raft 复制到
// 各节点，因此任意节点都可直接读取、无需转发。
func (n *Node) ListSwitches(cluster, operation string, limit int) []SwitchRecord {
	return n.FSM.Switches(cluster, operation, limit)
}

// ListAudits 返回本地复制的审计日志（最新在前）。任意节点可读。
func (n *Node) ListAudits(category, cluster string, limit int) []AuditEntry {
	return n.FSM.Audits(category, cluster, limit)
}

// EmitAudit 在 Leader 上尽力提交一条审计日志：任何失败都只记录、不返回，
// 调用方可在业务主流程中直接调用，既无需处理错误，也不会影响主操作。
func (n *Node) EmitAudit(a *AuditEntry) {
	if a == nil {
		return
	}
	if !n.IsLeader() {
		// 审计统一由 Leader 提交。Follower 上的敏感 / 变更请求会先被转发
		// 给 Leader，因此到这里仍非 Leader 时直接忽略。
		return
	}
	if err := n.AppendAudit(a); err != nil {
		log.Printf("[audit] failed to commit audit action=%s cluster=%s: %v", a.Action, a.Cluster, err)
	}
}

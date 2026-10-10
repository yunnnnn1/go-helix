package config

import (
	"encoding/json"
	"fmt"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"os"
	"strings"
	"sync"
	"time"
)

// Config 是统一的配置结构。Manager 与 Agent 都反序列化进本结构
// （未使用的段落留零值），通过 Get() 在进程内共享。
type Config struct {
	Manager    ManagerConfig  `json:"manager"`
	Agent      AgentConfig    `json:"agent"`
	Datacenter DatabaseConfig `json:"datacenter"`
	Monitor    MonitorConfig  `json:"monitor"`
	Notify     NotifyConfig   `json:"notify"`
	AuthToken  string         `json:"auth_token"`
}

type ManagerConfig struct {
	LogDir       string `json:"log_dir"`
	BindAddr     string `json:"bind_addr"`
	RaftID       string `json:"raft_id"`
	RaftDir      string `json:"raft_dir"`
	RaftAddr     string `json:"raft_addr"`
	APIAddr      string `json:"api_addr"`
	SecondCheck  bool   `json:"second_check"`
	StaleTimeout string `json:"stale_timeout"` // 例如 "60s"
	CheckTimeout string `json:"check_timeout"` // 例如 "2s"

	// Fencing 控制是否允许在旧主库被确认隔离（读回 super_read_only 为 ON）
	// 之前提升新主库。
	Fencing FencingConfig `json:"fencing"`

	// TLS 配置 Manager 对外/对 Agent 的内部 gRPC 与 HTTP 双向加密
	// （默认关闭，向后兼容）。
	TLS TLSConfig `json:"tls"`

	// MetricsEnabled 控制 Prometheus /metrics 端点是否开启。
	MetricsEnabled *bool `json:"metrics_enabled"`

	// RaftPeers 是静态的期望成员列表（种子列表）。它在每个节点上都
	// 相同，用于：(1) 启动时无需 CLI 参数即可自动加入已有集群；
	// (2) 为全新集群选择固定的引导种子（RaftPeers[0]）；
	// (3) 解析 Leader 的 API 地址以进行转发。
	// 运行时的 Raft Configuration 仍是成员关系的唯一事实来源。
	RaftPeers []RaftPeerConfig `json:"raft_peers"`

	// 故障转移超时设置
	ElectionTimeout string `json:"election_timeout"` // 例如 "20s"，默认 20s
	CatchupTimeout  string `json:"catchup_timeout"`  // 例如 "30s"，默认 30s
	SilencePeriod   string `json:"silence_period"`   // 例如 "5m"，默认 5m

	// Topology 控制层级处理、中间主库恢复，以及故障转移期间如何
	// 处理不可达的副本。
	Topology TopologyConfig `json:"topology"`

	// EndpointSwitch 将集群映射到其业务接入端点（VIP/域名）的
	// 切换设置。没有对应条目的集群不会触发端点钩子。
	EndpointSwitch map[string]EndpointSwitchConfig `json:"endpoint_switch"`

	// Tier 是当前 Manager 集群的服务等级（P0..P3；默认 P3）。
	// P0 的 Manager 集群仅管理已注册的 P0 集群。该值在启动时固定：
	// 所有节点必须一致（通过 Raft KV manager/tier 校验）。
	Tier string `json:"tier"`
}

// EndpointSwitchConfig 配置单个集群的 VIP/域名切换。
type EndpointSwitchConfig struct {
	// EndpointType/EndpointValue 标识业务接入端点。
	EndpointType  string `json:"endpoint_type"`  // none|vip|domain
	EndpointValue string `json:"endpoint_value"` // VIP 或域名

	// ExecHost 选择运行钩子的 Agent（"host:port" 形式的 agent 地址）；
	// 为空表示使用新主库所在主机。
	ExecHost string `json:"exec_host"`
	// SyncURL：非空时，agent 会在运行前下载最新的钩子。
	SyncURL string `json:"sync_url"`
	// Sha256 用于可选地校验同步下来的钩子。
	Sha256 string `json:"sha256"`
	// Timeout 限制单次钩子调用（同步+执行）的时长；默认 15s。
	Timeout string `json:"timeout"`
	// Required 使钩子失败成为致命错误（失败即关闭，fail-closed）。P0 集群设为 true。
	Required bool `json:"required"`
}

// EndpointSwitchFor 返回指定集群的端点切换配置（当该集群没有条目时
// ok=false）。
func EndpointSwitchFor(cluster string) (EndpointSwitchConfig, bool) {
	cfg := Get()
	if cfg == nil || cfg.Manager.EndpointSwitch == nil {
		return EndpointSwitchConfig{}, false
	}
	c, ok := cfg.Manager.EndpointSwitch[cluster]
	return c, ok
}

// TopologyConfig 配置非平铺复制拓扑的行为。这三个开关在缺省时
// 默认为 TRUE，因此它们以 *bool 形式存储，并通过访问器方法读取。
type TopologyConfig struct {
	// DetachLostReplicas：为 true 时，无法触达或无法收敛的副本会
	// 被剥离，以便提升继续进行（优先 RTO）。为 false 时，除非所有
	// 副本都收敛，否则中止提升。
	DetachLostReplicas *bool `json:"detach_lost_replicas"`
	// PreserveHierarchy：为 true 时，主库故障转移只会重新指向死亡
	// 主库的直属副本；级联的子树保持原有形态。
	PreserveHierarchy *bool `json:"preserve_hierarchy"`
	// IntermediateRecovery 通过提升死亡中间主库的某个下游副本，
	// 来实现对该中间主库的恢复。
	IntermediateRecovery *bool `json:"intermediate_master_recovery"`
}

// Detach 返回剥离策略（无配置默认 true）：无法触达/收敛的副本会被
// 剥离以继续提升（优先 RTO）。经 EffectiveTopology 解析后读取。
func (t TopologyConfig) Detach() bool {
	if t.DetachLostReplicas == nil {
		return true
	}
	return *t.DetachLostReplicas
}

// Preserve 返回层级保持策略（无配置默认 true）：主库故障只重指向其
// 直属副本，级联子树保持原形。
func (t TopologyConfig) Preserve() bool {
	if t.PreserveHierarchy == nil {
		return true
	}
	return *t.PreserveHierarchy
}

// IntermediateRecoverEnabled 返回中间主恢复开关；指针为 nil（配置缺失）
// 时取默认值 true。经 EffectiveTopology 解析后读取。
func (t TopologyConfig) IntermediateRecoverEnabled() bool {
	if t.IntermediateRecovery == nil {
		return true
	}
	return *t.IntermediateRecovery
}

// EffectiveTopology 是单个集群拓扑策略的唯一权威解析入口。
// 优先级：按集群的运行时覆盖（通过 Raft 分发）> 配置文件中的
// manager.topology > 内置默认值（true）。每个在切换/恢复过程中
// 执行这些开关的调用方，都必须携带集群名通过本函数处理，绝不能
// 直接读取全局配置，以免绕过按集群设置的值。
func EffectiveTopology(cluster string) TopologyConfig {
	var fileTC TopologyConfig
	if cfg := Get(); cfg != nil {
		fileTC = cfg.Manager.Topology
	}

	ov := topologyOverrideValue(cluster)
	var od, op, oi *bool
	if ov != nil {
		od, op, oi = ov.DetachLostReplicas, ov.PreserveHierarchy, ov.IntermediateRecovery
	}
	return TopologyConfig{
		DetachLostReplicas:   mergeBoolPtr(od, fileTC.DetachLostReplicas),
		PreserveHierarchy:    mergeBoolPtr(op, fileTC.PreserveHierarchy),
		IntermediateRecovery: mergeBoolPtr(oi, fileTC.IntermediateRecovery),
	}
}

// mergeBoolPtr 在 override 已设置时返回一个指向它的独立指针，
// 否则返回 fallback（可能为 nil，稍后解析为内置默认值）。
func mergeBoolPtr(override, fallback *bool) *bool {
	if override != nil {
		v := *override
		return &v
	}
	if fallback == nil {
		return nil
	}
	v := *fallback
	return &v
}

// 按集群的运行时拓扑覆盖。当某个集群存在对应条目时（通过 Raft
// 分发），其优先级高于从配置文件加载的 topology 段；缺少条目
// 表示“使用文件值”。保存的值均为副本，调用方无法修改共享状态。
var (
	topologyOverridesMu sync.RWMutex
	topologyOverrides   = map[string]*TopologyConfig{}
)

// SetTopologyOverride 安装/清除单个集群的运行时拓扑策略。
// 传入 nil 会移除该集群的覆盖，使其恢复为配置文件中的值。
func SetTopologyOverride(cluster string, tc *TopologyConfig) {
	topologyOverridesMu.Lock()
	defer topologyOverridesMu.Unlock()
	if tc == nil {
		delete(topologyOverrides, cluster)
		return
	}
	topologyOverrides[cluster] = tc
}

func topologyOverrideValue(cluster string) *TopologyConfig {
	topologyOverridesMu.RLock()
	defer topologyOverridesMu.RUnlock()
	return topologyOverrides[cluster]
}

// AllTopologyOverrides 返回每个集群覆盖的独立副本（用于对集合
// 做快照/镜像）。没有覆盖的集群不会出现在结果中。
func AllTopologyOverrides() map[string]TopologyConfig {
	topologyOverridesMu.RLock()
	defer topologyOverridesMu.RUnlock()
	out := make(map[string]TopologyConfig, len(topologyOverrides))
	for k, v := range topologyOverrides {
		if v == nil {
			continue
		}
		out[k] = *v
	}
	return out
}

// 全局通知运行时覆盖。当存在条目时（通过 Raft 分发），它会完全
// 替换从配置文件加载的通知（notify）段；覆盖为 nil 表示“使用
// 文件值”。它对应于按集群拓扑覆盖的通知版本，使控制台可以在
// 全集群范围编辑告警，而无需在每个节点上重写配置文件。
var (
	notifyOverrideMu sync.RWMutex
	notifyOverride   *NotifyConfig
)

// SetNotifyOverride 安装/清除运行时通知配置。传入 nil 会移除
// 覆盖，使其恢复为配置文件中的值。
func SetNotifyOverride(n *NotifyConfig) {
	notifyOverrideMu.Lock()
	defer notifyOverrideMu.Unlock()
	notifyOverride = n
}

// EffectiveNotify 返回生效的通知配置：存在通过 Raft 分发的运行时
// 覆盖时使用该覆盖，否则使用配置文件中的通知段。所有通知投递都
// 必须经过本函数，而不是直接读取文件值，以免绕过控制台编辑
// （通过 Raft 复制）。
func EffectiveNotify() NotifyConfig {
	notifyOverrideMu.RLock()
	ov := notifyOverride
	notifyOverrideMu.RUnlock()
	if ov != nil {
		return *ov
	}
	if cfg := Get(); cfg != nil {
		return cfg.Notify
	}
	return NotifyConfig{}
}

// ManagerTier 返回当前 Manager 集群规范化后的服务等级（P0..P3）；
// 未设置时默认为 P3。值格式错误时返回错误，以便配置拼写错误能
// 在启动时暴露。
func ManagerTier() (string, error) {
	raw := strings.TrimSpace(Get().Manager.Tier)
	if raw == "" {
		return "P3", nil
	}
	t := strings.ToUpper(raw)
	switch t {
	case "P0", "P1", "P2", "P3":
		return t, nil
	}
	return "", fmt.Errorf("invalid manager.tier %q (want one of P0..P3)", raw)
}

// RaftPeerConfig 描述一个期望的 Raft 成员。
type RaftPeerConfig struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	APIAddr  string `json:"api_addr"`
}

type AgentConfig struct {
	LogDir            string `json:"log_dir"`
	Timeout           string `json:"timeout"`            // 例如 "10s"
	HeartbeatInterval string `json:"heartbeat_interval"` // 例如 "5s"，默认 5s

	// 探测账号（业务）：仅供 Agent 用于健康检查。
	DefaultDBUser     string `json:"default_db_user"`
	DefaultDBPassword string `json:"default_db_password"`

	// 复制账号：仅用于主从复制。Agent 绝不会用它建立客户端连接；
	// 其凭据只会被嵌入到 CHANGE REPLICATION SOURCE 语句中。
	ReplicationDBUser     string `json:"replication_db_user"`
	ReplicationDBPassword string `json:"replication_db_password"`

	// 管理账号：专门用于故障转移/切换以及 GTID 操作。
	ManagerDBUser     string `json:"manager_db_user"`
	ManagerDBPassword string `json:"manager_db_password"`

	// AutoDiscoverType 选择在未配置任何实例时要自动发现的数据库
	// 类型。为空表示 "mysql"。
	AutoDiscoverType string `json:"auto_discover_type"`

	// FileFetchRoots 列出 FetchFile 允许写入的目录（监禁目录）。
	// target_dir 必须解析到其中某个目录之内。为空则禁用拉取。
	FileFetchRoots []string `json:"file_fetch_roots"`

	// 插件连接池设置（由所有插件/实例共享）
	PluginPool PluginPoolConfig `json:"plugin_pool"`

	// EndpointHooks 配置集群端点（VIP/域名）的可执行程序。
	EndpointHooks EndpointHooksConfig `json:"endpoint_hooks"`

	// TLS 配置 Agent gRPC 服务端的双向加密（默认关闭）。
	TLS TLSConfig `json:"tls"`

	// 调试/覆盖模式：手动指定实例，而非使用自动发现
	Instances []AgentInstanceConfig `json:"instances"`
}

// EndpointHooksConfig 配置 VIP/域名切换的可执行程序。钩子路径
// 按集群从本地配置解析（绝不来自网络请求）。
type EndpointHooksConfig struct {
	// 运行钩子时使用的用户（降权）。该用户必须在主机上存在；
	// 为空则禁用钩子执行。
	RunUser string `json:"run_user"`
	// Roots 限制钩子可执行程序所在的目录（监禁目录）。
	Roots []string `json:"roots"`
	// 按集群的可执行程序绑定。
	Hooks []EndpointHookBinding `json:"hooks"`
}

// EndpointHookBinding 将一个集群映射到一个本地可执行程序。
type EndpointHookBinding struct {
	Cluster string `json:"cluster"`
	Path    string `json:"path"`
}

// Lookup 返回指定集群配置的钩子路径。
func (c EndpointHooksConfig) Lookup(cluster string) (string, bool) {
	for _, h := range c.Hooks {
		if h.Cluster == cluster {
			return h.Path, true
		}
	}
	return "", false
}

// PluginPoolConfig 控制插件使用的、按地址共享的数据库连接池。
type PluginPoolConfig struct {
	// 按目标的 database/sql 限制
	MaxOpenConns int    `json:"max_open_conns"` // 默认 10
	MaxIdleConns int    `json:"max_idle_conns"` // 默认 2
	IdleTime     string `json:"idle_time"`      // 物理空闲连接的最大存活时间，默认 "10m"

	// 条目（槽位）管理
	MaxEntries      int    `json:"max_entries"`      // 缓存 *sql.DB 的 LRU 容量上限，默认 64
	ReclaimInterval string `json:"reclaim_interval"` // 空闲槽位的回收频率，默认 "5m"
	ReclaimAfter    string `json:"reclaim_after"`    // 槽位空闲超过该时长即被淘汰，默认 "30m"
}

// DefaultPluginPoolConfig 返回已填好全部默认值的连接池配置。
func DefaultPluginPoolConfig() PluginPoolConfig {
	return PluginPoolConfig{
		MaxOpenConns:    10,
		MaxIdleConns:    2,
		IdleTime:        "10m",
		MaxEntries:      64,
		ReclaimInterval: "5m",
		ReclaimAfter:    "30m",
	}
}

// WithDefaults 用默认值填充连接池中零值的字段，并返回结果。
func (p PluginPoolConfig) WithDefaults() PluginPoolConfig {
	d := DefaultPluginPoolConfig()
	if p.MaxOpenConns <= 0 {
		p.MaxOpenConns = d.MaxOpenConns
	}
	if p.MaxIdleConns <= 0 {
		p.MaxIdleConns = d.MaxIdleConns
	}
	if p.IdleTime == "" {
		p.IdleTime = d.IdleTime
	}
	if p.MaxEntries <= 0 {
		p.MaxEntries = d.MaxEntries
	}
	if p.ReclaimInterval == "" {
		p.ReclaimInterval = d.ReclaimInterval
	}
	if p.ReclaimAfter == "" {
		p.ReclaimAfter = d.ReclaimAfter
	}
	return p
}

type AgentInstanceConfig struct {
	BindAddr    string `json:"bind_addr"`    // Agent gRPC 绑定地址（例如 "0.0.0.0:50051"）
	ReportIP    string `json:"report_ip"`    // 上报给 datacenter 的 IP
	ClusterName string `json:"cluster_name"` // 例如 "cluster1"

	DB struct {
		Type     string `json:"type"`
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`

		// 针对复制账号和管理账号的按实例覆盖。
		ReplicationUser     string `json:"replication_user"`
		ReplicationPassword string `json:"replication_password"`
		ManagerUser         string `json:"manager_user"`
		ManagerPassword     string `json:"manager_password"`
	} `json:"db"`
}

type DatabaseConfig struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
	Timeout  string `json:"timeout"`

	// Path 供基于文件的数据库（SQLite）使用。
	Path string `json:"path"`

	// 连接池设置
	MaxOpenConns    int    `json:"max_open_conns"`
	MaxIdleConns    int    `json:"max_idle_conns"`
	ConnMaxLifetime string `json:"conn_max_lifetime"` // 例如 "5m"、"1h"
}

type MonitorConfig struct {
	Interval       string `json:"interval"`         // 例如 "3s"
	WorkerPoolSize int    `json:"worker_pool_size"` // 用于调查故障的并发 worker 数量
}

// Fencing 模式常量。
const (
	// FencingModeRequired 在旧主库无法被确认隔离时中止提升
	// （避免脑裂：宁可无法写入，也不要双主）。
	FencingModeRequired = "required"
	// FencingModeBestEffort 保留旧有行为：当无法验证隔离时，在
	// 告警后仍然进行提升。
	FencingModeBestEffort = "best_effort"
)

// TLSConfig 配置内部 gRPC/HTTP 的双向 TLS（mTLS）。默认 Enabled=false
// （明文，向后兼容）。CertFile/KeyFile 为本端证书；CAFile 为校验对端
// 证书使用的根证书；启用 mTLS 时三者都需提供。
type TLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file"`
}

// Validate 校验启用 TLS 时证书文件字段完整。
func (t TLSConfig) Validate() error {
	if !t.Enabled {
		return nil
	}
	missing := ""
	switch {
	case t.CertFile == "":
		missing = "cert_file"
	case t.KeyFile == "":
		missing = "key_file"
	case t.CAFile == "":
		missing = "ca_file"
	}
	if missing != "" {
		return fmt.Errorf("tls enabled but %s is not configured", missing)
	}
	return nil
}

// FencingConfig 配置提升前的隔离门禁。
type FencingConfig struct {
	// Enabled 开启该门禁。nil（配置块缺失）表示安全默认值（启用）；
	// 显式指定 true/false 可将其覆盖。
	Enabled *bool `json:"enabled"`
	// Mode 取值为 required（启用时的默认值）或 best_effort。
	Mode string `json:"mode"`
	// Timeout 限制隔离加读回尝试的时长。
	Timeout string `json:"timeout"` // 例如 "3s"
}

// Normalize 校验模式并填充默认值。已启用但模式为空/非法的配置
// 会默认使用 required。
func (f FencingConfig) Normalize() (FencingConfig, error) {
	mode := strings.ToLower(strings.TrimSpace(f.Mode))
	if mode == "" {
		mode = FencingModeRequired
	}
	if mode != FencingModeRequired && mode != FencingModeBestEffort {
		return f, fmt.Errorf("invalid fencing mode %q (want required|best_effort)", f.Mode)
	}
	f.Mode = mode
	return f, nil
}

// EffectiveFencing 返回解析后的隔离配置。
//
// 默认情况（配置块缺失或 Enabled 为 nil）：启用且模式为 required，
// 即使没有显式配置也会采用安全行为。如需放宽，设置
// mode="best_effort"；如需完全禁用门禁，设置 enabled=false。
func EffectiveFencing() FencingConfig {
	var f FencingConfig
	if cfg := Get(); cfg != nil {
		f = cfg.Manager.Fencing
	}

	enabled := true
	out := FencingConfig{
		Enabled: &enabled,
		Mode:    FencingModeRequired,
		Timeout: "3s",
	}
	if f.Timeout != "" {
		out.Timeout = f.Timeout
	}
	if f.Enabled != nil {
		out.Enabled = f.Enabled
	}
	mode := strings.ToLower(strings.TrimSpace(f.Mode))
	if mode == "" {
		mode = FencingModeRequired
	}
	if mode != FencingModeRequired && mode != FencingModeBestEffort {
		// 模式非法：保留安全默认值，而不是在热路径上报错。
		return out
	}
	out.Mode = mode
	return out
}

// NotifyConfig 配置告警投递。全局 Enabled 门禁加上每个通道各自的
// Enabled 标志，使运维人员可以独立开关各通道。只有已启用且配置
// 完整的通道才会真正收到事件。
type NotifyConfig struct {
	// Enabled 是总开关；为 false 时不会向任何地方投递事件。
	Enabled bool `json:"enabled"`

	Email    EmailNotifyConfig   `json:"email"`    // SMTP 邮件
	WeChat   WebhookNotifyConfig `json:"wechat"`   // 企业微信群机器人
	DingTalk WebhookNotifyConfig `json:"dingtalk"` // 钉钉群机器人
	Feishu   WebhookNotifyConfig `json:"feishu"`   // 飞书群机器人
}

// EmailNotifyConfig 配置通过 SMTP 发送告警。
type EmailNotifyConfig struct {
	Enabled  bool     `json:"enabled"`
	Host     string   `json:"host"`     // SMTP 主机，如 smtp.example.com
	Port     int      `json:"port"`     // SMTP 端口，如 465 / 587 / 25
	Username string   `json:"username"` // 登录用户名（为空则不认证）
	Password string   `json:"password"` // 登录口令（不写日志）
	From     string   `json:"from"`     // 发件人地址
	To       []string `json:"to"`       // 收件人地址列表
	// UseTLS 强制使用隐式 TLS（通常为 465 端口）。为 false 时，
	// 客户端先以明文启动，并在服务器支持时通过 STARTTLS 升级
	// （通常为 587/25 端口）。
	UseTLS bool `json:"use_tls"`
}

// WebhookNotifyConfig 配置群机器人 webhook（企业微信 / 钉钉 /
// 飞书）。Secret 是钉钉和飞书机器人使用的可选加签密钥；企业
// 微信群机器人通过内嵌在 URL 中的 key 进行鉴权。
type WebhookNotifyConfig struct {
	Enabled bool   `json:"enabled"`
	URL     string `json:"url"`    // 完整 webhook 地址
	Secret  string `json:"secret"` // 加签密钥（钉钉/飞书，可选；不写日志）
}

var (
	currentConfig *Config
	mu            sync.RWMutex
	lastModTime   time.Time
	filePath      string
)

// Load 从文件初始化配置
func Load(path string) error {
	filePath = path
	// 首次强制加载
	return reload(true)
}

// Get 安全地返回当前配置
func Get() *Config {
	mu.RLock()
	defer mu.RUnlock()
	return currentConfig
}

func reload(force bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// 未修改则跳过，除非强制加载
	if !force && !stat.ModTime().After(lastModTime) {
		return nil
	}

	decoder := json.NewDecoder(file)
	cfg := &Config{}
	if err := decoder.Decode(cfg); err != nil {
		return err
	}

	mu.Lock()
	currentConfig = cfg
	lastModTime = stat.ModTime()
	mu.Unlock()

	log.Printf("Configuration loaded from %s", filePath)
	return nil
}

// Watch 启动后台 goroutine，在配置变化时重新加载
func Watch(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := reload(false); err != nil {
				log.Printf("Error reloading config: %v", err)
			}
		}
	}()
}

// ParseDuration 将 s 解析为 Go duration 字符串；当 s 为空或非法时返回 def。
func ParseDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

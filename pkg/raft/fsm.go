package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

type StoreFSM struct {
	mu    sync.Mutex
	store map[string]string

	// switches / audits 是两类只追加的复制集合；next*Seq 是其下一条记录
	// 将要使用的序号。序号由 FSM 在应用时分配，因此在 Raft 确定的相同
	// 日志顺序下于各节点上保持一致。
	switches      []SwitchRecord
	nextSwitchSeq int64
	audits        []AuditEntry
	nextAuditSeq  int64

	onKVChange KVChangeFunc
}

func NewFSM() *StoreFSM {
	return &StoreFSM{
		store: make(map[string]string),
	}
}

// Apply 将一条 Raft 日志条目应用到有限状态机。历史 KV 条目与新的类型化
// 条目（switch / audit）在此按命令类型分发。
func (f *StoreFSM) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		// 返回错误而不是 panic——损坏的日志条目不应导致节点崩溃
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch c.Type {
	case cmdTypeSwitch:
		return f.applySwitch(c.Switch)
	case cmdTypeAudit:
		return f.applyAudit(c.Audit)
	default: // cmdTypeKV（含历史上无 type 字段的条目）
		f.store[c.Key] = c.Value

		// 通知已注册的观察者（例如用于回放运行时配置覆盖项）。
		if f.onKVChange != nil {
			f.onKVChange(c.Key, c.Value)
		}
		return nil
	}
}

// Get 返回指定键对应的值，以及该键是否存在。
func (f *StoreFSM) Get(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.store[key]
	return v, ok
}

// GetPrefix 返回所有键以 prefix 开头的条目的副本。
func (f *StoreFSM) GetPrefix(prefix string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string)
	for k, v := range f.store {
		if strings.HasPrefix(k, prefix) {
			out[k] = v
		}
	}
	return out
}

// onKVChange 会在每个节点上、每条 KV 条目被应用之后调用（Raft 日志会被
// 复制），使上层可以镜像集群范围的值，而无需本包依赖这些上层模块。
type KVChangeFunc func(key, value string)

// SetKVChangeObserver 注册一个回调，用于接收已应用的 KV 变更。
func (f *StoreFSM) SetKVChangeObserver(cb KVChangeFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onKVChange = cb
}

// Snapshot 返回当前状态的一个快照，包含 KV 存储与两类追加集合。
func (f *StoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 深拷贝 KV map。
	o := make(map[string]string)
	for k, v := range f.store {
		o[k] = v
	}
	// 深拷贝两类切片，避免快照与后续的追加 / 截断共享底层数组。
	sw := append([]SwitchRecord(nil), f.switches...)
	au := append([]AuditEntry(nil), f.audits...)

	return &fsmSnapshot{data: fsmData{
		Version:       currentSnapshotVersion,
		Store:         o,
		Switches:      sw,
		NextSwitchSeq: f.nextSwitchSeq,
		Audits:        au,
		NextAuditSeq:  f.nextAuditSeq,
	}}, nil
}

// Restore 将状态恢复到先前快照的状态。它同时兼容旧版本“裸 KV map”格式
// 与当前版本化信封格式。
func (f *StoreFSM) Restore(rc io.ReadCloser) error {
	raw, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	// 先用一层通用对象探测快照是否带 version 字段；据此区分新旧格式。
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	if _, ok := probe["version"]; ok {
		var d fsmData
		if err := json.Unmarshal(raw, &d); err != nil {
			return fmt.Errorf("failed to decode versioned snapshot: %w", err)
		}
		f.store = d.Store
		f.switches = d.Switches
		f.nextSwitchSeq = d.NextSwitchSeq
		f.audits = d.Audits
		f.nextAuditSeq = d.NextAuditSeq
		if f.store == nil {
			f.store = make(map[string]string)
		}
		return nil
	}

	// 旧格式：整个快照就是一个 map[string]string。
	o := make(map[string]string)
	if err := json.Unmarshal(raw, &o); err != nil {
		return fmt.Errorf("failed to decode legacy kv snapshot: %w", err)
	}
	f.store = o
	// 恢复旧快照不会引入追加集合；保持其为空、序号归零。
	f.switches, f.audits = nil, nil
	f.nextSwitchSeq, f.nextAuditSeq = 0, 0
	return nil
}

type fsmSnapshot struct {
	data fsmData
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// 编码版本化快照。
		b, err := json.Marshal(f.data)
		if err != nil {
			return err
		}

		// 将数据写入 sink。
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// 关闭 sink。
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

type command struct {
	Type string `json:"type,omitempty"`

	// KV 命令字段。
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`

	// 类型化命令载荷。
	Switch *SwitchRecord `json:"switch,omitempty"`
	Audit  *AuditEntry   `json:"audit,omitempty"`
}

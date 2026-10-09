// Package switchlog 提供对 Manager 写入 {logDir}/failover 和
// {logDir}/switchover 的切换日志文件的只读访问。它为控制台的"当前切换"
// 实时视图以及历史切换日志列表提供支撑。
package switchlog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	KindFailover   = "failover"
	KindSwitchover = "switchover"
	KindReplica    = "replica"
	KindTopology   = "topology"
	KindRollback   = "rollback"
	KindRecover    = "recover" // 历史遗留的合并恢复目录，仍可读取
)

// listableKinds 是在未指定具体 kind 时扫描的集合。其中包含历史遗留的
// recover 目录，以便旧的恢复日志依然可见。
var listableKinds = []string{
	KindFailover, KindSwitchover,
	KindReplica, KindTopology, KindRollback,
	KindRecover,
}

// Meta 描述一个切换日志文件，但不包含其内容。
type Meta struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Cluster   string    `json:"cluster"`
	File      string    `json:"file"`
	Date      string    `json:"date"`
	Size      int64     `json:"size"`
	StartTime time.Time `json:"start_time"`
	// Source 是本地目录中存放该文件的 Manager 节点的 API 地址
	// （host:port）。本地（非聚合）列表中为空。
	Source string `json:"source"`
}

// parseFilename 拆分形如 "cluster1_20261004.log" 的文件名。
func parseFilename(file string) (cluster, date string, ok bool) {
	base := filepath.Base(file)
	if !strings.HasSuffix(base, ".log") {
		return "", "", false
	}
	stem := strings.TrimSuffix(base, ".log")
	i := strings.LastIndex(stem, "_")
	if i <= 0 || i == len(stem)-1 {
		return "", "", false
	}
	return stem[:i], stem[i+1:], true
}

// rootFor 解析某个 kind 对应的目录并对其做边界检查，确保精心构造的 kind
// 值永远无法逃逸到已配置的日志目录之外。
func rootFor(logDir, kind string) (string, error) {
	switch kind {
	case KindFailover, KindSwitchover, KindReplica, KindTopology, KindRollback, KindRecover:
		root, err := filepath.Abs(filepath.Join(logDir, kind))
		if err != nil {
			return "", err
		}
		return root, nil
	case "":
		root, err := filepath.Abs(logDir)
		if err != nil {
			return "", err
		}
		return root, nil
	default:
		return "", fmt.Errorf("unknown log kind %q", kind)
	}
}

// List 返回切换日志的元数据。kind 为空时同时列出 failover 和 switchover
// 两个目录；否则仅列出指定 kind。cluster 非空时按集群名称过滤。结果按
// 最新在前的顺序排列。
func List(logDir, kind, cluster string) ([]*Meta, error) {
	kinds := listableKinds
	if kind != "" {
		if _, err := rootFor(logDir, kind); err != nil {
			return nil, err
		}
		kinds = []string{kind}
	}

	var out []*Meta
	for _, k := range kinds {
		root, err := rootFor(logDir, k)
		if err != nil {
			return nil, err
		}
		entries, err := os.ReadDir(root)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			cn, date, ok := parseFilename(e.Name())
			if !ok {
				continue
			}
			if cluster != "" && cn != cluster {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			m := &Meta{
				ID:      k + "/" + e.Name(),
				Kind:    k,
				Cluster: cn,
				File:    e.Name(),
				Date:    date,
				Size:    info.Size(),
			}
			if t, pErr := time.ParseInLocation("20060102", date, time.Local); pErr == nil {
				m.StartTime = t
			}
			out = append(out, m)
		}
	}

	// 最新在前：优先按解析出的日期排序，再以 kind/名称排序以保证顺序稳定。
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].StartTime.Equal(out[j].StartTime) {
			return out[i].StartTime.After(out[j].StartTime)
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

// resolveContentPath 将 id（"kind/file"）限定在 {logDir}/kind 内部，拒绝
// 文件部分中包含路径分隔符 / 目录穿越（traversal）的情况。
func resolveContentPath(logDir, id string) (string, error) {
	i := strings.Index(id, "/")
	if i <= 0 {
		return "", fmt.Errorf("invalid log id %q", id)
	}
	kind, file := id[:i], id[i+1:]
	if file == "" || strings.ContainsAny(file, `/\`) {
		return "", fmt.Errorf("invalid log id %q", id)
	}
	root, err := rootFor(logDir, kind)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, file)
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absPath, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("log id escapes log directory")
	}
	if _, _, ok := parseFilename(file); !ok {
		return "", fmt.Errorf("not a switch log file: %s", file)
	}
	return absPath, nil
}

// Read 返回由 id 指定的日志文件的原始内容。
func Read(logDir, id string) ([]byte, error) {
	path, err := resolveContentPath(logDir, id)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

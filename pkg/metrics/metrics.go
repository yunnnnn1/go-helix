// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

// Package metrics 提供一个极小、无第三方依赖的指标注册表，输出 Prometheus
// 文本展示格式。它刻意不依赖 prometheus/client_golang：go-helix 只需要少数几个
// Counter/Gauge，而将注册表保持为仅使用原子操作可维持无 CGO、最小化的构建。
// 以后可以用官方客户端替换它，而无需改变各调用点的语义。
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Kind 枚举指标类型。
type Kind string

const (
	KindCounter Kind = "counter"
	KindGauge   Kind = "gauge"
)

// metric 表示一个带有固定标签集合的具名指标。每个唯一的、有序的标签值组合
// 对应一个序列（作为 series 中的一个条目存储）。
type metric struct {
	name   string
	help   string
	kind   Kind
	labels []string // 固定的标签键，按注册顺序排列并保持稳定

	mu     sync.Mutex
	series map[string]*series
	order  []string // 序列键的插入顺序
}

type series struct {
	labelValues []string
	value       atomic.Int64
}

// Registry 是指标的集合。零值不可用，请使用 New 创建。
type Registry struct {
	mu      sync.RWMutex
	metrics map[string]*metric
	order   []string
}

// New 返回一个空的注册表。
func New() *Registry {
	return &Registry{metrics: make(map[string]*metric)}
}

// Default 是进程级别的注册表，供包级别辅助函数和 /metrics 端点使用。
var Default = New()

// seriesKey 根据标签值构建一个稳定的键。
func seriesKey(labelValues []string) string {
	return strings.Join(labelValues, "\x00")
}

// register 定义一个指标。labelKeys 是该指标固定的标签名。当用同名重复定义且
// 签名一致时，会返回已有的指标（幂等，在测试中很有用）。
func (r *Registry) register(name, help string, kind Kind, labelKeys []string) *metric {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.metrics[name]; ok {
		return m
	}
	cp := append([]string(nil), labelKeys...)
	m := &metric{
		name:   name,
		help:   help,
		kind:   kind,
		labels: cp,
		series: make(map[string]*series),
	}
	r.metrics[name] = m
	r.order = append(r.order, name)
	return m
}

// Counter 定义一个计数器。
func (r *Registry) Counter(name, help string, labels ...string) *Handle {
	return &Handle{m: r.register(name, help, KindCounter, labels)}
}

// Gauge 定义一个仪表盘。
func (r *Registry) Gauge(name, help string, labels ...string) *Handle {
	return &Handle{m: r.register(name, help, KindGauge, labels)}
}

// Handle 引用一个指标，并将更新路由到具体的序列。
type Handle struct {
	m *metric
}

// series 返回 labelValues 对应的序列（标签值数量必须与已注册的标签数量
// 一致），首次使用时创建。
func (h *Handle) series(labelValues []string) *series {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	key := seriesKey(labelValues)
	if s, ok := h.m.series[key]; ok {
		return s
	}
	s := &series{labelValues: append([]string(nil), labelValues...)}
	h.m.series[key] = s
	h.m.order = append(h.m.order, key)
	return s
}

// Inc 将值加 1。
func (h *Handle) Inc(labelValues ...string) { h.Add(1, labelValues...) }

// Add 将值增加 delta（Gauge 允许为负数）。
func (h *Handle) Add(delta int64, labelValues ...string) {
	h.series(labelValues).value.Add(delta)
}

// Set 设置一个绝对值（用于 Gauge）。
func (h *Handle) Set(v int64, labelValues ...string) {
	h.series(labelValues).value.Store(v)
}

// 基于 Default 的包级别便捷函数。

func Counter(name, help string, labels ...string) *Handle {
	return Default.Counter(name, help, labels...)
}

func Gauge(name, help string, labels ...string) *Handle {
	return Default.Gauge(name, help, labels...)
}

// ---------------------------------------------------------------------------
// 展示
// ---------------------------------------------------------------------------

// escapeLabelValue 按 Prometheus 的要求替换反斜杠、换行符和双引号。
func escapeLabelValue(v string) string {
	r := strings.NewReplacer("\\", `\\`, "\n", `\n`, `"`, `\"`)
	return r.Replace(v)
}

// renderSeries 渲染一行序列。
func renderSeries(name string, labels []string, s *series, b *strings.Builder) {
	b.WriteString(name)
	if len(labels) > 0 {
		b.WriteString("{")
		for i, k := range labels {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(b, "%s=\"%s\"", k, escapeLabelValue(s.labelValues[i]))
		}
		b.WriteString("}")
	}
	fmt.Fprintf(b, " %d\n", s.value.Load())
}

// Gather 按注册顺序写入所有指标的 Prometheus 文本展示格式，并将各序列归组到
// 其对应的 HELP/TYPE 行之下。
func (r *Registry) Gather(w io.Writer) error {
	r.mu.RLock()
	names := append([]string{}, r.order...)
	metrics := make(map[string]*metric, len(names))
	for _, n := range names {
		metrics[n] = r.metrics[n]
	}
	r.mu.RUnlock()

	var b strings.Builder
	for _, name := range names {
		m := metrics[name]
		fmt.Fprintf(&b, "# HELP %s %s\n", name, m.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", name, m.kind)

		m.mu.Lock()
		keys := append([]string{}, m.order...)
		m.mu.Unlock()
		// 即使创建顺序不同，也保证序列顺序稳定。
		sort.Strings(keys)
		for _, k := range keys {
			m.mu.Lock()
			s := m.series[k]
			m.mu.Unlock()
			renderSeries(name, m.labels, s, &b)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

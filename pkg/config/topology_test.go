// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package config

import "testing"

func boolPtr(b bool) *bool { return &b }

// resetTopologyOverrides 清除所有按集群的覆盖，使测试彼此独立，
// 也不受 Raft 下发状态的影响。
func resetTopologyOverrides() {
	topologyOverridesMu.Lock()
	topologyOverrides = map[string]*TopologyConfig{}
	topologyOverridesMu.Unlock()
}

// withFileTopology 临时安装一个文件级拓扑，并在清理时恢复之前的
// currentConfig。
func withFileTopology(t *testing.T, tc TopologyConfig) {
	t.Helper()
	mu.Lock()
	prev := currentConfig
	root := &Config{}
	root.Manager.Topology = tc
	currentConfig = root
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		currentConfig = prev
		mu.Unlock()
	})
}

func TestTopologyConfig_DefaultsTrue(t *testing.T) {
	tc := TopologyConfig{}
	if !tc.Detach() {
		t.Error("Detach() must default to true")
	}
	if !tc.Preserve() {
		t.Error("Preserve() must default to true")
	}
	if !tc.IntermediateRecoverEnabled() {
		t.Error("IntermediateRecoverEnabled() must default to true")
	}
}

func TestTopologyConfig_ExplicitValues(t *testing.T) {
	tc := TopologyConfig{
		DetachLostReplicas:   boolPtr(false),
		PreserveHierarchy:    boolPtr(false),
		IntermediateRecovery: boolPtr(false),
	}
	if tc.Detach() || tc.Preserve() || tc.IntermediateRecoverEnabled() {
		t.Error("explicit false must be honoured by all getters")
	}

	tc2 := TopologyConfig{
		DetachLostReplicas:   boolPtr(true),
		PreserveHierarchy:    boolPtr(true),
		IntermediateRecovery: boolPtr(true),
	}
	if !tc2.Detach() || !tc2.Preserve() || !tc2.IntermediateRecoverEnabled() {
		t.Error("explicit true must be honoured by all getters")
	}
}

// 在没有按集群覆盖时，EffectiveTopology 反映内置默认值（true）
// 或配置文件中设置的值。
func TestEffectiveTopology_DefaultAndFile(t *testing.T) {
	resetTopologyOverrides()
	t.Cleanup(resetTopologyOverrides)

	// 缺失/为空的文件拓扑会解析为内置默认值 true。
	eff := EffectiveTopology("cluster-a")
	if !eff.Detach() || !eff.Preserve() || !eff.IntermediateRecoverEnabled() {
		t.Fatal("effective policy must default to true without file value or override")
	}

	// 不存在覆盖时，会采用配置文件中设置的 false。
	withFileTopology(t, TopologyConfig{
		DetachLostReplicas:   boolPtr(false),
		PreserveHierarchy:    boolPtr(false),
		IntermediateRecovery: boolPtr(false),
	})
	eff = EffectiveTopology("cluster-a")
	if eff.Detach() || eff.Preserve() || eff.IntermediateRecoverEnabled() {
		t.Fatal("file-configured false must be effective without an override")
	}
}

// 按集群的覆盖仅在该集群上优先于文件值；其他集群仍保持文件值，
// 而清除覆盖会使该集群恢复为文件值。
func TestEffectiveTopology_PerClusterOverride(t *testing.T) {
	resetTopologyOverrides()
	t.Cleanup(resetTopologyOverrides)

	withFileTopology(t, TopologyConfig{
		DetachLostReplicas:   boolPtr(true),
		PreserveHierarchy:    boolPtr(true),
		IntermediateRecovery: boolPtr(true),
	})

	SetTopologyOverride("cluster-a", &TopologyConfig{
		DetachLostReplicas:   boolPtr(false),
		PreserveHierarchy:    boolPtr(false),
		IntermediateRecovery: boolPtr(false),
	})

	a := EffectiveTopology("cluster-a")
	if a.Detach() || a.Preserve() || a.IntermediateRecoverEnabled() {
		t.Error("per-cluster override false must win over file true on cluster-a")
	}
	b := EffectiveTopology("cluster-b")
	if !b.Detach() || !b.Preserve() || !b.IntermediateRecoverEnabled() {
		t.Error("cluster-b must be unaffected by the cluster-a override")
	}

	// 清除 cluster-a 的覆盖会使其恢复为文件值。
	SetTopologyOverride("cluster-a", nil)
	a = EffectiveTopology("cluster-a")
	if !a.Detach() || !a.Preserve() || !a.IntermediateRecoverEnabled() {
		t.Error("cluster-a must revert to file values once its override is cleared")
	}
}

// 部分覆盖只会替换它所设置的开关；其余开关回退到文件值
// （逐字段合并，绝不整体替换结构体）。
func TestEffectiveTopology_PartialOverrideMerge(t *testing.T) {
	resetTopologyOverrides()
	t.Cleanup(resetTopologyOverrides)

	withFileTopology(t, TopologyConfig{
		DetachLostReplicas:   boolPtr(true),
		PreserveHierarchy:    boolPtr(false),
		IntermediateRecovery: boolPtr(true),
	})

	// 仅覆盖 detach；preserve/intermediate 必须保持文件值。
	SetTopologyOverride("cluster-a", &TopologyConfig{
		DetachLostReplicas: boolPtr(false),
	})
	eff := EffectiveTopology("cluster-a")
	if eff.Detach() {
		t.Error("overridden detach must be false")
	}
	if eff.Preserve() {
		t.Error("preserve must fall back to the file value false")
	}
	if !eff.IntermediateRecoverEnabled() {
		t.Error("intermediate recovery must fall back to the file value true")
	}
}

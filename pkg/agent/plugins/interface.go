// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package plugins

import (
	"context"
)

// Plugin 表示 Agent 的一个可加载能力。
type Plugin interface {
	// Name 返回插件的唯一名称（例如 "gtid_sync"）
	Name() string

	// Init 初始化插件。可用于建立本地数据库连接、读取配置等。
	Init(ctx context.Context, config map[string]string) error

	// Execute 运行插件的某个具体动作。
	Execute(ctx context.Context, action string, params map[string]string) (map[string]string, error)
}

var registry = make(map[string]Plugin)

// Register 注册一个新插件。
func Register(p Plugin) {
	registry[p.Name()] = p
}

// Get 按名称获取已注册的插件。
func Get(name string) Plugin {
	return registry[name]
}

// GetAll 返回所有已注册的插件。
func GetAll() map[string]Plugin {
	return registry
}

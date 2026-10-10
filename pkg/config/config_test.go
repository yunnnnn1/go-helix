// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	// 创建一个临时 JSON 文件
	content := []byte(`{
		"manager": {
			"log_dir": "/var/log/github.com/yunnnnn1/go-helix/manager",
			"bind_addr": "0.0.0.0:8081",
			"raft_id": "node1",
			"second_check": true
		},
		"datacenter": {
			"type": "mysql",
			"host": "127.0.0.1",
			"port": 3306
		},
		"agent": {
			"log_dir": "/var/log/github.com/yunnnnn1/go-helix/agent",
			"timeout": "10s",
			"instances": [
				{
					"bind_addr": "0.0.0.0:50051",
					"cluster_name": "test_cluster"
				}
			]
		},
		"auth_token": "secret123"
	}`)

	tmpfile, err := os.CreateTemp("", "config_test_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpfile.Close()

	// 测试 Load
	err = Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	cfg := Get()
	if cfg == nil {
		t.Fatal("Get() returned nil")
	}

	// 断言
	if cfg.Manager.BindAddr != "0.0.0.0:8081" {
		t.Errorf("Expected Manager.BindAddr to be 0.0.0.0:8081, got %s", cfg.Manager.BindAddr)
	}
	if cfg.Manager.LogDir != "/var/log/github.com/yunnnnn1/go-helix/manager" {
		t.Errorf("Expected Manager.LogDir to be /var/log/github.com/yunnnnn1/go-helix/manager, got %s", cfg.Manager.LogDir)
	}
	if cfg.Manager.SecondCheck != true {
		t.Errorf("Expected Manager.SecondCheck to be true, got %v", cfg.Manager.SecondCheck)
	}
	if cfg.Datacenter.Port != 3306 {
		t.Errorf("Expected Datacenter.Port to be 3306, got %d", cfg.Datacenter.Port)
	}
	if cfg.AuthToken != "secret123" {
		t.Errorf("Expected AuthToken to be secret123, got %s", cfg.AuthToken)
	}
	if len(cfg.Agent.Instances) != 1 {
		t.Fatalf("Expected 1 Agent instance, got %d", len(cfg.Agent.Instances))
	}
	if cfg.Agent.Instances[0].ClusterName != "test_cluster" {
		t.Errorf("Expected Agent.Instances[0].ClusterName to be test_cluster, got %s", cfg.Agent.Instances[0].ClusterName)
	}
	if cfg.Agent.LogDir != "/var/log/github.com/yunnnnn1/go-helix/agent" {
		t.Errorf("Expected Agent.LogDir to be /var/log/github.com/yunnnnn1/go-helix/agent, got %s", cfg.Agent.LogDir)
	}
}

func TestReloadConfig(t *testing.T) {
	content1 := []byte(`{"auth_token": "first"}`)
	content2 := []byte(`{"auth_token": "second"}`)

	tmpfile, err := os.CreateTemp("", "config_reload_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpfile.Name())

	// 写入 content1
	os.WriteFile(tmpfile.Name(), content1, 0644)

	Load(tmpfile.Name())
	cfg := Get()
	if cfg.AuthToken != "first" {
		t.Fatalf("Expected AuthToken 'first', got %s", cfg.AuthToken)
	}

	// 休眠以确保 ModTime 不同（文件系统通常为 1 秒或毫秒级分辨率）
	time.Sleep(10 * time.Millisecond)

	// 写入 content2
	os.WriteFile(tmpfile.Name(), content2, 0644)

	// 强制重新加载，以避免某些操作系统因修改时间分辨率导致测试不稳定
	err = reload(true)
	if err != nil {
		t.Fatalf("reload() error = %v", err)
	}

	cfg = Get()
	if cfg.AuthToken != "second" {
		t.Fatalf("Expected AuthToken 'second', got %s", cfg.AuthToken)
	}
}

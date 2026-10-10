// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/logic"
)

// endpointTimeout 解析配置的单个钩子超时时间（默认 15s）。
func endpointTimeout(c config.EndpointSwitchConfig) time.Duration {
	return config.ParseDuration(c.Timeout, 15*time.Second)
}

// buildEndpointRequest 将 manager 配置映射为 logic 层的钩子请求。
func buildEndpointRequest(command, cluster string, c config.EndpointSwitchConfig, old, new *datacenter.Node) *logic.EndpointHookRequest {
	return &logic.EndpointHookRequest{
		Cluster:      cluster,
		Command:      command,
		EndpointType: c.EndpointType,
		Endpoint:     c.EndpointValue,
		OldMaster:    old.Addr(),
		NewMaster:    new.Addr(),
		ExecHost:     c.ExecHost,
		SyncURL:      c.SyncURL, // 仅在提升前（pre-promote）调用时同步
		Sha256:       c.Sha256,
		Timeout:      endpointTimeout(c),
	}
}

// endpointSwitcher 根据故障节点的数据库类型解析对应的 EndpointSwitcher。
func endpointSwitcher(dbType string) (logic.EndpointSwitcher, error) {
	if dbType == "" {
		dbType = "mysql"
	}
	return logic.GetEndpointSwitcher(dbType)
}

// prePromoteEndpoint 在提升前运行 status 钩子。钩子在此处同步
// （每次切换仅一次）。仅当为 Required（P0）时，失败才是致命的。
func prePromoteEndpoint(ctx context.Context, cluster string, c config.EndpointSwitchConfig, old, new *datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	switcher, err := endpointSwitcher(old.DbType)
	if err != nil {
		if c.Required {
			return err
		}
		fl.Printf("[Endpoint] Warning: %v", err)
		return nil
	}

	req := buildEndpointRequest("status", cluster, c, old, new)
	res, err := switcher.RunHook(ctx, req)
	if err != nil {
		if c.Required {
			return fmt.Errorf("endpoint precheck: %w", err)
		}
		fl.Printf("[Endpoint] Warning: precheck hook failed: %v", err)
		return nil
	}
	if !res.Success {
		if c.Required {
			return fmt.Errorf("endpoint precheck failed: %s", res.Message)
		}
		fl.Printf("[Endpoint] Warning: precheck reported failure: %s", res.Message)
		return nil
	}
	fl.Printf("[Endpoint] precheck ok, endpoint currently at %s", res.CurrentMaster)
	return nil
}

// postPromoteEndpoint 将入口（endpoint）切换到新主库（start），并通过轮询
// status 进行校验，直到钩子上报新地址为止。
func postPromoteEndpoint(ctx context.Context, cluster string, c config.EndpointSwitchConfig, old, new *datacenter.Node) error {
	fl := logging.FailoverLogger(ctx)
	switcher, err := endpointSwitcher(old.DbType)
	if err != nil {
		if c.Required {
			return err
		}
		fl.Printf("[Endpoint] Warning: %v", err)
		return nil
	}

	// start：不要重新同步（已在预检时同步）。
	startReq := buildEndpointRequest("start", cluster, c, old, new)
	startReq.SyncURL = ""
	res, err := switcher.RunHook(ctx, startReq)
	if err != nil {
		return endpointPostFailure(c, fmt.Errorf("endpoint switch hook: %w", err))
	}
	if !res.Success {
		return endpointPostFailure(c, fmt.Errorf("endpoint switch hook reported failure: %s", res.Message))
	}
	fl.Printf("[Endpoint] switch hook succeeded: %s", res.Message)

	// 通过轮询 status 进行校验；agent 钩子可能需要一点时间
	// 完成传播（GARP/DNS 更新），之后才会上报新的
	// 当前主库。
	timeout := endpointTimeout(c)
	deadline := time.Now().Add(timeout)
	statusReq := buildEndpointRequest("status", cluster, c, old, new)
	statusReq.SyncURL = ""

	for {
		vr, vErr := switcher.RunHook(ctx, statusReq)
		if vErr == nil && vr.Success && vr.CurrentMaster == new.Addr() {
			fl.Printf("[Endpoint] verified: endpoint points at new master %s", new.Addr())
			return nil
		}
		if time.Now().After(deadline) {
			detail := "endpoint verify timed out"
			if vErr != nil {
				detail = vErr.Error()
			} else if vr != nil {
				detail = fmt.Sprintf("endpoint still at %q: %s", vr.CurrentMaster, vr.Message)
			}
			return endpointPostFailure(c, fmt.Errorf("%s", detail))
		}
		time.Sleep(time.Second)
	}
}

// endpointPostFailure 在提升后应用失败策略：Required 失败会返回错误中止
// （主库已切换但入口未切换；需要人工介入），
// 其余情况仅作为警告。
func endpointPostFailure(c config.EndpointSwitchConfig, err error) error {
	if c.Required {
		return err
	}
	logging.Printf("[Endpoint] non-required endpoint hook failure: %v", err)
	return nil
}

// runEndpointHooks 是 Failover/Switch 使用的唯一集成入口：
// 提升前为阶段 "pre"，提升后为阶段 "post"。
//
// 入口标识（VIP/域名）的优先级：存储在 cluster_meta 中的值（由 Monitor
// 放入 ctx）优先；manager 配置文件作为兜底。运维相关设置
// （exec_host/sync_url/sha256/timeout/required）始终取自 manager 配置的
// endpoint_switch 条目。
func runEndpointHooks(ctx context.Context, phase string, old, new *datacenter.Node) error {
	c, hasConfig := config.EndpointSwitchFor(old.ClusterName)

	epType, epValue, hasStored := logic.ClusterEndpointFromContext(ctx)
	if !logic.HasRealEndpoint(epType, epValue) {
		// 存储的入口缺失或为空：回退到配置文件中的值。
		if hasConfig && logic.HasRealEndpoint(c.EndpointType, c.EndpointValue) {
			epType, epValue = c.EndpointType, c.EndpointValue
			hasStored = true
		}
	}

	// 各处都没有入口且没有配置条目：无需任何操作。
	if !hasStored && !hasConfig {
		return nil
	}

	// 传递给钩子的权威入口。
	c.EndpointType = epType
	c.EndpointValue = epValue

	if phase == "pre" {
		return prePromoteEndpoint(ctx, old.ClusterName, c, old, new)
	}
	return postPromoteEndpoint(ctx, old.ClusterName, c, old, new)
}

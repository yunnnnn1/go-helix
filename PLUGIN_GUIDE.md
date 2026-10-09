# 插件开发指南（Agent Plugin）

本文档说明如何为 go-helix 的 Agent 新增一个插件（Plugin）。

- 插件接口定义：[pkg/agent/plugins/interface.go](pkg/agent/plugins/interface.go)
- 共享连接池：[pkg/agent/plugins/connpool.go](pkg/agent/plugins/connpool.go)
- 参考样板：[pkg/agent/plugins/gtidsync/plugin.go](pkg/agent/plugins/gtidsync/plugin.go)
- 插件调用入口：[pkg/agent/server.go](pkg/agent/server.go) 的 `ExecutePlugin`

---

## 1. 核心设计原则（务必遵守）

1. **插件无状态**：插件在进程内是全局单例，`Init` **只做一次性全局准备，不要保存任何与具体 MySQL 实例绑定的连接/地址**。连接在每次 `Execute` 时按 params 中的目标地址动态获取。
2. **凭据由 Agent 本地注入，不经过网络**：Manager 经 gRPC 只传目标地址 `host`/`port`；Agent Server 在进程内把当前实例的 `db_host`/`db_port`/`db_user`/`db_pass`（及特权的 `manager_user`/`manager_pass`）注入 params，复制账户 `repl` 不注入。因此插件**不要要求调用方在请求里传密码**，也**不要把 params 整体打印到日志**。
3. **连接一律走共享池**：通过 `plugins.DefaultPool().Get(...)` 取 `*sql.DB`，不要自己 `sql.Open`。
4. **统一使用项目自身日志**：导入 `log "github.com/yunnnnn1/go-helix/pkg/logging"`，用 `log.Printf/...` 输出，**不要直接导入标准库 `log`**。
5. **写操作必须参数化**，杜绝 SQL 注入；长耗时逻辑受传入 `ctx` 的超时控制。

---

## 2. 新增插件步骤（共 3 步）

| 步骤 | 文件 | 动作 |
|------|------|------|
| ① | `pkg/agent/plugins/<name>/plugin.go`（新增） | 实现 `plugins.Plugin` 接口，在 `init()` 自注册 |
| ② | [pkg/agent/server.go](pkg/agent/server.go) | 在空白导入区加一行 `_ "github.com/yunnnnn1/go-helix/pkg/agent/plugins/<name>"` |
| ③ | `pkg/agent/plugins/<name>/plugin_test.go`（建议） | 编写单元测试 |

> **无需改动**：proto 定义、`ExecutePlugin` 分发逻辑、注册中心、Manager 侧调用——这些都是通用的。

---

## 3. 完整代码模板

新建 `pkg/agent/plugins/myplugin/plugin.go`：

```go
package myplugin

import (
	"context"
	"fmt"

	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
)

// MyPlugin 不持有任何实例级连接，保持无状态。
type MyPlugin struct{}

// 包加载即自注册，无需手动登记。
func init() {
	plugins.Register(&MyPlugin{})
}

func (p *MyPlugin) Name() string {
	return "my_plugin" // 必须全局唯一，Manager 用它作为 plugin_name
}

// Init 只做一次性全局准备；不要在这里保存连接。
func (p *MyPlugin) Init(ctx context.Context, cfg map[string]string) error {
	log.Printf("[Plugin:MyPlugin] ready (stateless)")
	return nil
}

// Execute 是业务入口，按 action 分发。
func (p *MyPlugin) Execute(ctx context.Context, action string, params map[string]string) (map[string]string, error) {
	switch action {
	case "Ping":
		// 示例：返回目标地址，验证调用链
		return map[string]string{
			"target": params["host"] + ":" + params["port"],
		}, nil

	// case "YourAction":
	// 	db, err := plugins.DefaultPool().Get(ctx,
	// 		net.JoinHostPort(params["db_host"], params["db_port"]),
	// 		params["db_user"], params["db_pass"])
	// 	...

	default:
		return nil, fmt.Errorf("my_plugin: unknown action %q", action)
	}
}
```

在 [pkg/agent/server.go](pkg/agent/server.go) 的导入处加一行：

```go
	// Register Plugins
	_ "github.com/yunnnnn1/go-helix/pkg/agent/plugins/gtidsync"
	_ "github.com/yunnnnn1/go-helix/pkg/agent/plugins/myplugin" // ← 新增
```

---

## 4. params 参数契约

插件在 `Execute` 中可读取的 params 键：

| 键 | 含义 | 来源 |
|----|------|------|
| `host` / `port` | **目标地址**（本次操作的远端，如旧 Master） | Manager 经 gRPC 传入 |
| `db_host` / `db_port` | **本地实例地址**（接收操作的当前实例） | Agent Server 进程内注入 |
| `db_user` / `db_pass` | 本地实例的探活凭据 | Agent Server 进程内注入 |
| `manager_user` / `manager_pass` | 本地实例的切换/特权账户凭据 | Agent Server 进程内注入（仅在配置了 manager 账户时） |

- 若插件需要自定义额外参数，直接在 `params` 中约定新键即可，无需改 proto（params 是 `map<string,string>`）。
- 插件收到的是 Server 复制后的 params，**原始请求 params 不会被修改**。
- **复制账户（repl）永不会注入**：它仅供从库 IO 线程认证，插件若需特权操作请使用 `manager_user/manager_pass`。
- `Init` 阶段的 cfg 只包含 `db_host/db_port/db_user/db_pass`（不含 manager 凭据），且仅用于一次性全局准备。

### gtid_sync 内置动作

| 动作 | 用途 | 额外 params |
|------|------|------------|
| `CatchupFrom` | 经复制协议（canal，源端 mysqld 存活）逻辑回放缺失事务 | `host`/`port` = 目标源 |
| `ReplayRescue` | 对已保存到本地的 binlog 片段离线解析（`BinlogParser`，不依赖源端 mysqld），`SET gtid_next` 保留原 GTID 回放并复验 | `fragment_path`（片段路径）、`target_gtid`（必须达到的 GTID） |

> `ReplayRescue` 由 Agent 的 `ApplyBinlogRescue` RPC 在片段落盘后调用；它要求 `db_host/db_port` 与 manager 凭据，回放后校验 `gtid_executed 包含 target_gtid`，未达成返回错误（fail closed）。

**gtid_sync 回放安全保护**：

- UPDATE/DELETE 经 `sqlbuilder` 强制带 `LIMIT 1`；实际影响行数 > 1（无唯一键的重复行）即判该事务失败；
- 行回放失败不计入完成计数；全部 SQL 经 `sqlbuilder` 参数化生成，防注入；
- canal 为一次性流，按调用现建现关，并保留 30s 兜底超时；本地 GTID 集合已包含目标时直接跳过。

---

## 5. 使用共享连接池

需要数据库连接时：

```go
import (
	"net"
	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
)

addr := net.JoinHostPort(params["db_host"], params["db_port"]) // 兼容 IPv6
db, err := plugins.DefaultPool().Get(ctx, addr, params["db_user"], params["db_pass"])
if err != nil {
	return nil, fmt.Errorf("connect %s: %w", addr, err)
}

// 使用参数化查询，受 ctx 超时控制
row := db.QueryRowContext(ctx, "SELECT @@global.read_only")
```

连接池特性（见 [connpool.go](pkg/agent/plugins/connpool.go)）：

- 按 `user@host:port` 缓存 `*sql.DB`；同目标的并发请求**只拨号一次**；
- 每个目标的物理连接数受 `max_open_conns` / `max_idle_conns` 限制；
- 空闲物理连接经 `idle_time` 自动断开；
- 后台按 `reclaim_interval` 回收超过 `reclaim_after` 未使用的槽位，槽位总数受 `max_entries` LRU 上限约束；
- Agent 退出时统一 `Close()`。

以上参数在 Agent 配置的 `agent.plugin_pool` 中调整，缺省值见 [pkg/config/config.go](pkg/config/config.go) 的 `DefaultPluginPoolConfig`。

---

## 6. 单元测试

新建 `pkg/agent/plugins/myplugin/plugin_test.go`。纯逻辑可直接构造插件调用，无需起 gRPC：

```go
package myplugin

import (
	"context"
	"testing"
)

func TestMyPlugin_Ping(t *testing.T) {
	p := &MyPlugin{}
	out, err := p.Execute(context.Background(), "Ping", map[string]string{
		"host": "10.0.0.1", "port": "3307",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out["target"] != "10.0.0.1:3307" {
		t.Errorf("unexpected target %q", out["target"])
	}
}

func TestMyPlugin_UnknownAction(t *testing.T) {
	p := &MyPlugin{}
	if _, err := p.Execute(context.Background(), "Nope", nil); err == nil {
		t.Error("expected error for unknown action")
	}
}
```

涉及真实连接的逻辑，可参考连接池测试 [connpool_test.go](pkg/agent/plugins/connpool_test.go) 中替换 `dialer` 的做法，避免连真实数据库。

---

## 7. 调用方式（联调）

通过 gRPC 对 Agent 发起 `ExecutePlugin`：

```json
{
  "plugin_name": "my_plugin",
  "action": "Ping",
  "params": { "host": "10.0.0.1", "port": "3307" }
}
```

注意请求中**只传目标地址，不传凭据**。

---

## 8. 常见坑（Pitfalls）

| 坑 | 后果 | 正确做法 |
|----|------|----------|
| 在 `Init` 里 `sql.Open` 并保存到结构体 | 多实例下连接被反复覆盖、串用目标 | 连接放 `Execute` 里走共享池 |
| 要求 Manager 在 params 传密码 / 打印整个 params | 凭据经网络传输、可能泄露到日志 | 凭据用注入的 `db_user/db_pass`，日志只打非敏感字段 |
| 自己 `sql.Open` 不关闭 | 连接泄漏 | 一律 `plugins.DefaultPool().Get` |
| 直接导入标准库 `log` | 绕过统一日志目录/轮转，日志分散 | 导入 `log "github.com/yunnnnn1/go-helix/pkg/logging"` |
| 拼接 SQL 字符串 | SQL 注入 | 参数化查询 / 占位符 |
| 用 `fmt.Sprintf("%s:%d")` 拼地址 | IPv6 地址非法 | 用 `net.JoinHostPort` |
| 启动无法随 ctx 取消的后台 goroutine | 请求结束后仍占用资源、挂死 | 监听 `ctx.Done()`，并加超时兜底（参考 gtid_sync 的 30s 保护） |

---

## 9. 校验命令

```bash
go build ./...
go vet ./...
go test -count=1 ./...
go test -count=1 -race github.com/yunnnnn1/go-helix/pkg/agent/...
```

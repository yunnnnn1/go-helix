// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package logging

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

type ctxKey struct{}

// defaultFlags 应用于本包创建的每个 logger。
const defaultFlags = log.LstdFlags | log.Lmicroseconds

var (
	baseMu sync.RWMutex
	// base 是进程级别的 logger。它默认输出到 stdout，并由 Init 重新配置为
	// 写入一个轮转文件。
	base = log.New(os.Stdout, "", defaultFlags)
)

// Default 返回进程级别的基础 logger。
func Default() *log.Logger {
	baseMu.RLock()
	defer baseMu.RUnlock()
	return base
}

func setBase(l *log.Logger) {
	baseMu.Lock()
	base = l
	baseMu.Unlock()
}

// Init 将基础 logger 配置为写入 {dir}/{name}.log，并自动进行轮转。当 dir 为空
// 时，基础 logger 继续写入 stdout。标准库的默认 logger 也会被重定向到同一个
// 输出目标，以便任何零散的第三方日志输出都能与我们自己的日志一起被捕获。
func Init(dir, name string) {
	if dir == "" {
		setBase(log.New(os.Stdout, "", defaultFlags))
		return
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("Failed to create log directory %s: %v", dir, err)
		return
	}

	writer := &lumberjack.Logger{
		Filename:   filepath.Join(dir, name+".log"),
		MaxSize:    100,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
	}
	setBase(log.New(writer, "", defaultFlags))

	// 将零散的标准库日志使用捕获到同一个轮转文件中。
	log.SetOutput(writer)
	log.SetFlags(defaultFlags)
}

// 包级别的日志 API。这些函数镜像标准库的 log 函数，但始终以配置好的基础
// logger 为目标，使各包可以导入 `log "github.com/yunnnnn1/go-helix/pkg/logging"` 并直接调用
// log.Printf/...，而无需做其他改动。

func Print(v ...any)                 { Default().Print(v...) }
func Printf(format string, v ...any) { Default().Printf(format, v...) }
func Println(v ...any)               { Default().Println(v...) }
func Fatal(v ...any)                 { Default().Fatal(v...) }
func Fatalf(format string, v ...any) { Default().Fatalf(format, v...) }

// SetOutput 将基础 logger 重定向到 w。
func SetOutput(w io.Writer) { Default().SetOutput(w) }

// 操作类型用于选择子目录，从而在物理上分隔各类操作日志。三种恢复变体各有
// 自己的类型，使控制台过滤条件与控制台操作按钮一一对应。
const (
	KindFailover   = "failover"   // 非计划：主库崩溃 / 不可达 / 等
	KindSwitchover = "switchover" // 计划内：优雅地接管主库
	KindReplica    = "replica"    // 修复单个副本的复制链路
	KindTopology   = "topology"   // 使整个集群收敛到一个主库
	KindRollback   = "rollback"   // 撤销最近一次切换
	KindRecover    = "recover"    // 旧版合并的恢复目录（只读）
)

// KindForRecover 将恢复请求的模式映射到其对应的日志类型。
func KindForRecover(mode string) string {
	switch mode {
	case "replica":
		return KindReplica
	case "topology":
		return KindTopology
	case "rollback":
		return KindRollback
	default:
		return KindRecover
	}
}

// NewOperationLogger 创建一个写入
// {dir}/{kind}/{cluster}_{date}.log 的 logger。kind 为 KindFailover 或
// KindSwitchover，从而将计划内切换日志与紧急故障切换日志分开存放。
func NewOperationLogger(dir, cluster, kind string) *log.Logger {
	opDir := filepath.Join(dir, kind)
	if err := os.MkdirAll(opDir, 0o755); err != nil {
		log.Printf("Failed to create operation log directory %s: %v", opDir, err)
		return Default()
	}

	dateStr := time.Now().Format("20060102")
	filename := filepath.Join(opDir, joinClusterDate(cluster, dateStr))

	return log.New(&lumberjack.Logger{
		Filename:   filename,
		MaxSize:    50,
		MaxBackups: 10,
		MaxAge:     90,
		Compress:   true,
	}, "", defaultFlags)
}

// NewFailoverLogger 作为紧急场景的简写予以保留。
func NewFailoverLogger(dir, cluster string) *log.Logger {
	return NewOperationLogger(dir, cluster, KindFailover)
}

func joinClusterDate(cluster, date string) string {
	return cluster + "_" + date + ".log"
}

// WithFailoverLogger 返回一个携带给定故障切换 logger 的子 context。
func WithFailoverLogger(ctx context.Context, l *log.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FailoverLogger 从 ctx 中提取故障切换 logger；当不存在作用域内的 logger 时，
// 回退到配置好的基础 logger。
func FailoverLogger(ctx context.Context) *log.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*log.Logger); ok && l != nil {
		return l
	}
	return Default()
}

package raft

import (
	"bytes"
	"sync"

	log "github.com/yunnnnn1/go-helix/pkg/logging"
)

// logSink 是一个 io.Writer，用于收集 Raft 的日志输出（hashicorp/raft 和
// hclog 以纯字节形式写入），并将每个完整的行转发给项目日志。完整的行会立即
// 输出；尚未以换行符结尾的尾部内容会保留到下一次 Write，因此日志行既不会被
// 拆分也不会丢失。
type logSink struct {
	mu   sync.Mutex
	buf  []byte
	emit func(line string)
}

// newLogSink 构建一个 sink。nodeID/addr 会标记到每一条输出的日志行上，以便
// 区分来自多个 Raft 节点的日志（addr 携带 Raft 绑定端口）。emit 默认为转发到
// pkg/logging，并加上 "[Raft nodeID addr]" 前缀；测试时也可传入自定义的 emit。
func newLogSink(nodeID, addr string, emit func(line string)) *logSink {
	if emit == nil {
		emit = func(line string) { log.Printf("[Raft %s %s] %s", nodeID, addr, line) }
	}
	return &logSink{emit: emit}
}

// Write 实现 io.Writer。它会缓冲 p，并输出每一个以换行符结尾的行。它始终
// 返回 len(p), nil——日志转发绝不会让调用方失败。
func (l *logSink) Write(p []byte) (int, error) {
	var lines []string

	l.mu.Lock()
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}
		lines = append(lines, string(l.buf[:i]))
		l.buf = l.buf[i+1:]
	}
	l.mu.Unlock()

	for _, line := range lines {
		l.emit(line)
	}
	return len(p), nil
}

// bufferedTail 返回当前保留的、尚未以换行符结尾的文本（供测试使用）。
func (l *logSink) bufferedTail() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.buf)
}

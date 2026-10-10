// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package agent

import (
	"bufio"
	"context"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

// allowedShellCommands 是 ExecuteShell（一元）和 ExecStream（流式）
// 共用的唯一白名单。该集合之外的命令一律不得执行。
var allowedShellCommands = map[string]bool{
	"df":     true,
	"uptime": true,
	"echo":   true,
	"ping":   true,
	"date":   true,
	"free":   true,
}

// ExecStream 运行白名单内的命令，并按行流式发送 stdout/stderr。
//
// 客户端断连时流 context 会被取消；exec.CommandContext 随即杀死
// 进程，因此长任务无法比其调用方存活更久。
func (s *Server) ExecStream(req *pb.ExecStreamRequest, srv pb.DBAgent_ExecStreamServer) error {
	ctx := srv.Context()

	if !allowedShellCommands[req.Command] {
		return s.sendStreamLine(srv, pb.ExecStreamLine_STDERR, "command not allowed for security reasons", -1, true)
	}
	if err := validateShellArgs(req.Command, req.Args); err != nil {
		return s.sendStreamLine(srv, pb.ExecStreamLine_STDERR, err.Error(), -1, true)
	}

	timeout := 10 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Agent.Timeout != "" {
		if d, err := time.ParseDuration(cfg.Agent.Timeout); err == nil {
			timeout = d
		}
	}
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, req.Command, req.Args...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return s.sendStreamLine(srv, pb.ExecStreamLine_STDERR, err.Error(), -1, true)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return s.sendStreamLine(srv, pb.ExecStreamLine_STDERR, err.Error(), -1, true)
	}

	log.Printf("[audit] exec-stream op=%s from=%s args=%v", req.Command, peerAddr(ctx), req.Args)

	if err := cmd.Start(); err != nil {
		return s.sendStreamLine(srv, pb.ExecStreamLine_STDERR, err.Error(), -1, true)
	}

	var (
		sendMu sync.Mutex
		wg     sync.WaitGroup
	)
	send := func(ch pb.ExecStreamLine_Channel, line string) {
		sendMu.Lock()
		defer sendMu.Unlock()
		if err := srv.Send(&pb.ExecStreamLine{Channel: ch, Line: line}); err != nil {
			log.Printf("[ExecStream] send failed: %v", err)
		}
	}

	pipe := func(ch pb.ExecStreamLine_Channel, r io.Reader) {
		defer wg.Done()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for sc.Scan() {
			send(ch, sc.Text())
		}
	}

	wg.Add(2)
	go pipe(pb.ExecStreamLine_STDOUT, stdoutPipe)
	go pipe(pb.ExecStreamLine_STDERR, stderrPipe)

	waitErr := cmd.Wait()
	wg.Wait()

	exitCode := 0
	if waitErr != nil {
		if exitError, ok := waitErr.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = -1
		}
	}

	sendMu.Lock()
	defer sendMu.Unlock()
	return srv.Send(&pb.ExecStreamLine{
		Channel:  pb.ExecStreamLine_STDOUT,
		ExitCode: int32(exitCode),
		Final:    true,
	})
}

// sendStreamLine 是用于提前返回错误的小辅助函数。
func (s *Server) sendStreamLine(srv pb.DBAgent_ExecStreamServer, ch pb.ExecStreamLine_Channel, line string, code int32, final bool) error {
	return srv.Send(&pb.ExecStreamLine{Channel: ch, Line: line, ExitCode: code, Final: final})
}

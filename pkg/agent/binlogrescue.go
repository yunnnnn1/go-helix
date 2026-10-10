// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package agent

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	"github.com/yunnnnn1/go-helix/pkg/config"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var sessionIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

const (
	// rescueDirName 是（fetch 根目录内）用于存放分片的子目录。
	rescueDirName = "binlog_rescue"
	// rescueActionReplay 是 gtid_sync 的动作：离线解析已保存的分片，
	// 并在保留源 GTID 的前提下重放它。
	rescueActionReplay = "ReplayRescue"
)

// ApplyBinlogRescue 将接收到的获救二进制日志分片写入受沙箱约束的临时文件，
// 然后请求 gtid_sync 插件离线解析并重放它，同时保留源 GTID
// （SET gtid_next）。插件负责执行最终的 GTID 校验，若未达到目标
// 状态则采取失败即关闭（fail closed）策略。
func (s *Server) ApplyBinlogRescue(srv pb.DBAgent_ApplyBinlogRescueServer) error {
	ctx := srv.Context()

	// 救援授权与上限不再读本地 binlog_rescue 配置，而由 Manager 读取候选
	// 节点表 meta_data 的 rescue_* 字段后，在数据流开头通过消息下发。
	if s.mgrUser == "" {
		return status.Error(codes.FailedPrecondition, "manager account not configured; cannot apply rescued binlog")
	}

	// 临时分片仍写入通用的文件拉取根目录（file_fetch_roots，不属于
	// binlog_rescue 配置），保持本地监禁。
	roots := []string{}
	if cfg := config.Get(); cfg != nil {
		roots = cfg.Agent.FileFetchRoots
	}
	if len(roots) == 0 {
		return status.Error(codes.FailedPrecondition,
			"no writable root configured for rescue fragment (agent.file_fetch_roots)")
	}

	var (
		f             *os.File
		tmpPath       string
		dir           string
		loaded        int64
		targetGTID    string
		rescueEnabled bool
		maxBytes      int64
	)
	openFragment := func(session string) error {
		tmpPath = filepath.Join(dir, session+".bin")
		var err error
		f, err = os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("create fragment file %s: %w", tmpPath, err)
		}
		return nil
	}
	cleanup := func() {
		if f != nil {
			f.Close()
		}
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}

	for {
		msg, rErr := srv.Recv()
		if rErr != nil {
			cleanup()
			if rErr == io.EOF {
				return status.Error(codes.Internal, "rescue stream ended before done marker")
			}
			return rErr
		}

		// 处理 Manager 下发的授权头：enabled=true 才放行，并创建分片目录。
		if msg.Enabled {
			rescueEnabled = true
		}
		if msg.MaxBytes > 0 {
			maxBytes = msg.MaxBytes
		}
		if rescueEnabled && dir == "" {
			dir = filepath.Join(roots[0], rescueDirName)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				cleanup()
				return status.Errorf(codes.Internal, "create rescue dir: %v", err)
			}
		}
		// 未收到授权就出现 session/数据，一律拒绝（fail-closed）。
		if (msg.SessionId != "" || len(msg.Data) > 0) && !rescueEnabled {
			cleanup()
			return status.Error(codes.PermissionDenied,
				"binlog rescue apply is not authorized for this node (meta_data.rescue_enabled)")
		}

		if msg.SessionId != "" {
			if !sessionIDRe.MatchString(msg.SessionId) {
				cleanup()
				return status.Errorf(codes.InvalidArgument, "invalid session_id %q", msg.SessionId)
			}
			if f == nil {
				if err := openFragment(msg.SessionId); err != nil {
					cleanup()
					return status.Errorf(codes.Internal, "%v", err)
				}
			}
		}

		if len(msg.Data) > 0 {
			if f == nil {
				sid, err := randomSessionID()
				if err != nil {
					cleanup()
					return status.Errorf(codes.Internal, "%v", err)
				}
				if err := openFragment(sid); err != nil {
					cleanup()
					return status.Errorf(codes.Internal, "%v", err)
				}
			}
			loaded += int64(len(msg.Data))
			limit := maxBytes
			if limit <= 0 {
				limit = defaultTailMaxBytes
			}
			if loaded > limit {
				cleanup()
				return status.Errorf(codes.ResourceExhausted, "rescue fragment exceeded max_bytes %d", limit)
			}
			if _, err := f.Write(msg.Data); err != nil {
				cleanup()
				return status.Errorf(codes.Internal, "write fragment: %v", err)
			}
		}

		if msg.Done {
			targetGTID = msg.TargetGtid
			break
		}
	}

	if f == nil || loaded == 0 {
		cleanup()
		return status.Error(codes.InvalidArgument, "rescue fragment is empty")
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return status.Errorf(codes.Internal, "fsync fragment: %v", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return status.Errorf(codes.Internal, "close fragment: %v", err)
	}
	f = nil

	log.Printf("[audit] binlog-rescue-apply from=%s fragment=%s bytes=%d targetGTID=%s",
		peerAddr(ctx), tmpPath, loaded, targetGTID)

	// 通过 gtid_sync 插件重放：离线解析 + SET gtid_next +
	// 应用后 GTID 校验，全部以纯 Go 实现。
	plugin := plugins.Get("gtid_sync")
	if plugin == nil {
		cleanup()
		return status.Error(codes.FailedPrecondition, "gtid_sync plugin not available")
	}
	params := s.instanceParams(map[string]string{
		"fragment_path": tmpPath,
		"target_gtid":   targetGTID,
	})

	result, pErr := plugin.Execute(ctx, rescueActionReplay, params)
	if pErr != nil {
		log.Printf("[BinlogRescue] replay failed: %v", pErr)
		cleanup()
		return status.Errorf(codes.Aborted, "rescue replay failed: %v", pErr)
	}

	executed := result["gtid_executed"]
	cleanup()

	if err := srv.SendAndClose(&pb.BinlogRescueResult{
		Success:      true,
		Message:      "rescue binlog applied and verified",
		GtidExecuted: executed,
	}); err != nil {
		return status.Errorf(codes.Internal, "send rescue result: %v", err)
	}
	return nil
}

// randomSessionID 返回一个不可预测的分片会话标识符。
func randomSessionID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}
	return "rescue-" + hex.EncodeToString(b), nil
}

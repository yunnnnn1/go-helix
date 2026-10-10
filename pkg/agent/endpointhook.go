// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

const (
	// endpointHookDefaultTimeout 在请求未指定超限时，为同步+执行设定的
	// 时间上限。
	endpointHookDefaultTimeout = 15 * time.Second
	// endpointHookMaxSyncBytes 为待同步的 hook 可执行文件设定的大小上限。
	endpointHookMaxSyncBytes int64 = 64 << 20 // 64 MiB
	// endpointHookPathEnv 是赋予降权后进程的最小 PATH；hook 不会继承
	// agent 的环境变量。
	endpointHookPathEnv = "/usr/local/bin:/usr/bin:/bin"
)

// hookResultJSON 是 hook 最后一行 stdout 上必须给出的结构化返回内容
// （见 SPEC「Hook 返回契约」）。
type hookResultJSON struct {
	Command       string         `json:"command"`
	Success       bool           `json:"success"`
	Cluster       string         `json:"cluster"`
	EndpointType  string         `json:"endpoint_type"`
	Endpoint      string         `json:"endpoint"`
	CurrentMaster string         `json:"current_master"`
	Message       string         `json:"message"`
	Details       map[string]any `json:"details"`
	Timestamp     string         `json:"timestamp"`
}

// RunEndpointHook 按需同步最新的 hook 可执行文件，然后以配置的非 root
// 用户身份、按固定的 argv 协议运行它，并解析结构化的 JSON 结果。
func (s *Server) RunEndpointHook(ctx context.Context, req *pb.EndpointHookRequest) (*pb.EndpointHookResult, error) {
	cfg := config.Get()
	hooks := cfg.Agent.EndpointHooks

	// 1. 根据本地配置解析该集群对应的可执行文件。
	rawPath, ok := hooks.Lookup(req.Cluster)
	if !ok {
		return hookErrorResult("no endpoint hook configured for cluster %s in agent.endpoint_hooks", req.Cluster), nil
	}
	hookPath, err := validateHookPath(hooks.Roots, rawPath)
	if err != nil {
		return hookErrorResult("%v", err), nil
	}

	// 2. 校验命令及基础输入。
	switch req.Command {
	case "status", "stop", "start":
	default:
		return hookErrorResult("invalid hook command %q (want status|stop|start)", req.Command), nil
	}
	timeout := endpointHookDefaultTimeout
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	// 3. 按需同步最新的可执行文件。
	if req.SyncUrl != "" {
		if err := syncHookExecutable(ctx, req, hookPath, timeout); err != nil {
			return hookErrorResult("sync hook: %v", err), nil
		}
	}

	// 4. 解析降权运行所用的用户（必须存在）。
	if strings.TrimSpace(hooks.RunUser) == "" {
		return hookErrorResult("agent.endpoint_hooks.run_user not configured"), nil
	}
	u, err := user.Lookup(hooks.RunUser)
	if err != nil {
		return hookErrorResult("hook run user %q: %v", hooks.RunUser, err), nil
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)

	// 5. 可执行文件必须存在，且可读/可执行。
	if info, statErr := os.Stat(hookPath); statErr != nil {
		return hookErrorResult("hook executable not found: %s", hookPath), nil
	} else if info.IsDir() {
		return hookErrorResult("hook path is a directory: %s", hookPath), nil
	}

	// 6. 构造 argv（固定协议；值通过参数传入，不使用 env/stdin）。
	args := []string{
		"--command", req.Command,
		"--cluster", req.Cluster,
		"--endpoint-type", req.EndpointType,
		"--endpoint", req.Endpoint,
		"--old-master", req.OldMaster,
		"--new-master", req.NewMaster,
	}

	// 7. 在受时限约束的上下文中降权运行。
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, hookPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)},
	}
	// 最小且显式的环境变量；HOME 取自运行用户。
	cmd.Env = []string{
		"PATH=" + endpointHookPathEnv,
		"HOME=" + u.HomeDir,
	}
	cmd.Dir = filepath.Dir(hookPath)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	exitCode := 0
	if runErr != nil {
		if exitError, ok := runErr.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = -1
		}
	}

	res := &pb.EndpointHookResult{
		ExitCode: int32(exitCode),
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
	}

	// 8. 将最后一行 stdout 解析为结构化结果。
	parsed, pErr := parseHookResult(stdout.String())
	if pErr != nil {
		res.Success = false
		res.Message = "invalid hook return: " + pErr.Error()
		log.Printf("[EndpointHook] cluster=%s command=%s invalid return: %v", req.Cluster, req.Command, pErr)
		return res, nil
	}

	// 成功需同时满足：退出码为 0、hook 自报成功，且其回显的
	// command/cluster 与本次请求一致。
	res.CurrentMaster = parsed.CurrentMaster
	res.Message = parsed.Message
	res.Success = exitCode == 0 && parsed.Success &&
		parsed.Command == req.Command && parsed.Cluster == req.Cluster

	log.Printf("[EndpointHook] cluster=%s command=%s success=%v current_master=%s",
		req.Cluster, req.Command, res.Success, res.CurrentMaster)
	return res, nil
}

// validateHookPath 清理路径，并确保其位于某个允许的根目录之内。
func validateHookPath(roots []string, path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("empty hook path")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	for _, r := range roots {
		ar, err := filepath.Abs(filepath.Clean(r))
		if err != nil {
			continue
		}
		if abs == ar || strings.HasPrefix(abs, ar+string(os.PathSeparator)) {
			return abs, nil
		}
	}
	return "", fmt.Errorf("hook path %q is outside agent.endpoint_hooks.roots", abs)
}

// parseHookResult 提取并解码最后一条非空的 stdout 行。
func parseHookResult(stdout string) (*hookResultJSON, error) {
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	last := ""
	for i := len(lines) - 1; i >= 0; i-- {
		if t := strings.TrimSpace(lines[i]); t != "" {
			last = t
			break
		}
	}
	if last == "" || !strings.HasPrefix(last, "{") {
		return nil, fmt.Errorf("last stdout line is not a JSON object")
	}
	var r hookResultJSON
	if err := json.Unmarshal([]byte(last), &r); err != nil {
		return nil, fmt.Errorf("decode result JSON: %w", err)
	}
	return &r, nil
}

// syncHookExecutable 将 hook 下载到同一目录下的临时文件，校验
// 大小/sha256，然后以原子方式将其重命名到 hook 路径。
// 属主不变；文件权限设置为 0755。
func syncHookExecutable(ctx context.Context, req *pb.EndpointHookRequest, dest string, timeout time.Duration) error {
	dlCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(dlCtx, http.MethodGet, req.SyncUrl, nil)
	if err != nil {
		return err
	}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy:       http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		},
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("request source: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("source returned HTTP %d", resp.StatusCode)
	}

	dir := filepath.Dir(dest)
	tmp, err := os.CreateTemp(dir, ".hook.tmp-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}

	hasher := sha256.New()
	var total int64
	buf := make([]byte, 256*1024)
	for {
		n, rErr := resp.Body.Read(buf)
		if n > 0 {
			total += int64(n)
			if total > endpointHookMaxSyncBytes {
				cleanup()
				return fmt.Errorf("hook size exceeds cap %d bytes", endpointHookMaxSyncBytes)
			}
			if _, wErr := tmp.Write(buf[:n]); wErr != nil {
				cleanup()
				return fmt.Errorf("write temp: %w", wErr)
			}
			hasher.Write(buf[:n])
		}
		if rErr != nil {
			if rErr == io.EOF {
				break
			}
			cleanup()
			return fmt.Errorf("read source: %w", rErr)
		}
	}

	if req.Sha256 != "" {
		got := hex.EncodeToString(hasher.Sum(nil))
		if !strings.EqualFold(got, req.Sha256) {
			cleanup()
			return fmt.Errorf("sha256 mismatch: got %s want %s", got, req.Sha256)
		}
	}

	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp: %w", err)
	}

	// 原子替换；保留属主（由 agent 用户写入），设置 0755，
	// 使运行用户可读/可执行但不可修改。
	if err := os.Chmod(tmpName, 0o755); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("finalize: %w", err)
	}
	log.Printf("[EndpointHook] synced %d bytes from %s -> %s", total, req.SyncUrl, dest)
	return nil
}

// hookErrorResult 构造一个结构化的失败结果。
func hookErrorResult(format string, args ...any) *pb.EndpointHookResult {
	return &pb.EndpointHookResult{
		Success: false,
		Message: fmt.Sprintf(format, args...),
	}
}

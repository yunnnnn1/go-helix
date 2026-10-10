// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

const (
	// defaultFetchMaxBytes 在请求未设置上限时采用的默认值。
	defaultFetchMaxBytes int64 = 1 << 30 // 1 GiB
	// fetchProgressInterval 控制阶段性进度更新的频率。
	fetchProgressInterval = 500 * time.Millisecond
)

// FetchFile 安全地将单个文件下载到受沙箱约束的目标目录。
func (s *Server) FetchFile(req *pb.FetchFileRequest, srv pb.DBAgent_FetchFileServer) error {
	ctx := srv.Context()

	roots := []string{}
	if cfg := config.Get(); cfg != nil {
		roots = cfg.Agent.FileFetchRoots
	}

	dir, finalPath, err := resolveJailedPath(roots, req.TargetDir, req.Filename)
	if err != nil {
		return sendFetchError(srv, err)
	}

	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultFetchMaxBytes
	}

	if err := validateSourceURL(ctx, req.SourceUrl); err != nil {
		return sendFetchError(srv, err)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return sendFetchError(srv, fmt.Errorf("create target dir: %w", err))
	}

	log.Printf("[audit] fetch-file from=%s url=%s target=%s", peerAddr(ctx), req.SourceUrl, finalPath)

	// HTTP 客户端。TLS 校验保持启用；流的 context 约束整个请求
	// （客户端断连即取消）。
	client := &http.Client{
		Transport: &http.Transport{
			Proxy:       http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		},
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.SourceUrl, nil)
	if err != nil {
		return sendFetchError(srv, err)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return sendFetchError(srv, fmt.Errorf("request source: %w", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return sendFetchError(srv, fmt.Errorf("source returned HTTP %d", resp.StatusCode))
	}

	var total int64 = -1
	if resp.ContentLength >= 0 {
		total = resp.ContentLength
		if total > maxBytes {
			return sendFetchError(srv, fmt.Errorf("file size %d exceeds cap %d", total, maxBytes))
		}
	}

	// 临时文件与目标位于同一目录，因此在同一文件系统上最终重命名是原子的。
	tmp, err := os.CreateTemp(dir, "."+req.Filename+".tmp-*")
	if err != nil {
		return sendFetchError(srv, fmt.Errorf("create temp file: %w", err))
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		_ = os.Remove(tmpName)
	}

	// 阶段性进度（单一发送 goroutine；在最终发送前停止）。
	var loaded int64
	progDone := make(chan struct{})
	var progWg sync.WaitGroup
	progWg.Add(1)
	go func() {
		defer progWg.Done()
		ticker := time.NewTicker(fetchProgressInterval)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-progDone:
				return
			case <-ticker.C:
				cur := atomic.LoadInt64(&loaded)
				speed := int64(float64(cur-last) / fetchProgressInterval.Seconds())
				last = cur
				_ = srv.Send(&pb.FetchProgress{
					Loaded: cur, Total: total, SpeedBps: speed, Phase: "downloading",
				})
			}
		}
	}()

	// 响应体：可选限流 + 大小封顶，并并发计算哈希。
	var source io.Reader = resp.Body
	if req.LimitKbps > 0 {
		source = &rateLimitedReader{r: source, bytesPerSecond: req.LimitKbps * 1024 / 8}
	}
	hasher := sha256.New()
	source = io.TeeReader(source, hasher)

	buf := make([]byte, 256*1024)
	for {
		n, rErr := source.Read(buf)
		if n > 0 {
			if atomic.AddInt64(&loaded, int64(n)) > maxBytes {
				close(progDone)
				progWg.Wait()
				cleanup()
				return sendFetchError(srv, fmt.Errorf("download exceeded cap %d bytes", maxBytes))
			}
			if _, wErr := tmp.Write(buf[:n]); wErr != nil {
				close(progDone)
				progWg.Wait()
				cleanup()
				return sendFetchError(srv, fmt.Errorf("write temp: %w", wErr))
			}
		}
		if rErr != nil {
			if rErr == io.EOF {
				break
			}
			close(progDone)
			progWg.Wait()
			cleanup()
			return sendFetchError(srv, fmt.Errorf("read source: %w", rErr))
		}
	}
	close(progDone)
	progWg.Wait()

	// 在文件对外可见之前校验 SHA256。
	if req.Sha256 != "" {
		got := hex.EncodeToString(hasher.Sum(nil))
		if !strings.EqualFold(got, req.Sha256) {
			cleanup()
			return sendFetchError(srv, fmt.Errorf("sha256 mismatch: got %s, want %s", got, req.Sha256))
		}
	}

	if err := tmp.Sync(); err != nil {
		cleanup()
		return sendFetchError(srv, fmt.Errorf("fsync: %w", err))
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return sendFetchError(srv, fmt.Errorf("close temp: %w", err))
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		_ = os.Remove(tmpName)
		return sendFetchError(srv, fmt.Errorf("finalize: %w", err))
	}

	return srv.Send(&pb.FetchProgress{
		Loaded: atomic.LoadInt64(&loaded), Total: total, Phase: "done",
		Done: true, Message: finalPath,
	})
}

// resolveJailedPath 校验文件名，并确保 targetDir 位于某个允许的根目录
// 之内。它返回绝对目录以及最终文件路径。
func resolveJailedPath(roots []string, targetDir, filename string) (string, string, error) {
	if len(roots) == 0 {
		return "", "", fmt.Errorf("file fetch disabled: agent.file_fetch_roots is not configured")
	}
	if filename == "" || strings.ContainsAny(filename, `/\`) || filename == "." || filename == ".." {
		return "", "", fmt.Errorf("invalid filename %q", filename)
	}
	if filepath.Base(filename) != filename {
		return "", "", fmt.Errorf("filename must not contain path elements: %q", filename)
	}

	absTarget, err := filepath.Abs(filepath.Clean(targetDir))
	if err != nil {
		return "", "", err
	}

	allowed := false
	for _, r := range roots {
		ar, err := filepath.Abs(filepath.Clean(r))
		if err != nil {
			continue
		}
		if absTarget == ar || strings.HasPrefix(absTarget, ar+string(os.PathSeparator)) {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", "", fmt.Errorf("target_dir %q is outside the allowed fetch roots", absTarget)
	}

	return absTarget, filepath.Join(absTarget, filename), nil
}

// validateSourceURL 强制实施协议/SSRF 策略：
//   - 允许对任意主机使用 https；
//   - 仅允许对回环/私有地址使用 http（内部软件源）；
//   - 云元数据的链路本地（link-local）端点一律阻止。
func validateSourceURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid source url: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("source url missing host")
	}

	ips := []net.IP{}
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return fmt.Errorf("resolve source host: %w", err)
		}
		for _, a := range resolved {
			ips = append(ips, a.IP)
		}
	}

	for _, ip := range ips {
		if isCloudMetadataIP(ip) {
			return fmt.Errorf("blocked source: cloud metadata address %s", ip)
		}
		switch u.Scheme {
		case "https":
			// ok
		case "http":
			if !isPrivateOrLoopback(ip) {
				return fmt.Errorf("http source must be loopback/private, got %s", ip)
			}
		default:
			return fmt.Errorf("unsupported source scheme %q (use http/https)", u.Scheme)
		}
	}
	return nil
}

func isCloudMetadataIP(ip net.IP) bool {
	// 链路本地 169.254.0.0/16 承载着云元数据服务（AWS/GCP/Azure）。
	ip4 := ip.To4()
	return ip4 != nil && ip4[0] == 169 && ip4[1] == 254
}

func isPrivateOrLoopback(ip net.IP) bool {
	if ip.IsLoopback() {
		return true
	}
	ip4 := ip.To4()
	if ip4 == nil {
		// IPv6 ULA fc00::/7
		return ip.IsPrivate()
	}
	return ip4.IsPrivate()
}

// rateLimitedReader 将读取速率节流到平均每秒字节数。
type rateLimitedReader struct {
	r              io.Reader
	bytesPerSecond int64
}

func (rr *rateLimitedReader) Read(p []byte) (int, error) {
	if rr.bytesPerSecond <= 0 {
		return rr.r.Read(p)
	}
	// 将每次读取限制为至多 100ms 对应的字节数，以避免突发流量过大。
	maxChunk := rr.bytesPerSecond / 10
	if maxChunk <= 0 {
		maxChunk = 1
	}
	if int64(len(p)) > maxChunk {
		p = p[:maxChunk]
	}
	n, err := rr.r.Read(p)
	if n > 0 {
		time.Sleep(time.Duration(n) * time.Second / time.Duration(rr.bytesPerSecond))
	}
	return n, err
}

func sendFetchError(srv pb.DBAgent_FetchFileServer, err error) error {
	log.Printf("[FetchFile] %v", err)
	return srv.Send(&pb.FetchProgress{Phase: "error", Message: err.Error()})
}

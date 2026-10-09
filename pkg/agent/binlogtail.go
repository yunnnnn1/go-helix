package agent

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// defaultTailMaxBytes 在配置和请求都未设置上限时，为 tail 流设定的
	// 字节数边界。
	defaultTailMaxBytes int64 = 1 << 30 // 1 GiB
	// tailChunkSize 是单个 BinlogChunk 的最大载荷。
	tailChunkSize = 1 << 20 // 1 MiB
	// binlogMagicSize 是二进制日志文件 4 字节的文件头（"\xfebin"）。
	binlogMagicSize = 4
	// binlogEventHeaderSize 是事件头的固定长度。
	binlogEventHeaderSize = 19
)

// StreamBinlogTail 流式发送比请求起始位置更新的原始二进制日志字节。
// 输出是一个拼接起来的 MHA 风格分片：起始文件的 magic 及其
// Format Description Event 会被保留，后续文件只贡献各自 FDE 之后的
// 字节，因此该分片可通过单次 mysqlbinlog 调用完成应用。
func (s *Server) StreamBinlogTail(req *pb.BinlogTailRequest, srv pb.DBAgent_StreamBinlogTailServer) error {
	ctx := srv.Context()

	// binlog 救援配置不再读本地 config 文件，全部由 Manager 从节点表
	// meta_data 的 rescue_* 字段读取并随请求下发。未启用即拒绝（fail-closed）。
	if !req.Enabled {
		return status.Error(codes.PermissionDenied, "binlog rescue is disabled for this node (meta_data.rescue_enabled)")
	}

	base, err := s.resolveBinlogBasename(ctx, req.BinlogDir, req.BinlogBasename)
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	files, err := readBinlogIndex(base + ".index")
	if err != nil {
		return status.Errorf(codes.Internal, "read binlog index: %v", err)
	}

	// 读取沙箱约束：当 Manager 下发了访问根目录时，二进制日志目录必须
	// 解析到其中某个根目录之内。
	if len(req.AccessRoots) > 0 {
		dir := filepath.Dir(base)
		if !insideAnyRoot(dir, req.AccessRoots) {
			return status.Errorf(codes.PermissionDenied, "binlog dir %q outside access roots", dir)
		}
	}

	startIdx := 0
	if req.StartFile != "" {
		startIdx = indexOfBinlog(files, filepath.Base(req.StartFile))
		if startIdx < 0 {
			return status.Errorf(codes.FailedPrecondition,
				"start file %s not found among local binlogs (purged? oldest=%s newest=%s)",
				req.StartFile, filepath.Base(files[0]), filepath.Base(files[len(files)-1]))
		}
	}

	// 载荷上限来自请求（Manager 用节点级 max_bytes 填充）；0 取内置默认。
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultTailMaxBytes
	}

	log.Printf("[audit] binlog-tail from=%s startFile=%s startPos=%d tailFiles=%d maxBytes=%d",
		peerAddr(ctx), req.StartFile, req.StartPos, len(files)-startIdx, maxBytes)

	var loaded int64
	// sendRange 以分块大小的片段流式发送单个文件的一段字节区间。
	sendRange := func(file string, from, to int64) error {
		if from >= to {
			return nil
		}
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("open %s: %w", file, err)
		}
		defer f.Close()
		if _, err := f.Seek(from, io.SeekStart); err != nil {
			return fmt.Errorf("seek %s:%d: %w", file, from, err)
		}

		buf := make([]byte, tailChunkSize)
		for from < to {
			want := int64(len(buf))
			if to-from < want {
				want = to - from
			}
			n, rErr := f.Read(buf[:want])
			if n > 0 {
				loaded += int64(n)
				if loaded > maxBytes {
					return fmt.Errorf("tail payload exceeded max_bytes %d", maxBytes)
				}
				// 复制一份：gRPC 不得持有我们可复用的缓冲区。
				payload := make([]byte, n)
				copy(payload, buf[:n])
				from += int64(n)
				if sErr := srv.Send(&pb.BinlogChunk{
					Data: payload,
					File: filepath.Base(file),
					Pos:  from,
				}); sErr != nil {
					return fmt.Errorf("send binlog chunk: %w", sErr)
				}
			}
			if rErr != nil && rErr != io.EOF {
				return fmt.Errorf("read %s: %w", file, rErr)
			}
			if rErr == io.EOF {
				break
			}
		}
		return nil
	}

	for i := startIdx; i < len(files); i++ {
		file := files[i]
		size, err := fileSizeOf(file)
		if err != nil {
			return status.Errorf(codes.Internal, "stat binlog %s: %v", file, err)
		}
		fdeEnd, err := fdeEndOffset(file)
		if err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}

		if i == startIdx {
			// 起始文件的 Magic + FDE，然后是请求起始位置处及其之后的
			// 全部内容（或紧接 FDE 之后的内容）。
			if err := sendRange(file, 0, fdeEnd); err != nil {
				return status.Errorf(codes.Internal, "%v", err)
			}
			cut := req.StartPos
			if cut < fdeEnd {
				cut = fdeEnd
			}
			if err := sendRange(file, cut, size); err != nil {
				return status.Errorf(codes.Internal, "%v", err)
			}
		} else {
			// 后续文件：分片已包含一个 FDE；跳过本地 FDE，流式发送
			// 其余部分。
			if err := sendRange(file, fdeEnd, size); err != nil {
				return status.Errorf(codes.Internal, "%v", err)
			}
		}
	}

	if err := srv.Send(&pb.BinlogChunk{Done: true}); err != nil {
		return status.Errorf(codes.Internal, "send tail done: %v", err)
	}
	log.Printf("[BinlogTail] completed, streamed %d byte(s)", loaded)
	return nil
}

// resolveBinlogBasename 查找二进制日志路径前缀（不含数字后缀）。
// 它首先向在线数据库查询 @@log_bin_basename；当数据库已停服时，
// 回退到 Manager 下发的静态二进制日志目录/基础名。
func (s *Server) resolveBinlogBasename(ctx context.Context, binlogDir, binlogBaseName string) (string, error) {
	addr := net.JoinHostPort(s.dbHost, strconv.Itoa(s.dbPort))
	user, pass := s.mgrUser, s.mgrPass
	if user == "" {
		user, pass = s.dbUser, s.dbPass
	}

	if db, gErr := plugins.DefaultPool().Get(ctx, "mysql", addr, user, pass); gErr == nil {
		var base string
		qErr := db.QueryRowContext(ctx, "SELECT @@global.log_bin_basename").Scan(&base)
		if qErr == nil && base != "" {
			log.Printf("[BinlogTail] resolved live log_bin_basename=%s", base)
			return base, nil
		}
		log.Printf("[BinlogTail] cannot read @@log_bin_basename (%v), trying static fallback", qErr)
	} else {
		log.Printf("[BinlogTail] cannot connect local DB at %s (%v), trying static fallback", addr, gErr)
	}

	if binlogDir != "" && binlogBaseName != "" {
		base := filepath.Join(binlogDir, binlogBaseName)
		log.Printf("[BinlogTail] using static fallback basename=%s", base)
		return base, nil
	}
	return "", fmt.Errorf("cannot resolve binlog location: DB unreadable and meta_data.binlog_dir/binlog_basename not reported")
}

// readBinlogIndex 将二进制日志 .index 文件解析为有序的文件路径列表。
// 相对路径条目会以 index 文件自身所在目录为基准进行解析。
func readBinlogIndex(indexPath string) ([]string, error) {
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", indexPath, err)
	}
	defer f.Close()

	var files []string
	dir := filepath.Dir(indexPath)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if !filepath.IsAbs(line) {
			line = filepath.Join(dir, line)
		}
		files = append(files, filepath.Clean(line))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", indexPath, err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("binlog index %s lists no files", indexPath)
	}
	return files, nil
}

// indexOfBinlog 返回基础名与 name 匹配的二进制日志文件的位置，
// 未找到则返回 -1。
func indexOfBinlog(files []string, name string) int {
	for i, f := range files {
		if filepath.Base(f) == name {
			return i
		}
	}
	return -1
}

// fdeEndOffset 返回二进制日志文件 Format Description Event 的结束
// 偏移量：FDE 是第一个事件，其固定 19 字节的事件头从偏移量 4 开始。
func fdeEndOffset(file string) (int64, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", file, err)
	}
	defer f.Close()

	var hdr [binlogEventHeaderSize]byte
	n, err := f.ReadAt(hdr[:], binlogMagicSize)
	if err != nil || n != len(hdr) {
		return 0, fmt.Errorf("read FDE header of %s: %w", file, err)
	}
	// event_size 是位于事件头偏移量 9 处的 uint32 小端值。
	eventSize := binary.LittleEndian.Uint32(hdr[9:13])
	if eventSize < binlogEventHeaderSize {
		return 0, fmt.Errorf("invalid FDE event_size %d in %s", eventSize, file)
	}
	return int64(binlogMagicSize) + int64(eventSize), nil
}

// fileSizeOf 返回文件当前的大小。
func fileSizeOf(file string) (int64, error) {
	st, err := os.Stat(file)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// insideAnyRoot 报告 dir 是否解析到某个根目录本身或其下的某条路径。
func insideAnyRoot(dir string, roots []string) bool {
	abs, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		return false
	}
	for _, r := range roots {
		ar, err := filepath.Abs(filepath.Clean(r))
		if err != nil {
			continue
		}
		if abs == ar || strings.HasPrefix(abs, ar+string(os.PathSeparator)) {
			return true
		}
	}
	return false
}

package agent

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/agent/backend"
	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	"github.com/yunnnnn1/go-helix/pkg/config"
	log "github.com/yunnnnn1/go-helix/pkg/logging"

	"google.golang.org/grpc/peer"

	// 注册 Backends
	_ "github.com/yunnnnn1/go-helix/pkg/agent/backend/mysql"
	// 注册 Plugins
	_ "github.com/yunnnnn1/go-helix/pkg/agent/plugins/gtidsync"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

type Server struct {
	pb.UnimplementedDBAgentServer
	backend backend.Backend

	// 本 Server 所代理的具体数据库实例的标识；仅用于在进程内向插件请求
	// 注入本地连接信息/凭据。
	dbHost string
	dbPort int
	dbUser string // 探测账户（app_user）
	dbPass string

	// 三个相互隔离的账户（见 SPEC.md「账户与权限」）：
	replUser string // repl：仅用于复制，绝不用于客户端连接
	replPass string
	mgrUser  string // manager：故障切换/主从切换及 GTID 操作
	mgrPass  string
}

func NewServer(dbType, host string, port int, appUser, appPass, replUser, replPass, mgrUser, mgrPass, cluster string) (*Server, error) {
	b, err := backend.GetBackend(dbType)
	if err != nil {
		return nil, err
	}

	// 先用集群名标记 backend，使后续每一行日志（包括连接建立过程）
	// 都能显示它属于哪个集群。
	b.SetCluster(cluster)

	// 探测连接（app_user）：用于健康检查/状态查询。失败并非致命：
	// 启动时数据库可能恰好暂时不可用。
	if err := b.Configure(host, port, appUser, appPass); err != nil {
		log.Printf("Warning: probe account setup failed: %v", err)
	}

	// 复制账户（repl）：仅为 CHANGE REPLICATION SOURCE 而保存。
	if replUser != "" {
		if err := b.SetReplicationCredentials(replUser, replPass); err != nil {
			log.Printf("Warning: replication account setup failed: %v", err)
		}
	} else {
		log.Printf("Warning: replication account not configured for %s:%d; ChangeMaster will be rejected", host, port)
	}

	// 管理连接：故障切换/主从切换及 GTID 操作都需要它。
	if mgrUser != "" {
		if err := b.ConfigureManager(host, port, mgrUser, mgrPass); err != nil {
			return nil, fmt.Errorf("manager account setup: %w", err)
		}
	} else {
		log.Printf("Warning: manager account not configured for %s:%d; failover/switch operations will be rejected", host, port)
	}

	// 初始化所有插件。Init 保留给一次性的全局设置使用；插件不得通过它
	// 持有实例专属的连接——它们应按目标地址为键，从共享连接池中
	// 获取连接。
	cfg := map[string]string{
		"db_host": host,
		"db_port": strconv.Itoa(port),
		"db_user": appUser,
		"db_pass": appPass,
	}
	for name, p := range plugins.GetAll() {
		if err := p.Init(context.Background(), cfg); err != nil {
			log.Printf("Warning: Failed to init plugin %s: %v", name, err)
		} else {
			log.Printf("Plugin %s loaded successfully", name)
		}
	}

	return &Server{
		backend:  b,
		dbHost:   host,
		dbPort:   port,
		dbUser:   appUser,
		dbPass:   appPass,
		replUser: replUser,
		replPass: replPass,
		mgrUser:  mgrUser,
		mgrPass:  mgrPass,
	}, nil
}

func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	alive, err := s.backend.Ping(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.PingResponse{Alive: alive}, nil
}

func (s *Server) KillProcess(ctx context.Context, req *pb.KillProcessRequest) (*pb.KillProcessResponse, error) {
	err := s.backend.KillProcess(ctx, req.ProcessId, req.IsOsPid)
	if err != nil {
		return &pb.KillProcessResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.KillProcessResponse{Success: true, Message: "Process killed"}, nil
}

func (s *Server) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	status, err := s.backend.GetStatus(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.GetStatusResponse{Status: status}, nil
}

// peerAddr 从 gRPC 上下文中提取已认证调用方的 TCP 地址。
// 它仅用于对特权操作进行审计日志记录。
func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

func (s *Server) ExecuteOperation(ctx context.Context, req *pb.OperationRequest) (*pb.OperationResponse, error) {
	log.Printf("[audit] op=%s from=%s params=%v", req.Operation, peerAddr(ctx), req.Params)
	result, err := s.backend.ExecuteOperation(ctx, req.Operation, req.Params)
	if err != nil {
		// 将业务错误作为 gRPC 错误返回，以便调用方的错误判断
		// （WaitCatchup 超时、提升步骤、主从切换回滚）能够真正触发，
		// 而不是收到一个 error 为 nil、Success=false 的响应。
		return nil, err
	}
	return &pb.OperationResponse{Success: true, Result: result}, nil
}

func (s *Server) ExecutePlugin(ctx context.Context, req *pb.PluginRequest) (*pb.PluginResponse, error) {
	log.Printf("[audit] plugin=%s action=%s from=%s", req.PluginName, req.Action, peerAddr(ctx))
	p := plugins.Get(req.PluginName)
	if p == nil {
		return &pb.PluginResponse{Success: false, Message: fmt.Sprintf("plugin %s not found", req.PluginName)}, nil
	}

	// 身份/凭据始终由 Server 注入（唯一权威来源），绝不取自网络请求。
	params := s.instanceParams(req.Params)

	result, err := p.Execute(ctx, req.Action, params)
	if err != nil {
		return &pb.PluginResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.PluginResponse{Success: true, Message: "success", Result: result}, nil
}

// instanceParams 返回 base 的副本，并在进程内注入本实例的身份以及
// 探测/管理凭据。这些值绝不会通过网络传输，也绝不能被记入日志。
// 它是为插件执行添加凭据的唯一位置，由 ExecutePlugin 和
// ApplyBinlogRescue 共用。
func (s *Server) instanceParams(base map[string]string) map[string]string {
	params := make(map[string]string, len(base)+8)
	for k, v := range base {
		params[k] = v
	}
	params["db_host"] = s.dbHost
	params["db_port"] = strconv.Itoa(s.dbPort)
	params["db_user"] = s.dbUser
	params["db_pass"] = s.dbPass
	if s.mgrUser != "" {
		params["manager_user"] = s.mgrUser
		params["manager_pass"] = s.mgrPass
	}
	return params
}

func (s *Server) ExecuteShell(ctx context.Context, req *pb.ShellRequest) (*pb.ShellResponse, error) {
	// 安全说明：默认不允许通过 RPC 执行任意 shell。
	// 若暴露给不受信任的管理端，这将构成远程命令执行漏洞。
	// 我们应只允许预定义命令，或对输入进行严格消毒。
	// 对于本 MHA 系统，agent 应执行特定的管理任务。
	// 我们将其限制为仅允许已放行的命令。
	// 安全说明：仅允许预定义命令（不允许任意 shell）。
	// 白名单/参数校验与流式的 ExecStream 共用。
	if !allowedShellCommands[req.Command] {
		return &pb.ShellResponse{
			Success:  false,
			Stderr:   "command not allowed for security reasons",
			ExitCode: -1,
		}, nil
	}

	// 执行前校验命令专属的参数。命令通过 exec 运行（不经 shell），
	// 但仍对 ping 的标志/目标加以限制，使其无法被用作内网扫描器。
	if err := validateShellArgs(req.Command, req.Args); err != nil {
		return &pb.ShellResponse{
			Success:  false,
			Stderr:   err.Error(),
			ExitCode: -1,
		}, nil
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

	// 捕获输出
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	exitCode := 0
	if err != nil {
		// 尝试获取退出码
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = -1 // 未知错误或被杀死
		}
	}

	return &pb.ShellResponse{
		Success:  err == nil,
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: int32(exitCode),
	}, nil
}

// pingAllowedFlags 列出唯一接受的 ping 标志；每个标志都要求带一个
// 数值参数。其他标志（如 -f 洪水 ping、广播）一律拒绝。
var pingAllowedFlags = map[string]bool{
	"-c": true, // 次数
	"-W": true, // 超时（Linux）
	"-i": true, // 间隔
	"-t": true, // TTL
}

// hostArgRe 匹配普通的 IPv4/主机名目标（不含 shell 元字符）。
var hostArgRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9.\-]*[a-zA-Z0-9])?$`)

// isValidHostArg 报告 s 是否是有效的单个 IP/主机名。
func isValidHostArg(s string) bool {
	if s == "" {
		return false
	}
	if net.ParseIP(s) != nil {
		return true
	}
	return hostArgRe.MatchString(s)
}

// validateShellArgs 实施按命令区分的参数限制。
func validateShellArgs(command string, args []string) error {
	switch command {
	case "ping":
		hosts := 0
		for i := 0; i < len(args); i++ {
			a := args[i]
			if strings.HasPrefix(a, "-") {
				if !pingAllowedFlags[a] {
					return fmt.Errorf("ping flag %q not allowed", a)
				}
				if i+1 >= len(args) {
					return fmt.Errorf("ping flag %q requires a numeric value", a)
				}
				if _, err := strconv.ParseFloat(args[i+1], 64); err != nil {
					return fmt.Errorf("ping flag %q value must be numeric, got %q", a, args[i+1])
				}
				i++ // 消费掉该值
				continue
			}
			if !isValidHostArg(a) {
				return fmt.Errorf("invalid ping target %q", a)
			}
			hosts++
		}
		if hosts > 1 {
			return fmt.Errorf("ping accepts exactly one target")
		}
	case "echo":
		if err := validateEchoArgs(args); err != nil {
			return err
		}
	}
	return nil
}

// echoAllowedFlags 是唯一接受的 echo 选项。特意不包含 `-e`/`-E`
// （转义解释），以防操作人员夹带转义序列；实践中只需要 `-n`
// （不输出结尾换行）。
var echoAllowedFlags = map[string]bool{
	"-n": true,
}

// echoSafeTextRe 只允许不含控制字符或 shell 元字符的普通可打印文本。
// echo 只能用于打印，绝不能作为任意/转义内容的透传通道。
var echoSafeTextRe = regexp.MustCompile(`^[A-Za-z0-9 _.\-:/=]+$`)

// validateEchoArgs 将 echo 限制为已知标志和安全的可打印文本。
func validateEchoArgs(args []string) error {
	const maxEchoArgs = 8
	if len(args) > maxEchoArgs {
		return fmt.Errorf("echo accepts at most %d arguments", maxEchoArgs)
	}
	for _, a := range args {
		if a == "" {
			return fmt.Errorf("echo argument must not be empty")
		}
		if strings.HasPrefix(a, "-") {
			if !echoAllowedFlags[a] {
				return fmt.Errorf("echo flag %q not allowed", a)
			}
			continue
		}
		if len(a) > 256 {
			return fmt.Errorf("echo text too long (max 256 chars)")
		}
		if !echoSafeTextRe.MatchString(a) {
			return fmt.Errorf("echo text contains unsupported characters: %q", a)
		}
	}
	return nil
}

// SSHCheck 尝试拨号远端主机的 SSH 端口，以判断其在网络层面是否可达
func (s *Server) SSHCheck(ctx context.Context, req *pb.SSHCheckRequest) (*pb.SSHCheckResponse, error) {
	port := req.TargetPort
	if port <= 0 {
		port = 22
	}
	addr := net.JoinHostPort(req.TargetHost, strconv.Itoa(int(port)))
	log.Printf("Performing SSH TCP check to %s", addr)

	// TCP 拨号超时
	timeout := 3 * time.Second
	if cfg := config.Get(); cfg != nil && cfg.Agent.Timeout != "" {
		if d, err := time.ParseDuration(cfg.Agent.Timeout); err == nil {
			timeout = d
		}
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return &pb.SSHCheckResponse{
			Reachable: false,
			Message:   fmt.Sprintf("failed to connect: %v", err),
		}, nil
	}
	conn.Close()

	return &pb.SSHCheckResponse{
		Reachable: true,
		Message:   "ssh port is open",
	}, nil
}

// GetDiskStatus 检查本地磁盘使用情况（使用 syscall）
func (s *Server) GetDiskStatus(ctx context.Context, req *pb.DiskStatusRequest) (*pb.DiskStatusResponse, error) {
	path := req.Path
	if path == "" {
		path = "/"
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return nil, fmt.Errorf("failed to check disk space: %v", err)
	}

	// 计算使用量
	all := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)

	if all == 0 {
		return nil, fmt.Errorf("total disk space is 0")
	}

	used := all - free
	usagePct := (float64(used) / float64(all)) * 100.0
	isFull := usagePct >= 95.0

	return &pb.DiskStatusResponse{
		UsagePercent: usagePct,
		IsFull:       isFull,
		Message:      fmt.Sprintf("Disk usage is at %.2f%%", usagePct),
	}, nil
}

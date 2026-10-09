package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/agent"
	"github.com/yunnnnn1/go-helix/pkg/agent/backend"
	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/db"
	_ "github.com/yunnnnn1/go-helix/pkg/db/drivers/mysql"  // 注册 MySQL 驱动
	_ "github.com/yunnnnn1/go-helix/pkg/db/drivers/sqlite" // 注册 SQLite 驱动与方言
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"

	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "config_agent.json", "Path to configuration file")
	flag.Parse()

	if err := config.Load(*configPath); err != nil {
		log.Printf("Warning: failed to load config file: %v. Using defaults.", err)
	}

	cfg := config.Get()
	if cfg == nil {
		log.Fatalf("Fatal: configuration is empty and could not be loaded.")
	}

	// 初始化结构化日志
	log.Init(cfg.Agent.LogDir, "agent")

	// 根据配置构建共享的插件连接池，并将其安装为进程级默认池。插件从该
	// 连接池中获取针对各目标的连接。
	poolCfg := cfg.Agent.PluginPool.WithDefaults()
	pluginPool, err := plugins.NewConnPool(poolCfg)
	if err != nil {
		log.Fatalf("Fatal: failed to build plugin connection pool: %v", err)
	}
	plugins.SetDefaultPool(pluginPool)

	poolCtx, poolCancel := context.WithCancel(context.Background())
	reclaimInterval, _ := time.ParseDuration(poolCfg.ReclaimInterval)
	reclaimAfter, _ := time.ParseDuration(poolCfg.ReclaimAfter)
	go pluginPool.StartReclaimer(poolCtx, reclaimInterval, reclaimAfter)

	// 确保退出时停止回收器并关闭每一个缓存的连接。
	defer func() {
		poolCancel()
		pluginPool.Close()
	}()

	instances := cfg.Agent.Instances
	if len(instances) == 0 {
		log.Println("No instances configured. Running in Auto-Discovery mode...")
		if cfg.Agent.DefaultDBUser == "" {
			log.Fatalf("Fatal: auto-discovery requires agent.default_db_user / default_db_password to be configured")
		}

		// 要发现的数据库类型；默认为 MySQL。
		autoType := cfg.Agent.AutoDiscoverType
		if autoType == "" {
			autoType = "mysql"
		}
		dialect, err := db.GetDialect(autoType)
		if err != nil {
			log.Fatalf("Fatal: cannot resolve dialect %q for auto-discovery: %v", autoType, err)
		}
		autoBackend, err := backend.GetBackend(autoType)
		if err == nil {
			discovered, err := autoBackend.Discover()
			if err != nil {
				log.Printf("Discovery failed: %v", err)
			} else {
				log.Printf("Discovered %d %s instances", len(discovered), autoType)
				reportIP := getOutboundIP()
				log.Printf("[Agent] Auto-discovery will report nodes under IP %s", reportIP)
				for _, inst := range discovered {
					instType := inst.Type
					if instType == "" {
						instType = autoType
					}
					agentPort := dialect.DeriveAgentPort(int32(inst.Port))
					instances = append(instances, config.AgentInstanceConfig{
						BindAddr:    "0.0.0.0:" + strconv.Itoa(int(agentPort)),
						ReportIP:    reportIP,
						ClusterName: "", // 由数据中心根据复制拓扑分配
						DB: struct {
							Type                string `json:"type"`
							Host                string `json:"host"`
							Port                int    `json:"port"`
							User                string `json:"user"`
							Password            string `json:"password"`
							ReplicationUser     string `json:"replication_user"`
							ReplicationPassword string `json:"replication_password"`
							ManagerUser         string `json:"manager_user"`
							ManagerPassword     string `json:"manager_password"`
						}{
							Type: instType,
							Host: inst.Host,
							Port: inst.Port,
						},
					})
				}
			}
		}
	}

	if len(instances) == 0 {
		log.Fatalf("Fatal: No instances configured and no instances discovered. Agent has nothing to do.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var grpcServers []*grpc.Server
	var serversMu sync.Mutex

	for _, instCfg := range instances {
		instCfg := instCfg // 捕获循环变量
		wg.Add(1)
		go func() {
			defer wg.Done()
			startInstance(ctx, cfg, instCfg, &grpcServers, &serversMu)
		}()
	}

	// 处理优雅关闭
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down agent and all instance servers...")
		cancel()
		serversMu.Lock()
		for _, s := range grpcServers {
			if s != nil {
				s.GracefulStop()
			}
		}
		serversMu.Unlock()
	}()

	wg.Wait()
	log.Println("Agent exited.")
}

func startInstance(ctx context.Context, globalCfg *config.Config, instCfg config.AgentInstanceConfig, servers *[]*grpc.Server, serversMu *sync.Mutex) {
	agentBind := instCfg.BindAddr
	if agentBind == "" {
		log.Printf("Skip instance: BindAddr is empty")
		return
	}

	dbType := instCfg.DB.Type
	if dbType == "" {
		log.Printf("Skip instance %s: Database Type is empty", agentBind)
		return
	}

	// 解析探测账号（app_user）：实例级配置覆盖 Agent 级配置
	probeUser := instCfg.DB.User
	probePass := instCfg.DB.Password
	if probeUser == "" {
		probeUser = globalCfg.Agent.DefaultDBUser
		probePass = globalCfg.Agent.DefaultDBPassword
	}

	// 解析复制账号（repl），覆盖顺序相同
	replUser := instCfg.DB.ReplicationUser
	replPass := instCfg.DB.ReplicationPassword
	if replUser == "" {
		replUser = globalCfg.Agent.ReplicationDBUser
		replPass = globalCfg.Agent.ReplicationDBPassword
	}
	if replUser == "" {
		log.Printf("Warning: replication account not configured for %s; ChangeMaster will fail", agentBind)
	}

	// 解析管理账号，覆盖顺序相同
	mgrUser := instCfg.DB.ManagerUser
	mgrPass := instCfg.DB.ManagerPassword
	if mgrUser == "" {
		mgrUser = globalCfg.Agent.ManagerDBUser
		mgrPass = globalCfg.Agent.ManagerDBPassword
	}
	if mgrUser == "" {
		log.Printf("Warning: manager account not configured for %s; failover/switch operations will fail", agentBind)
	}

	// 为该特定实例初始化数据库后端
	b, err := backend.GetBackend(dbType)
	if err != nil {
		log.Printf("Failed to initialize backend for %s: %v", agentBind, err)
		return
	}

	b.SetCluster(instCfg.ClusterName)

	if err := b.Configure(instCfg.DB.Host, instCfg.DB.Port, probeUser, probePass); err != nil {
		log.Printf("Warning: initial probe db connection failed for %s: %v", agentBind, err)
	}
	if replUser != "" {
		if err := b.SetReplicationCredentials(replUser, replPass); err != nil {
			log.Printf("Warning: set replication credentials failed for %s: %v", agentBind, err)
		}
	}
	if mgrUser != "" {
		if err := b.ConfigureManager(instCfg.DB.Host, instCfg.DB.Port, mgrUser, mgrPass); err != nil {
			log.Printf("Warning: initial management db connection failed for %s: %v", agentBind, err)
		}
	}

	// 启动心跳上报器（探测账号：仅用于 ping/状态查询）
	go startHeartbeat(ctx, dbType, agentBind, instCfg.DB.Host, instCfg.DB.Port, instCfg.ClusterName, probeUser, probePass, instCfg.ReportIP)

	// 启动 gRPC 服务器
	lis, err := net.Listen("tcp", agentBind)
	if err != nil {
		log.Printf("Failed to listen on %s: %v", agentBind, err)
		return
	}

	var opts []grpc.ServerOption
	opts = append(opts, grpc.UnaryInterceptor(authInterceptor(globalCfg)))

	s := grpc.NewServer(opts...)
	serversMu.Lock()
	*servers = append(*servers, s)
	serversMu.Unlock()

	agentServer, err := agent.NewServer(dbType, instCfg.DB.Host, instCfg.DB.Port, probeUser, probePass, replUser, replPass, mgrUser, mgrPass, instCfg.ClusterName)
	if err != nil {
		log.Printf("Failed to initialize agent server for %s: %v", agentBind, err)
		lis.Close()
		return
	}
	pb.RegisterDBAgentServer(s, agentServer)

	log.Printf("Agent instance for cluster [%s] listening at %v", instCfg.ClusterName, lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Printf("Failed to serve on %s: %v", agentBind, err)
	}
}

// getOutboundIP 返回主机的主要非环回 IPv4 地址。对 UDP 地址进行拨号并不会
// 真正发送数据包；它只是让操作系统选择本地出接口的地址，也就是其他节点
// 所看到的地址。
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("[Agent] Could not determine outbound IP: %v, falling back to 127.0.0.1", err)
		return "127.0.0.1"
	}
	defer conn.Close()
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return addr.IP.String()
	}
	return "127.0.0.1"
}

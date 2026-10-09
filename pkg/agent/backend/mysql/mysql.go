package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/agent/backend"
	"github.com/yunnnnn1/go-helix/pkg/db"
	log "github.com/yunnnnn1/go-helix/pkg/logging"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
)

type MySQLBackend struct {
	mu sync.RWMutex

	// readOnly 在无法读取实时变量时作为兜底值。
	readOnly bool

	// 被管理实例的标识，用于标记日志输出。单个 Agent 进程可以管理多个
	// 实例，因此每条日志都必须标明它所属的集群以及 host:port。
	cluster string
	host    string
	port    int

	// 业务账号（app_user）：仅用于健康探测/监控。
	db      *sql.DB
	version string

	// 重连退避。当数据库不可达或拒绝探测连接时，心跳绝不能在每个 tick
	// 都重建并 ping 一个新连接池：每次失败的握手都会累加服务端按主机统计的
	// 错误计数，最终触发 MySQL Error 1129（主机被屏蔽）。failedConns
	// 统计连续失败次数；nextRetry 是允许尝试重连的最早时间。
	failedConns int
	nextRetry   time.Time

	// 复制账号（repl）：仅用于主从复制。绝不会使用该账号建立客户端连接
	// 池；其凭据只会被写入 CHANGE REPLICATION SOURCE 语句，供副本 IO
	// 线程向源端进行认证。
	replUser string
	replPass string

	// 管理员账号：用于所有故障切换/主备切换操作以及 GTID 相关操作的专用
	// 连接池。在 ConfigureManager 成功之前 managerDB 为 nil。
	managerUser string
	managerPass string
	managerDB   *sql.DB
}

func init() {
	backend.Register("mysql", NewMySQLBackend())
}

func NewMySQLBackend() *MySQLBackend {
	return &MySQLBackend{
		readOnly: false,
	}
}

func (b *MySQLBackend) Clone() backend.Backend {
	return NewMySQLBackend()
}

// SetCluster 记录集群名，以便每条日志都能带上该标记。在自动发现模式下，
// 在数据中心分配集群名之前，它可能为空。
func (b *MySQLBackend) SetCluster(name string) {
	b.mu.Lock()
	b.cluster = name
	b.mu.Unlock()
}

// tag 返回用于标识被管理实例的日志前缀：集群已知时为
// "cluster host:port"，否则仅为 "host:port"。
func (b *MySQLBackend) tag() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	addr := fmt.Sprintf("%s:%d", b.host, b.port)
	if b.cluster != "" {
		return b.cluster + " " + addr
	}
	return addr
}

// printf 记录一条带有被管理实例集群和 host:port 标记的日志，从而可以
// 区分单个 Agent 进程中来自多个实例/集群的输出。
func (b *MySQLBackend) printf(format string, args ...interface{}) {
	log.Printf("[MySQL %s] "+format, append([]interface{}{b.tag()}, args...)...)
}

// openSQLDB 通过方言为该实例构建 database/sql 连接池。
func openSQLDB(host string, port int, user, pass string) (*sql.DB, error) {
	// DSN 的构造位于方言中；后端只保留 database/sql 层面的逻辑。
	dialect, err := db.GetDialect("mysql")
	if err != nil {
		return nil, fmt.Errorf("resolve mysql dialect: %w", err)
	}
	dsn := dialect.BuildDSN(db.ConnSpec{
		Host:     host,
		Port:     int32(port),
		User:     user,
		Password: pass,
		DBName:   "information_schema",
		Timeout:  "5s",
	})
	sqlDB, err := sql.Open(dialect.SQLDriverName(), dsn)
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(5)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxLifetime(time.Minute * 5)
	return sqlDB, nil
}

// Configure 确保业务（监控/状态）连接可用。
//
// 连接采用「长连接」：整个实例生命周期内只维护一个 *sql.DB 连接池。
// database/sql 会在底层连接断开后于下次查询时自动重连，因此一次瞬时 Ping
// 失败绝不 Close/重建连接池（那样会把 Agent→MySQL 变成短连接）。连接池
// 仅在首次调用（或目标地址确实变化）时才创建。
func (b *MySQLBackend) Configure(host string, port int, user, pass string) error {
	b.mu.Lock()
	if b.db != nil && b.host == host && b.port == port {
		// 长连接已存在：复用现有连接池，仅做一次 Ping 校验，不重建。
		db := b.db
		b.mu.Unlock()

		if err := db.Ping(); err != nil {
			// 服务端当前不可达：保留连接池，database/sql 会在下次调用时
			// 重连。返回错误让调用方进入重连退避，避免 MySQL Error 1129。
			return fmt.Errorf("ping %s:%d: %w", host, port, err)
		}
		b.recordVersionAndResetBackoff()
		return nil
	}
	b.mu.Unlock()

	sqlDB, err := openSQLDB(host, port, user, pass)
	if err != nil {
		return err
	}

	b.mu.Lock()
	if b.db != nil {
		b.db.Close()
	}
	b.host = host
	b.port = port
	b.db = sqlDB
	b.mu.Unlock()

	// 校验连接。握手失败会被返回，以便调用方（心跳）应用重连退避，而不是
	// 立即重试并触发 MySQL 按主机统计的连接错误屏蔽（Error 1129）。
	if err := sqlDB.Ping(); err != nil {
		// 启动时服务端暂不可达：保留长连接池，不 Close；database/sql
		// 会在下次使用时自动重连。
		return fmt.Errorf("business account connect %s:%d: %w", host, port, err)
	}

	b.recordVersionAndResetBackoff()
	return nil
}

// recordVersionAndResetBackoff 在连接成功后读取服务端版本（best-effort）
// 并清除重连退避状态。
func (b *MySQLBackend) recordVersionAndResetBackoff() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.db != nil {
		var ver string
		if err := b.db.QueryRow("SELECT VERSION()").Scan(&ver); err == nil {
			b.version = ver
		}
	}
	b.resetBackoff()
}

// SetReplicationCredentials 记录仅供复制使用的账号（repl）。这些凭据
// 绝不会用于建立客户端连接，只会被嵌入 CHANGE REPLICATION SOURCE
// 语句，供副本 IO 线程向其上游进行认证。
func (b *MySQLBackend) SetReplicationCredentials(user, pass string) error {
	if user == "" {
		return fmt.Errorf("replication user is required")
	}
	b.mu.Lock()
	b.replUser = user
	b.replPass = pass
	b.mu.Unlock()
	return nil
}

// ConfigureManager 使用管理员账号打开一个独立的连接池。故障切换/主备
// 切换以及 GTID 操作始终通过该连接池执行，使高权限操作既不使用探测账号
// 也不使用复制账号。
func (b *MySQLBackend) ConfigureManager(host string, port int, user, pass string) error {
	if user == "" {
		return fmt.Errorf("manager user is required")
	}

	sqlDB, err := openSQLDB(host, port, user, pass)
	if err != nil {
		return fmt.Errorf("open manager connection: %w", err)
	}

	if err := sqlDB.Ping(); err != nil {
		sqlDB.Close()
		return fmt.Errorf("manager account connect %s:%d: %w", host, port, err)
	}

	b.mu.Lock()
	if b.managerDB != nil {
		b.managerDB.Close()
	}
	b.managerUser = user
	b.managerPass = pass
	b.managerDB = sqlDB
	b.mu.Unlock()

	b.printf("Manager account %s configured", user)
	return nil
}

// managerPool 返回管理员连接池；如果管理员账号从未配置，操作会显式报错。
func (b *MySQLBackend) managerPool() (*sql.DB, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.managerDB == nil {
		return nil, fmt.Errorf("manager account not configured")
	}
	return b.managerDB, nil
}

// Discover 发现本机的 MySQL 实例
func (b *MySQLBackend) Discover() ([]backend.InstanceInfo, error) {
	var instances []backend.InstanceInfo

	// 使用 `netstat -tlnp` 或 `ss -tlnp` 查找 mysqld 进程
	// 由于运行在 Linux/MacOS 上，可以尝试将 `pgrep` 或 `ps` 与端口检查结合使用。
	// 为了跨平台的简洁性，并避免当下引入 CGO/gopsutil 依赖，
	// 我们使用 shell 命令来查找 mysqld 的监听端口。

	// ps -ef | grep mysqld | grep -v grep
	cmd := exec.Command("sh", "-c", "netstat -tlnp 2>/dev/null | grep mysqld || ss -tlnp 2>/dev/null | grep mysqld")
	out, err := cmd.Output()
	if err != nil {
		// 如果命令执行失败，返回空列表而非错误，因为这可能只是意味着没有发现实例
		return instances, nil
	}

	lines := strings.Split(string(out), "\n")
	portRegex := regexp.MustCompile(`:(\d+)\s+`)
	pidRegex := regexp.MustCompile(`(\d+)/mysqld`)

	discoveredPorts := make(map[int]bool)
	var candidates []backend.InstanceInfo

	for _, line := range lines {
		if line == "" {
			continue
		}

		portMatch := portRegex.FindStringSubmatch(line)
		pidMatch := pidRegex.FindStringSubmatch(line)

		if len(portMatch) > 1 && len(pidMatch) > 1 {
			port, _ := strconv.Atoi(portMatch[1])
			pid, _ := strconv.Atoi(pidMatch[1])

			if !discoveredPorts[port] {
				discoveredPorts[port] = true
				candidates = append(candidates, backend.InstanceInfo{
					Type: "mysql",
					Host: "127.0.0.1",
					Port: port,
					PID:  pid,
				})
			}
		}
	}

	// 协议级校验：mysqld 会同时监听 SQL 端口与 X Protocol(mysqlx) 端口
	// （如 3306 与 33060），netstat 里都显示为 mysqld。X Protocol 端口不
	// 使用经典握手协议，直接连接会让驱动读取握手包时 panic。逐个用方言
	// Probe 验证，只保留真正讲 MySQL 握手协议的 SQL 端口。
	dialect, dErr := db.GetDialect("mysql")
	if dErr != nil {
		return nil, fmt.Errorf("discover: resolve mysql dialect: %w", dErr)
	}
	for _, c := range candidates {
		addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
		if pErr := dialect.Probe(context.Background(), addr, 2*time.Second); pErr != nil {
			log.Printf("[Discover] skip non-SQL port %s: %v", addr, pErr)
			continue
		}
		instances = append(instances, c)
	}

	return instances, nil
}

func (b *MySQLBackend) Ping(ctx context.Context) (bool, error) {
	if b.db == nil {
		return false, fmt.Errorf("db not configured")
	}
	if err := b.db.PingContext(ctx); err != nil {
		return false, nil
	}
	return true, nil
}

// 重连退避上限，经过调优以远低于 MySQL 默认的
// max_connect_errors（100）。因此即使是长达一分钟的中断，也只会产生
// 少数几次失败握手，而不是每隔几秒就一次。
const (
	backoffBase   = 2 * time.Second
	backoffFactor = 2
	backoffMax    = 5 * time.Minute
)

// ShouldReconnect 报告探测当前是否可以（重新）打开连接。在退避窗口
// 生效期间它返回 false，使心跳可以跳过重连，而不会再产生一次失败的
// 服务端握手。
func (b *MySQLBackend) ShouldReconnect(now time.Time) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return !now.Before(b.nextRetry)
}

// NoteConnectFailure 记录一次失败的探测连接，并在下次重连尝试前应用
// 指数退避（设有上限）。
func (b *MySQLBackend) NoteConnectFailure() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failedConns++
	d := backoffBase
	for i := 1; i < b.failedConns && d < backoffMax; i++ {
		d *= backoffFactor
	}
	if d > backoffMax {
		d = backoffMax
	}
	b.nextRetry = time.Now().Add(d)
	return d
}

// resetBackoff 在连接成功后清除失败状态。
func (b *MySQLBackend) resetBackoff() {
	b.failedConns = 0
	b.nextRetry = time.Time{}
}

func (b *MySQLBackend) GetStatus(ctx context.Context) (map[string]string, error) {
	if b.db == nil {
		return nil, fmt.Errorf("db not configured")
	}

	// 真实实现：查询 MySQL
	// 目前是返回模拟/真实混合数据，还是只返回真实数据？
	// 用户要求 "monitor mysql"，因此我们应当查询真实的 MySQL。

	status := make(map[string]string)

	// 1. 获取全局变量（read_only、super_read_only、log_bin_basename 等）。
	// log_bin_basename 是 binlog 的完整路径前缀，用于心跳上报并由元数据
	// 记录，作为故障时读取 binlog 的兜底位置。
	rows, err := b.db.QueryContext(ctx, "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('read_only', 'super_read_only', 'log_bin_basename', 'rpl_semi_sync_master_enabled', 'rpl_semi_sync_source_enabled', 'rpl_semi_sync_master_clients', 'rpl_semi_sync_source_clients', 'gtid_executed')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err == nil {
			status[name] = value
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 2. 获取副本状态（IO/SQL 线程、上游边界以及二进制日志坐标）。
	// 优先使用 SHOW REPLICA STATUS；在 MySQL 5.7 上回退到 SHOW SLAVE STATUS。
	slaveRows, err := b.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		slaveRows, err = b.db.QueryContext(ctx, "SHOW SLAVE STATUS")
	}
	if err != nil {
		// 不能保持沉默：没有这一行，该节点会被错误地判定为主库。
		b.printf("SHOW REPLICA/SLAVE STATUS failed: %v", err)
	} else {
		defer slaveRows.Close()
		cols, colsErr := slaveRows.Columns()
		if colsErr != nil {
			b.printf("Failed to read replica status columns: %v", colsErr)
		} else if slaveRows.Next() {
			// 扫描到 map 中
			scanArgs := make([]interface{}, len(cols))
			values := make([]sql.RawBytes, len(cols))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			if scanErr := slaveRows.Scan(scanArgs...); scanErr != nil {
				b.printf("Error scanning replica status row: %v", scanErr)
			}
			for i, col := range cols {
				status[col] = string(values[i])
			}
		}
		if rowErr := slaveRows.Err(); rowErr != nil {
			b.printf("Error iterating replica status: %v", rowErr)
		}
	}

	// 将 MySQL 特有的 SHOW 输出归一化到与数据库类型无关的键上。
	// Receiver（接收方）= I/O 线程（拉取变更流）；applier（应用方）= SQL
	// 线程（回放变更流）。其他数据库类型将其等价概念映射到相同的中立键上。

	// 布尔值以 "true"/"false" 输出，以便消费方能够 ParseBool。
	// MySQL 对变量返回 "ON"/"OFF"，对副本状态返回 "Yes"/"No"。

	if val, ok := status["read_only"]; ok {
		status["read_only"] = strconv.FormatBool(val == "ON")
	}

	if val, ok := status["Replica_IO_Running"]; ok {
		status["receiver_running"] = strconv.FormatBool(val == "Yes")
	} else if val, ok := status["Slave_IO_Running"]; ok {
		status["receiver_running"] = strconv.FormatBool(val == "Yes")
	}

	if val, ok := status["Replica_SQL_Running"]; ok {
		status["applier_running"] = strconv.FormatBool(val == "Yes")
	} else if val, ok := status["Slave_SQL_Running"]; ok {
		status["applier_running"] = strconv.FormatBool(val == "Yes")
	}

	if val, ok := status["rpl_semi_sync_source_enabled"]; ok {
		status["semi_sync_master"] = strconv.FormatBool(val == "ON")
	} else if val, ok := status["rpl_semi_sync_master_enabled"]; ok {
		status["semi_sync_master"] = strconv.FormatBool(val == "ON")
	}

	// 当键缺失时回退到模拟值（用于数据库尚未完全就绪时的安全/演示兜底）
	if _, ok := status["read_only"]; !ok {
		status["read_only"] = strconv.FormatBool(b.readOnly)
	}

	// ----------------------------------------------------------------------
	// 中立状态契约：供本地调用方（心跳）使用的、与数据库类型无关的键。
	// 上面的 MySQL 原生键保留给 gRPC 消费方使用。
	// ----------------------------------------------------------------------
	b.mu.RLock()
	status["version"] = b.version
	b.mu.RUnlock()

	// 上游边界：较新的内核（包括当前这个 9.5 构建）只暴露 Source_*
	// 列，而官方 8.x/5.7 保留 Master_* 别名。两者都接受。
	status["upstream_host"] = firstNonEmpty(status, "Source_Host", "Master_Host")
	status["upstream_port"] = firstNonEmpty(status, "Source_Port", "Master_Port")

	// 二进制日志坐标优先使用源端自身的文件；当该节点只有中转（relay）值时，
	// 回退到中转日志坐标。
	status["binlog_file"] = firstNonEmpty(status, "Source_Log_File", "Master_Log_File", "Relay_Source_Log_File", "Relay_Master_Log_File")
	status["binlog_pos"] = firstNonEmpty(status, "Exec_Source_Log_Pos", "Exec_Master_Log_Pos", "Read_Source_Log_Pos", "Read_Master_Log_Pos")

	status["semi_sync_enabled"] = status["semi_sync_master"]
	status["semi_sync_clients"] = firstNonEmpty(status, "rpl_semi_sync_source_clients", "rpl_semi_sync_master_clients")
	status["replication_lag_seconds"] = firstNonEmpty(status, "Seconds_Behind_Source", "Seconds_Behind_Master")

	return status, nil
}

func (b *MySQLBackend) KillProcess(ctx context.Context, pid int64, isOSPID bool) error {
	if isOSPID {
		cmd := exec.CommandContext(ctx, "kill", "-9", strconv.FormatInt(pid, 10))
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("OS kill %d failed: %v (output: %s)", pid, err, string(out))
		}
		b.printf("OS process %d killed", pid)
		return nil
	}

	mgr, err := b.managerPool()
	if err != nil {
		return err
	}
	// pid 为 int64 —— 使用 %d 不存在注入风险
	if _, err := mgr.ExecContext(ctx, fmt.Sprintf("KILL %d", pid)); err != nil {
		return fmt.Errorf("KILL thread %d failed: %w", pid, err)
	}
	b.printf("MySQL thread %d killed", pid)
	return nil
}

// majorVersion 从 VERSION() 中解析主版本号（例如 "9.5.0-log" -> 9、
// "8.0.36" -> 8、"5.7.42" -> 5）。版本未知时返回 0。
func (b *MySQLBackend) majorVersion() int {
	b.mu.RLock()
	v := b.version
	b.mu.RUnlock()
	if i := strings.IndexByte(v, '.'); i > 0 {
		if n, err := strconv.Atoi(v[:i]); err == nil {
			return n
		}
	}
	return 0
}

// useReplicaSyntax 报告复制控制是否必须使用现代的 REPLICA/SOURCE
// 语法。旧版 MASTER/SLAVE 语法在 8.0.22 中被弃用，并在 MySQL 8.4 中被
// 移除，因此所有 >= 8 的版本（包括 9.x 以及诸如当前 9.5 构建之类的兼容
// 派生版本）都需要使用新语法。版本未知时，我们乐观地优先尝试新语法；
// execReplicationAdmin 仍会在遇到 1064 错误时回退。
func (b *MySQLBackend) useReplicaSyntax() bool {
	major := b.majorVersion()
	return major == 0 || major >= 8
}

// isSyntaxError 识别因关键字不受支持而返回的 Error 1064（例如在只认识
// STOP REPLICA 的服务端上执行 STOP SLAVE）。通过匹配驱动的错误文本来
// 判断，可使本包无需导入具体的 MySQL 驱动，与方言抽象保持一致。
func isSyntaxError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1064")
}

// execReplicationAdmin 在给定连接池上执行一条复制管理语句，优先使用
// 现代的 REPLICA/SOURCE 形式，并在较旧的服务端（5.7 / 早期
// 8.0）上透明地回退到旧版 MASTER/SLAVE 形式。任何其他失败都会被返回，
// 而不会被吞掉。
func (b *MySQLBackend) execReplicationAdmin(ctx context.Context, db *sql.DB, modern, legacy string) error {
	primary, fallback := modern, legacy
	if !b.useReplicaSyntax() {
		primary, fallback = legacy, modern
	}

	if _, err := db.ExecContext(ctx, primary); err != nil {
		if fallback == "" || !isSyntaxError(err) {
			return fmt.Errorf("exec %q: %w", primary, err)
		}
		b.printf("%q rejected (%v), falling back to %q", primary, err, fallback)
		if _, err2 := db.ExecContext(ctx, fallback); err2 != nil {
			return fmt.Errorf("exec %q: %w (fallback %q: %v)", primary, err, fallback, err2)
		}
	}
	return nil
}

// quoteSQLString 对值加引号，以便在 MySQL 语句中使用。这些语句无法
// 使用 ? 占位符，因此必须显式转义反斜杠/引号。
func quoteSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'"
}

func (b *MySQLBackend) changeMaster(ctx context.Context, db *sql.DB, host string, port int) error {
	// 安全性：校验 host 以防止 SQL 注入。
	// 对主机名/IP，我们只允许字母、数字、点和连字符。
	for _, c := range host {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '-') {
			return fmt.Errorf("invalid host character detected: %c", c)
		}
	}

	b.mu.RLock()
	user, pass := b.replUser, b.replPass
	b.mu.RUnlock()
	if user == "" {
		return fmt.Errorf("replication account not configured")
	}

	// CHANGE REPLICATION SOURCE / CHANGE MASTER 无法使用 ? 绑定；host
	// 白名单、整型端口以及加引号的字面量保证了安全性。副本使用复制专用账号
	// 向新源端进行认证。
	portStr := strconv.Itoa(port)
	modern := "CHANGE REPLICATION SOURCE TO " +
		"SOURCE_HOST=" + quoteSQLString(host) + ", " +
		"SOURCE_PORT=" + portStr + ", " +
		"SOURCE_USER=" + quoteSQLString(user) + ", " +
		"SOURCE_PASSWORD=" + quoteSQLString(pass) + ", " +
		"SOURCE_AUTO_POSITION=1"
	legacy := "CHANGE MASTER TO " +
		"MASTER_HOST=" + quoteSQLString(host) + ", " +
		"MASTER_PORT=" + portStr + ", " +
		"MASTER_USER=" + quoteSQLString(user) + ", " +
		"MASTER_PASSWORD=" + quoteSQLString(pass) + ", " +
		"MASTER_AUTO_POSITION=1"

	return b.execReplicationAdmin(ctx, db, modern, legacy)
}

// isUnknownVariable 识别 Error 1193，该错误表示系统变量不存在 —— 通常
// 是因为半同步插件尚未安装。
func isUnknownVariable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1193")
}

// enableSemiSyncSource 使用现代变量 rpl_semi_sync_source_enabled 在复制
// 源端启用半同步。当变量未知（1193）时，它会安装一次半同步源端插件然后
// 重试。失败会被返回，由调用方决定如何处理；提升流程将其视为警告。
func (b *MySQLBackend) enableSemiSyncSource(ctx context.Context, db *sql.DB) error {
	const enableStmt = "SET GLOBAL rpl_semi_sync_source_enabled = 1"

	if _, err := db.ExecContext(ctx, enableStmt); err == nil {
		b.printf("Semi-sync source enabled (rpl_semi_sync_source_enabled=1)")
		return nil
	} else if !isUnknownVariable(err) {
		return fmt.Errorf("enable semi-sync source: %w", err)
	} else {
		b.printf("rpl_semi_sync_source_enabled unknown (%v), attempting to install semi-sync source plugin", err)
	}

	// 插件名同样由 master 重命名为 source。优先尝试现代插件，再以旧版插件
	// 作为回退。
	installStmts := []string{
		"INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so'",
		"INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so'",
	}
	var lastErr error
	for _, stmt := range installStmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			lastErr = err
			b.printf("%s failed: %v", stmt, err)
			continue
		}
		b.printf("Installed semi-sync plugin via: %s", stmt)
		if _, err := db.ExecContext(ctx, enableStmt); err != nil {
			lastErr = err
			b.printf("Enable after install failed: %v", err)
			break
		}
		b.printf("Semi-sync source enabled after plugin install")
		return nil
	}
	return fmt.Errorf("enable semi-sync source failed: %v", lastErr)
}

// WaitCatchup 使用的宽限期，用于快速失败而不是无限挂起：
var (
	// catchupIOThreadGrace：等待已停止的 IO/SQL 线程重连的最长时间。
	catchupIOThreadGrace = 3 * time.Second
	// catchupSourceGrace：等待到源端的管理连接打开、或源端 GTID 读取/解析
	// 成功的最长时间。
	catchupSourceGrace = 5 * time.Second
	// catchupStatusGrace：容忍 SHOW REPLICA STATUS 读取错误的最长时间。
	catchupStatusGrace = 5 * time.Second
)

// setGlobalAndVerify 设置一个全局系统变量并回读，以确认新值确实生效。
// 变量名始终是调用方提供的常量（绝不是用户输入），因此字符串插值是安全的。
// 如果 SET 失败或回读值不一致，它会返回错误；因此只有在变更被验证后，
// 才会打印成功日志。
func setGlobalAndVerify(ctx context.Context, db *sql.DB, varName string, wantOn bool) error {
	flag := "0"
	if wantOn {
		flag = "1"
	}
	if _, err := db.ExecContext(ctx, "SET GLOBAL "+varName+" = "+flag); err != nil {
		return fmt.Errorf("set %s=%s: %w", varName, flag, err)
	}

	var got string
	if err := db.QueryRowContext(ctx, "SELECT @@global."+varName).Scan(&got); err != nil {
		return fmt.Errorf("verify %s: %w", varName, err)
	}
	gotOn := got == "1" || strings.EqualFold(got, "ON")
	if gotOn != wantOn {
		return fmt.Errorf("%s did not take effect: want %s, got %q", varName, flag, got)
	}
	return nil
}

// intervalsOf 从解析后的 MySQL GTID 集合中，提取属于单个服务器 UUID 的
// 闭区间 GTID 段（"numbers"）。当集合中没有该 UUID 的条目时返回 nil。
func intervalsOf(set gomysql.GTIDSet, serverUUID string) gomysql.IntervalSlice {
	ms, ok := set.(*gomysql.MysqlGTIDSet)
	if !ok {
		return nil
	}
	sid, err := uuid.Parse(serverUUID)
	if err != nil {
		return nil
	}
	tagIntervals, ok := (*ms)[sid]
	if !ok {
		return nil
	}
	// 经典（无标签）GTID 使用空 Tag。当只存在一个带标签的组时，使用它；
	// 若存在多个标签，则没有单一的区间流可供比较，因此返回 nil。
	if iv, ok := tagIntervals[gomysql.Tag{}]; ok {
		return iv
	}
	if len(tagIntervals) == 1 {
		for _, iv := range tagIntervals {
			return iv
		}
	}
	return nil
}

// sameUUIDIntervals 报告给定服务器 UUID 对应的区间在两个 GTID 集合中
// 是否相同，即本地节点是否恰好执行了该源端产生的全部相同事务
// （uuid + gtid numbers）。
func sameUUIDIntervals(local, source gomysql.GTIDSet, serverUUID string) (bool, error) {
	li := intervalsOf(local, serverUUID)
	si := intervalsOf(source, serverUUID)
	if len(si) == 0 {
		return false, fmt.Errorf("source gtid_executed has no entry for source uuid %s", serverUUID)
	}
	if len(li) != len(si) {
		return false, nil
	}
	for i := range si {
		if li[i].Start != si[i].Start || li[i].Stop != si[i].Stop {
			return false, nil
		}
	}
	return true, nil
}

// intervalsString 将 IntervalSlice 渲染为 GTID 字符串中使用的闭区间
// 形式（"n" 或 "a-b"，以逗号分隔）。区间以半开区间 [Start, Stop)
// 存储，因此可见的末端为 Stop-1。
func intervalsString(iv gomysql.IntervalSlice) string {
	parts := make([]string, 0, len(iv))
	for _, in := range iv {
		end := in.Stop - 1
		if end == in.Start {
			parts = append(parts, strconv.FormatInt(in.Start, 10))
		} else {
			parts = append(parts, strconv.FormatInt(in.Start, 10)+"-"+strconv.FormatInt(end, 10))
		}
	}
	return strings.Join(parts, ",")
}

// opHandler 是单个管理操作的处理器：ExecuteOperation 已取好管理员
// 连接 mgr，处理器只需执行具体逻辑并返回结果。
type opHandler func(b *MySQLBackend, ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error)

// operationHandlers 是操作名到处理器的分发表。操作名是跨层契约
// （logic 层与测试都依赖），与重构前的 switch 分支一一对应。
var operationHandlers = map[string]opHandler{
	"StopSlave":            (*MySQLBackend).opStopSlave,
	"StartSlave":           (*MySQLBackend).opStartSlave,
	"ResetSlave":           (*MySQLBackend).opResetSlave,
	"ChangeMaster":         (*MySQLBackend).opChangeMaster,
	"SetReadOnly":          (*MySQLBackend).opSetReadOnly,
	"EnableSemiSyncMaster": (*MySQLBackend).opEnableSemiSyncMaster,
	"KillConnections":      (*MySQLBackend).opKillConnections,
	"WaitCatchup":          (*MySQLBackend).opWaitCatchup,
}

func (b *MySQLBackend) ExecuteOperation(ctx context.Context, operation string, params map[string]string) (map[string]string, error) {
	b.printf("Executing Operation: %s Params: %v", operation, params)

	// 所有操作都是高权限管理任务，必须通过管理员账号执行，绝不能使用探测
	// 账号或复制账号。
	mgr, err := b.managerPool()
	if err != nil {
		b.printf("Operation rejected: %v", err)
		return nil, err
	}

	handler, ok := operationHandlers[operation]
	if !ok {
		return nil, fmt.Errorf("unknown operation: %s", operation)
	}
	return handler(b, ctx, mgr, params)
}

func (b *MySQLBackend) opStopSlave(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	if err := b.execReplicationAdmin(ctx, mgr, "STOP REPLICA", "STOP SLAVE"); err != nil {
		b.printf("StopSlave failed: %v", err)
		return nil, err
	}
	return map[string]string{"message": "Replica stopped"}, nil

}

func (b *MySQLBackend) opStartSlave(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	if err := b.execReplicationAdmin(ctx, mgr, "START REPLICA", "START SLAVE"); err != nil {
		b.printf("StartSlave failed: %v", err)
		return nil, err
	}
	return map[string]string{"message": "Replica started"}, nil

}

func (b *MySQLBackend) opResetSlave(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	suffix := ""
	if params["all"] == "true" {
		suffix = " ALL"
	}
	if err := b.execReplicationAdmin(ctx, mgr, "RESET REPLICA"+suffix, "RESET SLAVE"+suffix); err != nil {
		b.printf("ResetSlave failed: %v", err)
		return nil, err
	}
	return map[string]string{"message": "Replica reset"}, nil

}

func (b *MySQLBackend) opChangeMaster(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	host := params["host"]
	port := params["port"]

	// 校验 host 和 port 以防止 SQL 注入
	if host != "" {
		host = strings.ReplaceAll(host, "'", "")
		host = strings.ReplaceAll(host, "\"", "")
		host = strings.ReplaceAll(host, "\\", "")
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %v", err)
	}

	// 严格的 host 白名单：CHANGE REPLICATION SOURCE / CHANGE MASTER
	// 无法使用占位符语法，因此在字符串被 changeMaster 插值之前，拒绝任何
	// 不是纯主机名/IP 的内容。
	for _, c := range host {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '-') {
			return nil, fmt.Errorf("invalid host format: potential injection detected")
		}
	}

	if err := b.changeMaster(ctx, mgr, host, p); err != nil {
		b.printf("CHANGE MASTER/SOURCE failed: %v", err)
		return nil, err
	}
	return map[string]string{"message": "Replication source changed"}, nil

}

func (b *MySQLBackend) opSetReadOnly(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	val, _ := strconv.ParseBool(params["read_only"])
	b.readOnly = val

	_, hasSuper := params["super_read_only"]
	superVal := val
	if hasSuper {
		superVal, _ = strconv.ParseBool(params["super_read_only"])
	}

	// 按照 MySQL 要求的顺序设置变量：
	//  - 启用时：先设置 super_read_only=1（它会隐式设置 read_only=1），再设置 read_only；
	//  - 禁用时：先设置 super_read_only=0（super 开启时无法清除 read_only），再设置 read_only。
	if val || (hasSuper && superVal) {
		if hasSuper {
			if err := setGlobalAndVerify(ctx, mgr, "super_read_only", superVal); err != nil {
				b.printf("SET super_read_only=%t failed: %v", superVal, err)
				return nil, err
			}
		}
		if err := setGlobalAndVerify(ctx, mgr, "read_only", val); err != nil {
			b.printf("SET read_only=%t failed: %v", val, err)
			return nil, err
		}
	} else {
		if hasSuper {
			if err := setGlobalAndVerify(ctx, mgr, "super_read_only", false); err != nil {
				b.printf("SET super_read_only=false failed: %v", err)
				return nil, err
			}
		}
		if err := setGlobalAndVerify(ctx, mgr, "read_only", false); err != nil {
			b.printf("SET read_only=false failed: %v", err)
			return nil, err
		}
	}

	// 只有在两个变量都已设置并回读后才记录成功，因此该行现在反映的是数据库
	// 的真实状态。
	b.printf("Read-only state confirmed: read_only=%t, super_read_only(present=%t)=%t", val, hasSuper, superVal)
	return map[string]string{"message": fmt.Sprintf("Read Only set to %v", val)}, nil

}

func (b *MySQLBackend) opEnableSemiSyncMaster(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	// 变量由 rpl_semi_sync_master_enabled 重命名为
	// rpl_semi_sync_source_enabled（8.0.26）；旧名称在 8.4 中被移除。
	if err := b.enableSemiSyncSource(ctx, mgr); err != nil {
		b.printf("EnableSemiSyncMaster failed: %v", err)
		return nil, err
	}
	return map[string]string{"message": "Semi-sync enabled"}, nil

}

func (b *MySQLBackend) opKillConnections(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	rows, err := mgr.QueryContext(ctx, "SELECT ID, USER, COMMAND FROM INFORMATION_SCHEMA.PROCESSLIST WHERE ID != CONNECTION_ID()")
	if err != nil {
		return nil, fmt.Errorf("query processlist failed: %w", err)
	}
	var killed, failed int
	for rows.Next() {
		var id int64
		var user, command string
		if err := rows.Scan(&id, &user, &command); err != nil {
			continue
		}
		if user == "system user" || command == "Daemon" || user == "event_scheduler" {
			continue
		}
		if _, err := mgr.ExecContext(ctx, fmt.Sprintf("KILL %d", id)); err != nil {
			b.printf("Failed to kill connection %d: %v", id, err)
			failed++
		} else {
			killed++
		}
	}
	rows.Close()
	msg := fmt.Sprintf("Killed %d connections (%d failed)", killed, failed)
	b.printf("%s", msg)
	return map[string]string{"message": msg, "killed": strconv.Itoa(killed), "failed": strconv.Itoa(failed)}, nil

}

func (b *MySQLBackend) opWaitCatchup(ctx context.Context, mgr *sql.DB, params map[string]string) (map[string]string, error) {
	b.printf("Waiting for GTID catchup...")
	started := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// 是否追平按旧主库（当前复制源）的 server_uuid 判断：当副本自身
	// gtid_executed 在该 uuid 下携带的编号，等于旧主库在同一 uuid 下报告的
	// 编号时，副本即已追平。Retrieved_Gtid_Set 和整个集合相等都不是正确的
	// 判断标准 —— 在 GTID 自动定位下，IO 线程会跳过已在先前源端执行过的
	// 事务，因此这些 uuid:numbers 会出现在 Executed 中却不在 Retrieved 中。
	var sourceDB *sql.DB
	defer func() {
		if sourceDB != nil {
			sourceDB.Close()
		}
	}()
	var sourceOpenErr error
	lastState := ""

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("WaitCatchup timed out: %w", ctx.Err())
		case <-ticker.C:
		}

		row, hasRow, err := b.getReplicaStatusRow(ctx, mgr)
		if err != nil {
			if time.Since(started) > catchupStatusGrace {
				return nil, fmt.Errorf("read replica status failed after %s: %w", catchupStatusGrace, err)
			}
			b.printf("Read replica status failed: %v", err)
			continue
		}
		// 未配置为副本：没有需要等待的内容。
		if !hasRow {
			return map[string]string{"message": "No replication configured, skipping catchup"}, nil
		}

		ioRunning := firstNonEmpty(row, "Replica_IO_Running", "Slave_IO_Running") == "Yes"
		sqlRunning := firstNonEmpty(row, "Replica_SQL_Running", "Slave_SQL_Running") == "Yes"
		if !ioRunning || !sqlRunning {
			thread := "IO"
			if !ioRunning && !sqlRunning {
				thread = "IO and SQL"
			} else if !sqlRunning {
				thread = "SQL"
			}
			if time.Since(started) < catchupIOThreadGrace {
				b.printf("Replica %s thread not running (IO err: %s, SQL err: %s), waiting briefly...",
					thread,
					firstNonEmpty(row, "Last_IO_Error", "Last_Error"),
					firstNonEmpty(row, "Last_SQL_Error", "Last_Error"))
				continue
			}
			return nil, fmt.Errorf("replication %s thread not running after %s; cannot wait for catchup (check replication source and replication account)", thread, catchupIOThreadGrace)
		}

		// 只向源端打开一次管理连接，跨多个 tick 复用。
		if sourceDB == nil {
			sourceHost := firstNonEmpty(row, "Source_Host", "Master_Host")
			sourcePort, _ := strconv.Atoi(firstNonEmpty(row, "Source_Port", "Master_Port"))
			b.mu.RLock()
			mgrUser, mgrPass := b.managerUser, b.managerPass
			b.mu.RUnlock()

			sd, openErr := openSQLDB(sourceHost, sourcePort, mgrUser, mgrPass)
			if openErr != nil {
				sourceOpenErr = openErr
			} else if pingErr := sd.PingContext(ctx); pingErr != nil {
				sd.Close()
				sourceOpenErr = pingErr
			} else {
				sourceDB = sd
				b.printf("Connected to replication source %s:%d for GTID comparison", sourceHost, sourcePort)
			}
		}

		if sourceDB == nil {
			if time.Since(started) > catchupSourceGrace {
				return nil, fmt.Errorf("could not open management connection to source after %s: %w", catchupSourceGrace, sourceOpenErr)
			}
			b.printf("Opening management connection to source failed (%v), retrying...", sourceOpenErr)
			continue
		}

		// 源端侧已执行的 GTID 集合。
		var sourceGTID string
		if err := sourceDB.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&sourceGTID); err != nil {
			if time.Since(started) > catchupSourceGrace {
				return nil, fmt.Errorf("read source gtid_executed failed after %s: %w", catchupSourceGrace, err)
			}
			continue
		}

		// 非 GTID 复制：两侧都没有可比较的内容。
		if strings.TrimSpace(sourceGTID) == "" {
			b.printf("Source reports empty gtid_executed, skipping GTID catchup")
			return map[string]string{"message": "No GTID replication detected, skipping catchup"}, nil
		}

		localGTID := firstNonEmpty(row, "Executed_Gtid_Set")
		if strings.TrimSpace(localGTID) == "" {
			// 全新副本，尚未应用任何内容：继续等待。
			state := "empty-local"
			if state != lastState {
				b.printf("GTID catchup in progress, local executed set is empty, waiting for source transactions")
				lastState = state
			}
			continue
		}

		// 旧主库（当前复制源）的 server_uuid，直接取自 9.x 的
		// SHOW REPLICA STATUS 输出。
		sourceUUID := firstNonEmpty(row, "Source_UUID", "Master_UUID")
		if sourceUUID == "" {
			return nil, fmt.Errorf("SHOW REPLICA STATUS did not report Source_UUID, cannot compare per-source GTID")
		}

		localSet, errL := gomysql.ParseGTIDSet("mysql", localGTID)
		sourceSet, errS := gomysql.ParseGTIDSet("mysql", sourceGTID)
		if errL != nil || errS != nil {
			if time.Since(started) > catchupSourceGrace {
				return nil, fmt.Errorf("parse gtid sets failed (local: %v, source: %v)", errL, errS)
			}
			continue
		}

		// 追平 == 本地节点的已执行集合恰好携带旧主库所报告的
		// <old-master-uuid>:<gtid numbers>。
		equal, eqErr := sameUUIDIntervals(localSet, sourceSet, sourceUUID)
		if eqErr != nil {
			if time.Since(started) > catchupSourceGrace {
				return nil, eqErr
			}
			b.printf("GTID per-uuid comparison unavailable: %v, retrying...", eqErr)
			continue
		}
		if equal {
			b.printf("GTID catchup complete: local executed %s:%s matches old master", sourceUUID, intervalsString(intervalsOf(sourceSet, sourceUUID)))
			return map[string]string{"message": "Caught up", "gtid_executed": sourceGTID}, nil
		}

		state := localGTID + "|" + sourceUUID + "|" + sourceGTID
		if state != lastState {
			b.printf("GTID catchup in progress, local numbers for source %s lag behind (local=%s, source=%s)", sourceUUID, localGTID, sourceGTID)
			lastState = state
		}
	}
}

// getReplicaStatusRow 将唯一的 SHOW REPLICA STATUS 行读取到 map 中。
// 当服务端当前未配置复制时 hasRow 为 false。
func (b *MySQLBackend) getReplicaStatusRow(ctx context.Context, db *sql.DB) (row map[string]string, hasRow bool, err error) {
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		rows, err = db.QueryContext(ctx, "SHOW SLAVE STATUS")
		if err != nil {
			return nil, false, err
		}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, false, err
	}
	if !rows.Next() {
		return nil, false, nil
	}

	scanArgs := make([]interface{}, len(cols))
	values := make([]sql.RawBytes, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return nil, false, err
	}

	colMap := make(map[string]string, len(cols))
	for i, col := range cols {
		colMap[col] = string(values[i])
	}
	return colMap, true, rows.Err()
}

// firstNonEmpty 返回在给定键中找到的第一个非空值。它使调用方能够容忍
// 不同 MySQL 版本及兼容派生版本之间 Source_* 与 Master_* 的列名差异。
func firstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}

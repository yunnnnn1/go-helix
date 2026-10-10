package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// newMockBackend 构造一个以 sqlmock 作为管理员连接池的后端，用于隔离测试
// 各管理操作，无需真实 MySQL。
func newMockBackend(t *testing.T) (*MySQLBackend, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	b := NewMySQLBackend()
	b.managerDB = db
	b.db = db
	b.host, b.port = "127.0.0.1", 3306
	b.cluster = "cluster1"
	return b, mock
}

func TestExecuteOperationUnknown(t *testing.T) {
	b, _ := newMockBackend(t)
	_, err := b.ExecuteOperation(context.Background(), "Bogus", nil)
	if err == nil || !contains(err.Error(), "unknown operation") {
		t.Fatalf("want unknown-operation error, got %v", err)
	}
}

func TestExecuteOperationManagerNotConfigured(t *testing.T) {
	b := NewMySQLBackend()
	_, err := b.ExecuteOperation(context.Background(), "StopSlave", nil)
	if err == nil || !contains(err.Error(), "manager account not configured") {
		t.Fatalf("want not-configured error, got %v", err)
	}
}

func TestOpSlaveLifecycle(t *testing.T) {
	b, mock := newMockBackend(t)

	mock.ExpectExec("STOP REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := b.ExecuteOperation(context.Background(), "StopSlave", nil); err != nil {
		t.Fatalf("StopSlave: %v", err)
	}

	mock.ExpectExec("START REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := b.ExecuteOperation(context.Background(), "StartSlave", nil); err != nil {
		t.Fatalf("StartSlave: %v", err)
	}

	mock.ExpectExec("RESET REPLICA$").WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := b.ExecuteOperation(context.Background(), "ResetSlave", nil); err != nil {
		t.Fatalf("ResetSlave: %v", err)
	}

	mock.ExpectExec("RESET REPLICA ALL").WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := b.ExecuteOperation(context.Background(), "ResetSlave",
		map[string]string{"all": "true"}); err != nil {
		t.Fatalf("ResetSlave all: %v", err)
	}
}

func TestOpSlaveAdminSyntaxFallback(t *testing.T) {
	b, mock := newMockBackend(t)

	// 现代语法被 1064 拒绝，透明回退到旧语法。
	mock.ExpectExec("STOP REPLICA").
		WillReturnError(errMy("Error 1064: You have an error in your SQL syntax"))
	mock.ExpectExec("STOP SLAVE").WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := b.ExecuteOperation(context.Background(), "StopSlave", nil); err != nil {
		t.Fatalf("expected fallback success, got %v", err)
	}
}

func TestOpChangeMaster(t *testing.T) {
	b, mock := newMockBackend(t)
	b.replUser, b.replPass = "repl", "p"

	mock.ExpectExec("CHANGE REPLICATION SOURCE TO SOURCE_HOST").
		WillReturnResult(sqlmock.NewResult(0, 0))

	res, err := b.ExecuteOperation(context.Background(), "ChangeMaster", map[string]string{
		"host": "127.0.0.1", "port": "3308",
	})
	if err != nil {
		t.Fatalf("ChangeMaster: %v", err)
	}
	if res["message"] != "Replication source changed" {
		t.Fatalf("unexpected result %v", res)
	}

	// 端口非法。
	if _, err := b.ExecuteOperation(context.Background(), "ChangeMaster", map[string]string{
		"host": "h", "port": "abc",
	}); err == nil || !contains(err.Error(), "invalid port") {
		t.Fatalf("want invalid-port error, got %v", err)
	}

	// host 注入字符。
	if _, err := b.ExecuteOperation(context.Background(), "ChangeMaster", map[string]string{
		"host": "h; DROP TABLE x", "port": "1",
	}); err == nil || !contains(err.Error(), "injection") {
		t.Fatalf("want injection error, got %v", err)
	}
}

func TestOpChangeMasterMissingReplAccount(t *testing.T) {
	b, _ := newMockBackend(t)
	// replUser 未配置：在执行 SQL 前即报错，无 SQL 期望。
	_, err := b.ExecuteOperation(context.Background(), "ChangeMaster", map[string]string{
		"host": "h", "port": "1",
	})
	if err == nil || !contains(err.Error(), "replication account not configured") {
		t.Fatalf("want missing repl account error, got %v", err)
	}
}

func TestOpSetReadOnly(t *testing.T) {
	b, mock := newMockBackend(t)

	// read_only=true（无 super）：SET 后回读为 ON。
	mock.ExpectExec("SET GLOBAL read_only = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("ON"))

	if _, err := b.ExecuteOperation(context.Background(), "SetReadOnly",
		map[string]string{"read_only": "true"}); err != nil {
		t.Fatalf("set read only: %v", err)
	}

	// super_read_only=false：按顺序先 super 后 read。
	mock.ExpectExec("SET GLOBAL super_read_only = 0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT @@global.super_read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))
	mock.ExpectExec("SET GLOBAL read_only = 0").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("OFF"))

	if _, err := b.ExecuteOperation(context.Background(), "SetReadOnly", map[string]string{
		"read_only": "false", "super_read_only": "false",
	}); err != nil {
		t.Fatalf("clear read only: %v", err)
	}
}

func TestOpSetReadOnlyVerifyMismatch(t *testing.T) {
	b, mock := newMockBackend(t)

	mock.ExpectExec("SET GLOBAL read_only = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	// 回读为 OFF，与期望不一致 → 报错。
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("OFF"))

	_, err := b.ExecuteOperation(context.Background(), "SetReadOnly",
		map[string]string{"read_only": "true"})
	if err == nil || !contains(err.Error(), "did not take effect") {
		t.Fatalf("want take-effect error, got %v", err)
	}
}

func TestEnableSemiSync(t *testing.T) {
	b, mock := newMockBackend(t)

	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_enabled = 1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	if _, err := b.ExecuteOperation(context.Background(), "EnableSemiSyncMaster", nil); err != nil {
		t.Fatalf("direct enable: %v", err)
	}
}

func TestEnableSemiSyncInstallPlugin(t *testing.T) {
	b, mock := newMockBackend(t)

	// 变量未知（1193）→ 安装插件 → 再启用。
	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_enabled = 1").
		WillReturnError(errMy("Error 1193: Unknown system variable"))
	mock.ExpectExec("INSTALL PLUGIN rpl_semi_sync_source").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_enabled = 1").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := b.ExecuteOperation(context.Background(), "EnableSemiSyncMaster", nil); err != nil {
		t.Fatalf("enable after install: %v", err)
	}
}

func TestEnableSemiSyncHardError(t *testing.T) {
	b, mock := newMockBackend(t)

	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_enabled = 1").
		WillReturnError(errMy("Error 1227: access denied"))
	_, err := b.ExecuteOperation(context.Background(), "EnableSemiSyncMaster", nil)
	if err == nil {
		t.Fatal("want hard error propagated")
	}
}

func TestOpKillConnections(t *testing.T) {
	b, mock := newMockBackend(t)

	procRows := sqlmock.NewRows([]string{"ID", "USER", "COMMAND"}).
		AddRow(5, "app", "Query").             // 被杀
		AddRow(6, "system user", "Connect").   // 跳过：复制线程
		AddRow(7, "event_scheduler", "Daemon") // 跳过：事件 / Daemon
	mock.ExpectQuery("PROCESSLIST").WillReturnRows(procRows)
	mock.ExpectExec("KILL 5").WillReturnResult(sqlmock.NewResult(0, 0))

	res, err := b.ExecuteOperation(context.Background(), "KillConnections", nil)
	if err != nil {
		t.Fatalf("KillConnections: %v", err)
	}
	if res["killed"] != "1" || res["failed"] != "0" {
		t.Fatalf("want killed=1 failed=0, got %v", res)
	}
}

func TestKillProcessMySQLThread(t *testing.T) {
	b, mock := newMockBackend(t)
	mock.ExpectExec("KILL 9").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := b.KillProcess(context.Background(), 9, false); err != nil {
		t.Fatalf("kill thread: %v", err)
	}
}

func TestGetStatus(t *testing.T) {
	b, mock := newMockBackend(t)
	b.version = "8.0.36"

	varRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("read_only", "ON").
		AddRow("rpl_semi_sync_source_enabled", "ON").
		AddRow("gtid_executed", "uuid:1-10")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES").WillReturnRows(varRows)

	// 副本状态：空结果集（当前未配置复制）。
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host"}))

	st, err := b.GetStatus(context.Background())
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if st["read_only"] != "true" {
		t.Fatalf("read_only normalized wrong: %q", st["read_only"])
	}
	if st["semi_sync_master"] != "true" {
		t.Fatalf("semi sync normalized wrong: %q", st["semi_sync_master"])
	}
	if st["version"] != "8.0.36" {
		t.Fatalf("version missing: %q", st["version"])
	}
}

func TestGetStatusWithReplica(t *testing.T) {
	b, mock := newMockBackend(t)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "OFF"))
	replRows := sqlmock.NewRows([]string{
		"Replica_IO_Running", "Replica_SQL_Running", "Source_Host", "Source_Port",
		"Source_Log_File", "Exec_Source_Log_Pos", "Seconds_Behind_Source",
	}).AddRow("Yes", "Yes", "10.0.0.1", "3306", "bin.000001", "123", "0")
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(replRows)

	st, err := b.GetStatus(context.Background())
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if st["receiver_running"] != "true" || st["applier_running"] != "true" {
		t.Fatalf("repl threads normalized wrong: %+v", st)
	}
	if st["upstream_host"] != "10.0.0.1" || st["binlog_file"] != "bin.000001" {
		t.Fatalf("upstream/binlog normalized wrong: %+v", st)
	}
}

func TestWaitCatchupContextCancel(t *testing.T) {
	b, _ := newMockBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 进入循环即取消。

	_, err := b.ExecuteOperation(ctx, "WaitCatchup", nil)
	if err == nil || !contains(err.Error(), "timed out") {
		t.Fatalf("want timeout error, got %v", err)
	}
}

func TestWaitCatchupNoReplication(t *testing.T) {
	b, mock := newMockBackend(t)
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host"})) // 空结果。

	res, err := b.ExecuteOperation(context.Background(), "WaitCatchup", nil)
	if err != nil {
		t.Fatalf("want no-replication result, got %v", err)
	}
	if !contains(res["message"], "No replication configured") {
		t.Fatalf("unexpected message: %v", res)
	}
}

func TestBackoffSequence(t *testing.T) {
	b := NewMySQLBackend()

	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	for i, w := range want {
		if d := b.NoteConnectFailure(); d != w {
			t.Fatalf("failure %d want %s got %s", i+1, w, d)
		}
	}
	// 立即重连应被拒绝。
	if b.ShouldReconnect(time.Now()) {
		t.Fatal("should not reconnect within backoff window")
	}
	// 超过 5 分钟退避窗口后允许重连。
	if !b.ShouldReconnect(time.Now().Add(backoffMax + time.Second)) {
		t.Fatal("should reconnect after backoff window")
	}
	// 退避有上限：继续失败不会超过 backoffMax。
	for i := 0; i < 20; i++ {
		b.NoteConnectFailure()
	}
	if d := b.NoteConnectFailure(); d > backoffMax {
		t.Fatalf("backoff exceeded cap: %s", d)
	}

	// 成功后重置。
	b.resetBackoff()
	if b.failedConns != 0 || b.ShouldReconnect(time.Now()) == false {
		t.Fatal("backoff not reset")
	}
}

func TestSetReplicationCredentials(t *testing.T) {
	b := NewMySQLBackend()
	if err := b.SetReplicationCredentials("", "p"); err == nil {
		t.Fatal("empty user must error")
	}
	if err := b.SetReplicationCredentials("repl", "p"); err != nil {
		t.Fatalf("valid credentials: %v", err)
	}
	if b.replUser != "repl" {
		t.Fatal("credentials not stored")
	}
}

func TestSetClusterAndTag(t *testing.T) {
	b := NewMySQLBackend()
	b.host, b.port = "h", 3306

	if got := b.tag(); got != "h:3306" {
		t.Fatalf("tag without cluster wrong: %q", got)
	}
	b.SetCluster("c")
	if got := b.tag(); got != "c h:3306" {
		t.Fatalf("tag with cluster wrong: %q", got)
	}
}

func TestPing(t *testing.T) {
	b := NewMySQLBackend()
	if ok, err := b.Ping(context.Background()); err == nil || ok {
		t.Fatal("nil db must error")
	}

	b2, mock := newMockBackend(t)
	mock.ExpectPing()
	ok, err := b2.Ping(context.Background())
	if !ok || err != nil {
		t.Fatalf("ping want true, got %v %v", ok, err)
	}
}

func TestConfigureReuse(t *testing.T) {
	b, mock := newMockBackend(t) // b.db 已指向 mock，host/port 已设置。
	mock.ExpectPing()
	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))

	if err := b.Configure("127.0.0.1", 3306, "u", "p"); err != nil {
		t.Fatalf("Configure reuse: %v", err)
	}
	if b.version != "8.0.36" {
		t.Fatalf("version not recorded: %q", b.version)
	}
}

func TestVersionSyntaxHelpers(t *testing.T) {
	cases := map[string]bool{
		"": true, "8.0.36": true, "9.5.0": true, "5.7.42": false, "bad": true,
	}
	b := NewMySQLBackend()
	for v, want := range cases {
		b.version = v
		if got := b.useReplicaSyntax(); got != want {
			t.Errorf("version %q useReplica want %v got %v", v, want, got)
		}
	}
	if !isSyntaxError(errMy("Error 1064 x")) {
		t.Error("isSyntaxError should detect 1064")
	}
	if isSyntaxError(nil) {
		t.Error("nil should not be syntax error")
	}
}

func TestQuoteSQLString(t *testing.T) {
	if got := quoteSQLString("a'b\\c"); got != `'a''b\\c'` {
		t.Fatalf("quote wrong: %q", got)
	}
}

func TestIntervalAndGTIDHelpers(t *testing.T) {
	if got := intervalsString(gomysql.IntervalSlice{
		{Start: 1, Stop: 3}, {Start: 5, Stop: 6},
	}); got != "1-2,5" {
		t.Fatalf("intervalsString wrong: %q", got)
	}

	source := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	local := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	srcSet, _ := gomysql.ParseGTIDSet("mysql", source)
	locSet, _ := gomysql.ParseGTIDSet("mysql", local)
	uuid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	eq, err := sameUUIDIntervals(locSet, srcSet, uuid)
	if err != nil || !eq {
		t.Fatalf("equal sets should compare true: eq=%v err=%v", eq, err)
	}

	if iv := intervalsOf(srcSet, uuid); len(iv) != 1 || iv[0].Start != 1 || iv[0].Stop != 11 {
		t.Fatalf("intervalsOf wrong: %+v", iv)
	}
	if iv := intervalsOf(srcSet, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"); iv != nil {
		t.Fatalf("missing uuid should be nil: %+v", iv)
	}
}

func TestSameUUIDIntervalsLagging(t *testing.T) {
	source := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	local := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-5"
	srcSet, _ := gomysql.ParseGTIDSet("mysql", source)
	locSet, _ := gomysql.ParseGTIDSet("mysql", local)

	eq, err := sameUUIDIntervals(locSet, srcSet,
		"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	if err != nil || eq {
		t.Fatalf("lagging set should be unequal: eq=%v err=%v", eq, err)
	}

	// 源集合缺少该 UUID → 报错。
	if _, err := sameUUIDIntervals(srcSet, locSet,
		"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"); err == nil {
		t.Fatal("missing source uuid should error")
	}
}

func TestGetStatusQueryError(t *testing.T) {
	b, mock := newMockBackend(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES").WillReturnError(errors.New("boom"))
	if _, err := b.GetStatus(context.Background()); err == nil {
		t.Fatal("query error should propagate")
	}
}

// ---- 小工具 ----

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// errMy 返回一个包含给定文本的普通错误，模拟驱动报错。
func errMy(text string) error { return errors.New(text) }

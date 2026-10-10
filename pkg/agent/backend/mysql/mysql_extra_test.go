// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	_ "github.com/yunnnnn1/go-helix/pkg/db/drivers/mysql" // 注册 MySQL 方言 / 驱动（openSQLDB 需要）
)

// 未使用的端口，连接立即被拒绝（不会长时间等待）。
const deadPort = 59999

func TestClone(t *testing.T) {
	if got := NewMySQLBackend().Clone(); got == nil {
		t.Fatal("Clone returned nil")
	}
}

func TestOpenSQLDB(t *testing.T) {
	db, err := openSQLDB("127.0.0.1", 3306, "u", "p")
	if err != nil {
		t.Fatalf("openSQLDB (no connect) should not error: %v", err)
	}
	db.Close()
}

func TestConfigureNewHostPingRefused(t *testing.T) {
	b := NewMySQLBackend()
	err := b.Configure("127.0.0.1", deadPort, "u", "p")
	if err == nil || !contains(err.Error(), "business account connect") {
		t.Fatalf("want connect error, got %v", err)
	}
	if b.db == nil {
		t.Fatal("pool should be retained even when ping fails")
	}
}

func TestConfigureManagerErrors(t *testing.T) {
	b := NewMySQLBackend()
	if err := b.ConfigureManager("h", 1, "", "p"); err == nil {
		t.Fatal("empty user must error")
	}

	b2 := NewMySQLBackend()
	err := b2.ConfigureManager("127.0.0.1", deadPort, "mgr", "p")
	if err == nil || !contains(err.Error(), "manager account connect") {
		t.Fatalf("want connect error, got %v", err)
	}
}

func TestDiscover(t *testing.T) {
	b := NewMySQLBackend()
	instances, err := b.Discover()
	if err != nil {
		t.Fatalf("Discover should not hard-fail: %v", err)
	}
	// 在无本机 mysqld 的环境下返回空列表即可（主要覆盖命令执行/解析路径）。
	_ = instances
}

func TestKillProcessOS(t *testing.T) {
	b := NewMySQLBackend()
	// 不存在的 PID：kill 返回错误，覆盖 OS 分支而不会杀掉任何真实进程。
	if err := b.KillProcess(context.Background(), 88888888, true); err == nil {
		t.Fatal("kill of bogus pid should error")
	}
}

func TestExecReplicationAdminErrors(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := NewMySQLBackend()

	// 主语句硬错误（非语法）：直接返回，不回退。
	mock.ExpectExec("STOP REPLICA").WillReturnError(errors.New("boom"))
	if err := b.execReplicationAdmin(context.Background(), db, "STOP REPLICA", "STOP SLAVE"); err == nil {
		t.Fatal("want hard error")
	}

	// 语法回退后旧语句仍失败。
	mock.ExpectExec("STOP REPLICA").WillReturnError(errMy("Error 1064 syntax"))
	mock.ExpectExec("STOP SLAVE").WillReturnError(errors.New("legacy boom"))
	if err := b.execReplicationAdmin(context.Background(), db, "STOP REPLICA", "STOP SLAVE"); err == nil {
		t.Fatal("want fallback failure")
	}
}

func TestEnableSemiSyncAllInstallFail(t *testing.T) {
	b, mock := newMockBackend(t)
	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_enabled").
		WillReturnError(errMy("Error 1193 unknown"))
	mock.ExpectExec("INSTALL PLUGIN rpl_semi_sync_source").
		WillReturnError(errors.New("install1 fail"))
	mock.ExpectExec("INSTALL PLUGIN rpl_semi_sync_master").
		WillReturnError(errors.New("install2 fail"))

	if err := b.enableSemiSyncSource(context.Background(), b.managerDB); err == nil {
		t.Fatal("want error when all install attempts fail")
	}
}

func TestSetGlobalAndVerifyErrors(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// SET 失败。
	mock.ExpectExec("SET GLOBAL read_only").WillReturnError(errors.New("set boom"))
	if err := setGlobalAndVerify(context.Background(), db, "read_only", true); err == nil {
		t.Fatal("want set error")
	}

	// 回读查询失败。
	mock.ExpectExec("SET GLOBAL read_only").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT @@global.read_only").WillReturnError(errors.New("query boom"))
	if err := setGlobalAndVerify(context.Background(), db, "read_only", true); err == nil {
		t.Fatal("want verify query error")
	}
}

func TestIntervalsOfBadUUID(t *testing.T) {
	set, err := gomysql.ParseGTIDSet("mysql", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	if err != nil {
		t.Fatal(err)
	}
	if iv := intervalsOf(set, "not-a-uuid"); iv != nil {
		t.Fatalf("bad uuid should return nil, got %+v", iv)
	}
}

func TestOpSlaveExecError(t *testing.T) {
	b, mock := newMockBackend(t)
	mock.ExpectExec("STOP REPLICA").WillReturnError(errors.New("boom"))
	if _, err := b.ExecuteOperation(context.Background(), "StopSlave", nil); err == nil {
		t.Fatal("want exec error")
	}
}

// TestWaitCatchupThreadNotRunning：副本 IO 线程停止且超过宽限期后返回错误。
func TestWaitCatchupThreadNotRunning(t *testing.T) {
	old := catchupIOThreadGrace
	catchupIOThreadGrace = 120 * time.Millisecond
	defer func() { catchupIOThreadGrace = old }()

	b, mock := newMockBackend(t)
	row := sqlmock.NewRows([]string{
		"Replica_IO_Running", "Replica_SQL_Running", "Last_IO_Error", "Last_SQL_Error",
	}).AddRow("No", "Yes", "io err", "")
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(row)

	_, err := b.ExecuteOperation(context.Background(), "WaitCatchup", nil)
	if err == nil || !contains(err.Error(), "thread not running") {
		t.Fatalf("want thread error, got %v", err)
	}
}

// TestWaitCatchupStatusReadError：读取副本状态持续失败且超过宽限期。
func TestWaitCatchupStatusReadError(t *testing.T) {
	old := catchupStatusGrace
	catchupStatusGrace = 100 * time.Millisecond
	defer func() { catchupStatusGrace = old }()

	b, mock := newMockBackend(t)
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnError(errors.New("repl boom"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnError(errors.New("slave boom"))

	_, err := b.ExecuteOperation(context.Background(), "WaitCatchup", nil)
	if err == nil || !contains(err.Error(), "read replica status failed") {
		t.Fatalf("want status read error, got %v", err)
	}
}

// TestGetReplicaStatusRowSlaveFallback：REPLICA 查询失败后 SLAVE 成功。
func TestGetReplicaStatusRowSlaveFallback(t *testing.T) {
	b, mock := newMockBackend(t)
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnError(errors.New("repl unsupported"))
	rows := sqlmock.NewRows([]string{"Slave_IO_Running"}).AddRow("Yes")
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(rows)

	row, hasRow, err := b.getReplicaStatusRow(context.Background(), b.managerDB)
	if err != nil || !hasRow {
		t.Fatalf("want row via slave fallback: hasRow=%v err=%v", hasRow, err)
	}
	if row["Slave_IO_Running"] != "Yes" {
		t.Fatalf("row content wrong: %+v", row)
	}
}

func TestPingErrorSwallowed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := NewMySQLBackend()
	b.db = db
	mock.ExpectPing().WillReturnError(errors.New("ping boom"))
	ok, err := b.Ping(context.Background())
	if ok || err != nil {
		t.Fatalf("Ping swallows errors, want false/nil got %v/%v", ok, err)
	}
}

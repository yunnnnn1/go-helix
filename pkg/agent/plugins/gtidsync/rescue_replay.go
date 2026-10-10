// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package gtidsync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	gtidsql "github.com/yunnnnn1/go-helix/pkg/agent/plugins/gtidsync/sqlbuilder"
	log "github.com/yunnnnn1/go-helix/pkg/logging"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

const (
	// actionReplayRescue 通过离线解析文件（不要求源端有运行中的 mysqld），
	// 重放一段已保存的本地二进制日志片段。
	actionReplayRescue = "ReplayRescue"
)

// errRescueBoundary 是流越过目标快照后事件回调返回的哨兵错误；
// 它用于干净地停止 ParseReader。
var errRescueBoundary = errors.New("rescue target boundary reached")

func (p *GTIDSyncPlugin) replayRescue(ctx context.Context, params map[string]string) (map[string]string, error) {
	fragmentPath := strings.TrimSpace(params["fragment_path"])
	if fragmentPath == "" {
		return nil, fmt.Errorf("gtid_sync ReplayRescue: fragment_path is required")
	}
	targetGTID := strings.TrimSpace(params["target_gtid"])
	if targetGTID == "" {
		return nil, fmt.Errorf("gtid_sync ReplayRescue: target_gtid is required")
	}

	localAddr := joinPort(params["db_host"], params["db_port"])
	user, pass := params["manager_user"], params["manager_pass"]
	if user == "" {
		user, pass = params["db_user"], params["db_pass"]
	}

	localDB, err := plugins.DefaultPool().Get(ctx, pluginDBType, localAddr, user, pass)
	if err != nil {
		return nil, fmt.Errorf("connect candidate %s: %w", localAddr, err)
	}

	targetSet, err := mysql.ParseGTIDSet("mysql", targetGTID)
	if err != nil {
		return nil, fmt.Errorf("parse target gtid: %w", err)
	}

	var candidateStr string
	if err := localDB.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&candidateStr); err != nil {
		return nil, fmt.Errorf("read candidate gtid_executed: %w", err)
	}
	candidateSet, err := mysql.ParseGTIDSet("mysql", candidateStr)
	if err != nil {
		return nil, fmt.Errorf("parse candidate gtid: %w", err)
	}
	log.Printf("[Plugin:GTIDSync] ReplayRescue fragment=%s target=%s candidate=%s", fragmentPath, targetGTID, candidateStr)

	f, err := os.Open(fragmentPath)
	if err != nil {
		return nil, fmt.Errorf("open fragment %s: %w", fragmentPath, err)
	}
	defer f.Close()

	conn, err := localDB.Conn(ctx) // 单连接：gtid_next 与事务必须共用它
	if err != nil {
		return nil, fmt.Errorf("get candidate connection: %w", err)
	}
	defer conn.Close()

	player := &rescuePlayer{
		ctx:          ctx,
		conn:         conn,
		localDB:      localDB,
		targetSet:    targetSet,
		candidateSet: candidateSet,
		schemaCache:  map[string]*schema.Table{},
	}

	binlogParser := replication.NewBinlogParser()
	perr := binlogParser.ParseReader(f, player.onEvent)
	if perr != nil && !errors.Is(perr, errRescueBoundary) && !isTruncatedTail(perr) {
		_ = player.cleanup()
		return nil, fmt.Errorf("parse rescue fragment: %w", perr)
	}
	if err := player.cleanup(); err != nil {
		return nil, err
	}

	// 应用后校验：候选实例的 executed 集合此时必须包含整个补救目标。
	var executedStr string
	if err := localDB.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&executedStr); err != nil {
		return nil, fmt.Errorf("read post-rescue gtid_executed: %w", err)
	}
	executedSet, err := mysql.ParseGTIDSet("mysql", executedStr)
	if err != nil {
		return nil, fmt.Errorf("parse post-rescue gtid: %w", err)
	}
	if !executedSet.Contain(targetSet) {
		return nil, fmt.Errorf("rescue applied but candidate gtid_executed %s does not contain target %s", executedStr, targetGTID)
	}

	log.Printf("[Plugin:GTIDSync] ReplayRescue verified applied=%d gtid_executed=%s", player.applied, executedStr)
	return map[string]string{
		"message":       "rescue fragment applied and verified",
		"applied":       strconv.Itoa(player.applied),
		"gtid_executed": executedStr,
	}, nil
}

// rescuePlayer 通过单条候选连接驱动一段二进制日志片段的离线重放。
type rescuePlayer struct {
	ctx     context.Context
	conn    *sql.Conn
	localDB *sql.DB

	targetSet    mysql.GTIDSet // 故障主库的 executed 集合（上界）
	candidateSet mysql.GTIDSet // 重放前候选实例的 executed 集合
	schemaCache  map[string]*schema.Table

	// 按事务记录的状态
	skipping  bool // 事务已存在于候选实例上
	gtidArmed bool // 已设置 gtid_next，等待其对应事务
	inTxn     bool // 已用 BEGIN 开启的 DML 事务
	applied   int
}

// onEvent 是 BinlogParser 的回调函数。
func (pl *rescuePlayer) onEvent(ev *replication.BinlogEvent) error {
	switch e := ev.Event.(type) {

	case *replication.GTIDEvent:
		gset, err := e.GTIDNext()
		if err != nil {
			return fmt.Errorf("resolve rescue gtid: %w", err)
		}

		// 不在所捕获目标内的事务比需要追平的位点更新 -> 在重放它之前停止。
		if !pl.targetSet.Contain(gset) {
			log.Printf("[Plugin:GTIDSync] rescue passed target boundary at %s", gset.String())
			return errRescueBoundary
		}
		// 事务已在候选实例上执行 -> 跳过整个事务。
		if pl.candidateSet.Contain(gset) {
			pl.skipping = true
			return nil
		}

		pl.skipping = false
		pl.gtidArmed = true
		return pl.stmt("SET gtid_next = ?", gset.String())

	case *replication.QueryEvent:
		if pl.skipping {
			pl.skipping = false
			return nil
		}
		query := strings.TrimSpace(string(e.Query))
		if query == "" {
			return nil
		}
		// GTID 二进制日志中的 DML 事务带有显式的 BEGIN。
		if strings.EqualFold(query, "BEGIN") {
			if !pl.gtidArmed {
				return nil // 没有 GTID 上下文：没有需要重放的内容
			}
			pl.inTxn = true
			return pl.stmt("BEGIN")
		}

		// 否则是一条 DDL 语句（自动提交，并消耗已待命的 GTID）。
		if !pl.gtidArmed {
			// GTID 组之外的语句（非事务性）：忽略。
			return nil
		}
		log.Printf("[Plugin:GTIDSync] rescue replaying DDL: %s", query)
		if err := pl.stmt(query); err != nil {
			return fmt.Errorf("rescue DDL failed (%s): %w", query, err)
		}
		pl.gtidArmed = false
		pl.applied++
		return pl.stmt("SET gtid_next = 'AUTOMATIC'")

	case *replication.RowsEvent:
		if pl.skipping {
			return nil
		}
		return pl.handleRows(ev.Header, e)

	case *replication.XIDEvent:
		if pl.skipping {
			pl.skipping = false
			return nil
		}
		// 关闭 DML 事务并释放已分配的 GTID。
		if pl.inTxn {
			if err := pl.stmt("COMMIT"); err != nil {
				return fmt.Errorf("rescue COMMIT failed: %w", err)
			}
			pl.inTxn = false
		}
		pl.gtidArmed = false
		pl.applied++
		return pl.stmt("SET gtid_next = 'AUTOMATIC'")
	}
	return nil
}

// handleRows 将一个 RowsEvent 解码为针对候选实例重放的 SQL，
// 复用共享的 SQL 构建器。
func (pl *rescuePlayer) handleRows(header *replication.EventHeader, e *replication.RowsEvent) error {
	tm := e.Table
	if tm == nil {
		return fmt.Errorf("rescue rows event has no table map (tableID=%d)", e.TableID)
	}
	dbName, tableName := string(tm.Schema), string(tm.Table)

	table, err := pl.lookupSchema(dbName, tableName)
	if err != nil {
		return err
	}

	// 仿照 canal 的无符号处理，使无符号列按有符号负数解码出的值被还原为
	// 其真实的无符号值后再回送。
	convertUnsigned(e.Rows, table)

	apply := func(build func() (string, []interface{}, error)) error {
		s, args, bErr := build()
		if bErr != nil {
			log.Printf("[Plugin:GTIDSync] rescue SQL build failed: %v", bErr)
			return nil
		}
		if err := pl.stmt(s, args...); err != nil {
			return fmt.Errorf("rescue execute %s: %w", s, err)
		}
		return nil
	}

	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		for _, row := range e.Rows {
			if err := apply(func() (string, []interface{}, error) {
				return gtidsql.BuildInsertSql(table, row)
			}); err != nil {
				return err
			}
		}

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			idx := i
			if err := apply(func() (string, []interface{}, error) {
				return gtidsql.BuildUpdateSql(table, e.Rows[idx], e.Rows[idx+1])
			}); err != nil {
				return err
			}
		}

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
		replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		for _, row := range e.Rows {
			if err := apply(func() (string, []interface{}, error) {
				return gtidsql.BuildDeleteSql(table, row)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// lookupSchema 返回缓存的 schema.Table，或从候选实例加载其列/索引
// （提升为主库期间候选 mysqld 处于运行状态）。
func (pl *rescuePlayer) lookupSchema(dbName, tableName string) (*schema.Table, error) {
	key := dbName + "." + tableName
	if t, ok := pl.schemaCache[key]; ok {
		return t, nil
	}
	t, err := schema.NewTableFromSqlDB(pl.localDB, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("load schema for %s: %w", key, err)
	}
	pl.schemaCache[key] = t
	return t, nil
}

// stmt 在固定的候选连接上执行一条语句。
func (pl *rescuePlayer) stmt(query string, args ...interface{}) error {
	if _, err := pl.conn.ExecContext(pl.ctx, query, args...); err != nil {
		log.Printf("[Plugin:GTIDSync] rescue stmt failed: %q err=%v", query, err)
		return err
	}
	return nil
}

// cleanup 回滚 / 释放因片段在事务中途结束（例如尾部截断）而残留的
// 任何事务或 GTID。
func (pl *rescuePlayer) cleanup() error {
	if pl.inTxn {
		if _, err := pl.conn.ExecContext(pl.ctx, "ROLLBACK"); err != nil {
			return fmt.Errorf("rescue rollback open txn: %w", err)
		}
		pl.inTxn = false
	}
	if pl.gtidArmed {
		if _, err := pl.conn.ExecContext(pl.ctx, "SET gtid_next = 'AUTOMATIC'"); err != nil {
			return fmt.Errorf("rescue reset gtid_next: %w", err)
		}
		pl.gtidArmed = false
	}
	return nil
}

// convertUnsigned 将无符号列解码出的有符号值改写为对应的无符号值，
// 与 canal RowsEvent.handleUnsigned 保持一致。
func convertUnsigned(rows [][]interface{}, table *schema.Table) {
	if len(table.UnsignedColumns) == 0 {
		return
	}
	for i := 0; i < len(rows); i++ {
		for _, columnIdx := range table.UnsignedColumns {
			if columnIdx >= len(rows[i]) {
				continue
			}
			switch value := rows[i][columnIdx].(type) {
			case int8:
				rows[i][columnIdx] = uint8(value)
			case int16:
				rows[i][columnIdx] = uint16(value)
			case int32:
				if table.Columns[columnIdx].Type == schema.TYPE_MEDIUM_INT {
					rows[i][columnIdx] = uint32(16777215 + value + 1)
				} else {
					rows[i][columnIdx] = uint32(value)
				}
			case int64:
				rows[i][columnIdx] = uint64(value)
			case int:
				rows[i][columnIdx] = uint(value)
			}
		}
	}
}

// isTruncatedTail 判断某个错误是否为在线二进制日志文件最末尾的半个事件
// （崩溃可能导致最后一个事件不完整）。
func isTruncatedTail(err error) bool {
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "EOF") || strings.Contains(s, "unexpected EOF")
}

// joinPort 构造 host:port，可兼容端口为空的情况。
func joinPort(host, port string) string {
	if port == "" {
		return host
	}
	return host + ":" + port
}

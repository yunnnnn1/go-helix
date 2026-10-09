package datacenter

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/yunnnnn1/go-helix/pkg/logging"
)

// sqliteStore 基于内嵌 SQLite 文件实现 MetaStore。它使用
// 可移植 SQL（CASE 聚合、由 Go 计算时间阈值），并将时间戳以
// 定宽 UTC 文本存储，使其可按字典序比较。
type sqliteStore struct {
	db *sql.DB
}

// tsLayout 是定宽、可按字典序排序的 UTC 时间戳格式。不含
// 小数秒（心跳粒度为秒）。
const tsLayout = "2006-01-02T15:04:05Z"

func formatTS(t time.Time) string { return t.UTC().Format(tsLayout) }

func parseTS(s string) (time.Time, error) {
	return time.Parse(tsLayout, s)
}

func (s *sqliteStore) Close() error { return s.db.Close() }

// EnsureSchema 以幂等方式创建元数据表。
func (s *sqliteStore) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS meta_data (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			cluster_name    TEXT NOT NULL DEFAULT '',
			role            TEXT NOT NULL DEFAULT '',
			ip              TEXT NOT NULL DEFAULT '',
			port            INTEGER NOT NULL DEFAULT 0,
			agent_port      INTEGER NOT NULL DEFAULT 0,
			master_host     TEXT NOT NULL DEFAULT '',
			master_port     INTEGER NOT NULL DEFAULT 0,
			dbtype          TEXT NOT NULL DEFAULT '',
			db_version      TEXT NOT NULL DEFAULT '',
			zone            TEXT NOT NULL DEFAULT '',
			region          TEXT NOT NULL DEFAULT '',
			idc             TEXT NOT NULL DEFAULT '',
			vip             TEXT NOT NULL DEFAULT '',
			priority        INTEGER NOT NULL DEFAULT 0,
			is_deleted      INTEGER NOT NULL DEFAULT 0,
			env             TEXT NOT NULL DEFAULT '',
			create_time     TEXT NOT NULL DEFAULT '',
			update_time     TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'PENDING',
			disk_usage_pct  REAL NOT NULL DEFAULT 0,
			load_value      REAL NOT NULL DEFAULT 0,
			gtid_executed   TEXT NOT NULL DEFAULT '',
			binlog_file     TEXT NOT NULL DEFAULT '',
			binlog_pos      INTEGER NOT NULL DEFAULT 0,
			binlog_dir      TEXT NOT NULL DEFAULT '',
			binlog_basename TEXT NOT NULL DEFAULT '',
			rescue_enabled  INTEGER NOT NULL DEFAULT 0,
			rescue_access_roots TEXT NOT NULL DEFAULT '[]',
			rescue_max_bytes   INTEGER NOT NULL DEFAULT 0,
			io_thread_running  INTEGER NOT NULL DEFAULT 0,
			sql_thread_running INTEGER NOT NULL DEFAULT 0,
			semi_sync_enabled  INTEGER NOT NULL DEFAULT 0,
			seconds_behind     INTEGER NOT NULL DEFAULT 0,
			using_gtid         INTEGER NOT NULL DEFAULT 0,
			UNIQUE(ip, port)
		)`,
		`CREATE TABLE IF NOT EXISTS switch_history (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			cluster_name  TEXT NOT NULL DEFAULT '',
			operation     TEXT NOT NULL DEFAULT '',
			recover_mode  TEXT NOT NULL DEFAULT '',
			old_master    TEXT NOT NULL DEFAULT '',
			new_master    TEXT NOT NULL DEFAULT '',
			analysis_code TEXT NOT NULL DEFAULT '',
			result        TEXT NOT NULL DEFAULT '',
			create_time   TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS topology_snapshot (
			id            INTEGER PRIMARY KEY AUTOINCREMENT,
			cluster_name  TEXT NOT NULL DEFAULT '',
			edges_json    TEXT NOT NULL DEFAULT '',
			create_time   TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS cluster_meta (
			cluster_name     TEXT PRIMARY KEY,
			cluster_priority TEXT NOT NULL DEFAULT 'P3',
			registered       INTEGER NOT NULL DEFAULT 0,
			endpoint_type    TEXT NOT NULL DEFAULT 'none',
			endpoint_value   TEXT NOT NULL DEFAULT '',
			remark           TEXT NOT NULL DEFAULT '',
			create_time      TEXT NOT NULL DEFAULT '',
			update_time      TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, st := range stmts {
		if _, err := s.db.ExecContext(ctx, st); err != nil {
			return fmt.Errorf("sqlite ensure schema: %w", err)
		}
	}
	return nil
}

// scanSQLiteNode 映射一行 meta_data 记录，并解析 TEXT 类型的时间戳。
func scanSQLiteNode(scanner interface {
	Scan(dest ...interface{}) error
}) (*Node, error) {
	var n Node
	var createStr, updateStr string
	err := scanner.Scan(&n.ID, &n.ClusterName, &n.Role, &n.Host, &n.Port, &n.AgentPort, &n.MasterHost, &n.MasterPort,
		&n.DbType, &n.DbVersion, &n.Zone, &n.Region, &n.IDC, &n.Vip, &n.Priority, &n.IsDeleted, &n.Env,
		&createStr, &updateStr, &n.Status)
	if err != nil {
		return nil, err
	}
	if createStr != "" {
		if t, pErr := parseTS(createStr); pErr == nil {
			n.CreateTime = t
		}
	}
	if updateStr != "" {
		if t, pErr := parseTS(updateStr); pErr == nil {
			n.UpdateTime = t
		}
	}
	n.DC = n.IDC
	return &n, nil
}

func (s *sqliteStore) GetStaleNodes(ctx context.Context, role string, olderThan time.Duration) ([]*Node, error) {
	// 截止时间在 Go 中计算；TEXT 时间戳按字典序比较。
	cutoff := formatTS(time.Now().Add(-olderThan))
	query := fmt.Sprintf("SELECT %s FROM meta_data WHERE is_deleted = 0 AND status <> 'PENDING' AND update_time < ?", nodeSelectCols)
	args := []interface{}{cutoff}
	if role != "" {
		query += " AND role = ?"
		args = append(args, role)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*Node
	for rows.Next() {
		n, err := scanSQLiteNode(rows)
		if err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		nodes = append(nodes, n)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (s *sqliteStore) GetClusterNodes(ctx context.Context, clusterName string) ([]*Node, error) {
	query := fmt.Sprintf("SELECT %s FROM meta_data WHERE cluster_name = ? AND is_deleted = 0", nodeSelectCols)

	rows, err := s.db.QueryContext(ctx, query, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*Node
	for rows.Next() {
		n, err := scanSQLiteNode(rows)
		if err != nil {
			continue
		}
		nodes = append(nodes, n)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (s *sqliteStore) ListClusters(ctx context.Context) ([]*ClusterSummary, error) {
	// 可移植聚合：布尔值不会隐式当作 0/1，因此使用 CASE。
	query := `
		SELECT cluster_name,
		       COUNT(*)                                              AS node_count,
		       SUM(CASE WHEN role = 'master' THEN 1 ELSE 0 END)     AS master_count,
		       SUM(CASE WHEN role = 'slave'  THEN 1 ELSE 0 END)     AS slave_count,
		       SUM(CASE WHEN status = 'ONLINE' THEN 1 ELSE 0 END)   AS online_count,
		       MAX(update_time)                                     AS last_heartbeat
		FROM meta_data
		WHERE is_deleted = 0
		GROUP BY cluster_name
		ORDER BY cluster_name`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*ClusterSummary
	for rows.Next() {
		var name string
		var nodeCount, masterCount, slaveCount, onlineCount sql.NullInt64
		var lastStr sql.NullString
		if err := rows.Scan(&name, &nodeCount, &masterCount, &slaveCount, &onlineCount, &lastStr); err != nil {
			log.Printf("ListClusters scan error: %v", err)
			continue
		}
		var lastHB time.Time
		if lastStr.Valid && lastStr.String != "" {
			lastHB, _ = parseTS(lastStr.String)
		}
		nc, mc, oc := int(nodeCount.Int64), int(masterCount.Int64), int(onlineCount.Int64)
		out = append(out, &ClusterSummary{
			ClusterName:   name,
			NodeCount:     nc,
			MasterCount:   mc,
			SlaveCount:    int(slaveCount.Int64),
			OnlineCount:   oc,
			Healthy:       mc == 1 && oc == nc,
			LastHeartbeat: lastHB,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	metas, mErr := s.ListClusterMetas(ctx)
	if mErr != nil {
		log.Printf("ListClusters: failed to read cluster_meta: %v (defaults used)", mErr)
	}
	mergeClusterMeta(out, metas)
	return out, nil
}

// scanSQLiteClusterMeta 映射一行 cluster_meta 记录。时间戳以文本
// 读取并在 Go 中解析（与 scanSQLiteNode 类似，直接扫描进 time.Time 时
// sqlite 驱动可能不接受我们的格式）。
func scanSQLiteClusterMeta(scanner interface {
	Scan(dest ...interface{}) error
}) (ClusterMeta, error) {
	var m ClusterMeta
	var priority string
	var registered int
	var createStr, updateStr string
	if err := scanner.Scan(&m.ClusterName, &priority, &registered,
		&m.EndpointType, &m.EndpointValue, &m.Remark, &createStr, &updateStr); err != nil {
		return ClusterMeta{}, err
	}
	p, err := ParseClusterPriority(priority)
	if err != nil {
		return ClusterMeta{}, err
	}
	m.ClusterPriority = p
	m.Registered = registered != 0
	if createStr != "" {
		if t, pErr := parseTS(createStr); pErr == nil {
			m.CreateTime = t
		}
	}
	if updateStr != "" {
		if t, pErr := parseTS(updateStr); pErr == nil {
			m.UpdateTime = t
		}
	}
	return m, nil
}

func (s *sqliteStore) GetClusterMeta(ctx context.Context, cluster string) (ClusterMeta, error) {
	query := `SELECT ` + clusterMetaCols + ` FROM cluster_meta WHERE cluster_name = ?`
	m, err := scanSQLiteClusterMeta(s.db.QueryRowContext(ctx, query, cluster))
	if err == sql.ErrNoRows {
		return DefaultClusterMeta(cluster), nil
	}
	return m, err
}

func (s *sqliteStore) ListClusterMetas(ctx context.Context) (map[string]ClusterMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+clusterMetaCols+` FROM cluster_meta`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]ClusterMeta)
	for rows.Next() {
		m, err := scanSQLiteClusterMeta(rows)
		if err != nil {
			log.Printf("ListClusterMetas scan error: %v", err)
			continue
		}
		out[m.ClusterName] = m
	}
	return out, rows.Err()
}

func (s *sqliteStore) SetClusterPriority(ctx context.Context, cluster string, p ClusterPriority) error {
	now := formatTS(time.Now())
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cluster_meta
		  (cluster_name, cluster_priority, registered, endpoint_type, endpoint_value, remark, create_time, update_time)
		VALUES (?, ?, 0, 'none', '', '', ?, ?)
		ON CONFLICT(cluster_name) DO UPDATE
		  SET cluster_priority = excluded.cluster_priority, update_time = excluded.update_time`,
		cluster, string(p), now, now)
	return err
}

func (s *sqliteStore) RegisterCluster(ctx context.Context, m ClusterMeta) error {
	endpointType, err := NormalizeEndpointType(m.EndpointType)
	if err != nil {
		return err
	}
	now := formatTS(time.Now())
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO cluster_meta
		  (cluster_name, cluster_priority, registered, endpoint_type, endpoint_value, remark, create_time, update_time)
		VALUES (?, ?, 1, ?, ?, ?, ?, ?)
		ON CONFLICT(cluster_name) DO UPDATE
		  SET cluster_priority = excluded.cluster_priority,
		      registered = 1,
		      endpoint_type = excluded.endpoint_type,
		      endpoint_value = excluded.endpoint_value,
		      remark = excluded.remark,
		      update_time = excluded.update_time`,
		m.ClusterName, string(m.ClusterPriority), endpointType, m.EndpointValue, m.Remark, now, now)
	return err
}

func (s *sqliteStore) ResolveClusterName(ctx context.Context, hb Heartbeat) (string, error) {
	rs := hb.replica()

	// 1. 自身已注册 -> 保留其集群
	if name := s.lookupClusterName(ctx, "SELECT cluster_name FROM meta_data WHERE ip = ? AND port = ? AND is_deleted = 0 LIMIT 1", hb.Host, hb.Port); name != "" {
		return name, nil
	}

	// 2. 上游已注册 -> 归属到上游的集群
	if rs.UpstreamHost != "" {
		if name := s.lookupClusterName(ctx, "SELECT cluster_name FROM meta_data WHERE ip = ? AND port = ? AND is_deleted = 0 LIMIT 1", rs.UpstreamHost, int(rs.UpstreamPort)); name != "" {
			return name, nil
		}
	}

	// 3. 指向我的下游 -> 归属到该集群
	if name := s.lookupClusterName(ctx, "SELECT cluster_name FROM meta_data WHERE master_host = ? AND master_port = ? AND is_deleted = 0 LIMIT 1", hb.Host, hb.Port); name != "" {
		return name, nil
	}

	// 4. 全新的组 -> 分配下一个 clusterN
	return s.allocateNextClusterName(ctx), nil
}

func (s *sqliteStore) lookupClusterName(ctx context.Context, query string, args ...interface{}) string {
	var name string
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&name); err == nil {
		return name
	}
	return ""
}

func (s *sqliteStore) allocateNextClusterName(ctx context.Context) string {
	rows, err := s.db.QueryContext(ctx, "SELECT cluster_name FROM meta_data WHERE cluster_name <> ''")
	if err != nil {
		return "cluster1"
	}
	defer rows.Close()

	maxN := 0
	for rows.Next() {
		var cn string
		if rows.Scan(&cn) == nil {
			if n := matchClusterN(cn); n > maxN {
				maxN = n
			}
		}
	}
	return fmt.Sprintf("cluster%d", maxN+1)
}

func (s *sqliteStore) UpsertHeartbeat(ctx context.Context, hb Heartbeat) error {
	rs := hb.replica()
	now := formatTS(time.Now())
	// 节点上报的真实 binlog 路径随心跳写入 meta_data，与 binlog_file/
	// binlog_pos 同表收敛。
	binDir, binBase := splitBinlogBasename(rs.Extra["log_bin_basename"])

	updateQuery := `UPDATE meta_data SET update_time = ?, status = 'ONLINE', role = ?, db_version = ?,
	disk_usage_pct = ?, load_value = ?,
	gtid_executed = ?, binlog_file = ?, binlog_pos = ?,
	binlog_dir = ?, binlog_basename = ?,
	io_thread_running = ?, sql_thread_running = ?,
	semi_sync_enabled = ?, seconds_behind = ?, using_gtid = ?,
	agent_port = ?, master_host = ?, master_port = ?
	WHERE ip = ? AND port = ? AND is_deleted = 0`

	res, err := s.db.ExecContext(ctx, updateQuery, now, hb.Role, hb.Version,
		hb.DiskUsage, hb.Load,
		rs.gtidExecuted(), rs.binlogFile(), rs.binlogPos(),
		binDir, binBase,
		rs.ReceiverRunning, rs.ApplierRunning,
		rs.SyncReplication, rs.LagSeconds, rs.usingGTID(),
		rs.AgentPort, rs.UpstreamHost, rs.UpstreamPort,
		hb.Host, hb.Port)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}

	// 新记录以 PENDING 插入；下一次心跳将其翻转为 ONLINE
	// （与 MySQL 相同的一个周期注册窗口）。rescue_* 策略列取表默认值。
	insertQuery := `INSERT INTO meta_data
	(cluster_name, role, ip, port, agent_port, master_host, master_port,
	 dbtype, db_version, zone, region, idc, vip, priority, env,
	 disk_usage_pct, load_value, gtid_executed, binlog_file, binlog_pos,
	 binlog_dir, binlog_basename,
	 io_thread_running, sql_thread_running, semi_sync_enabled, seconds_behind, using_gtid,
	 is_deleted, status, create_time, update_time)
	VALUES
	(?, ?, ?, ?, ?, ?, ?,
	 ?, ?, '', '', '', '', 0, '',
	 ?, ?, ?, ?, ?,
	 ?, ?,
	 ?, ?, ?, ?, ?,
	 0, 'PENDING', ?, ?)`

	_, err = s.db.ExecContext(ctx, insertQuery,
		hb.Cluster, hb.Role, hb.Host, hb.Port, rs.AgentPort, rs.UpstreamHost, rs.UpstreamPort, hb.DBType, hb.Version,
		hb.DiskUsage, hb.Load, rs.gtidExecuted(), rs.binlogFile(), rs.binlogPos(),
		binDir, binBase,
		rs.ReceiverRunning, rs.ApplierRunning, rs.SyncReplication, rs.LagSeconds, rs.usingGTID(),
		now, now)
	return err
}

func (s *sqliteStore) InsertSwitchRecord(ctx context.Context, rec *SwitchRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO switch_history
		   (cluster_name, operation, recover_mode, old_master, new_master, analysis_code, result, create_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.ClusterName, rec.Operation, rec.RecoverMode,
		rec.OldMaster, rec.NewMaster, rec.AnalysisCode, rec.Result, formatTS(time.Now()))
	return err
}

func (s *sqliteStore) GetLatestSwitch(ctx context.Context, clusterName string) (*SwitchRecord, error) {
	query := `SELECT ` + switchRecordCols + ` FROM switch_history
		WHERE cluster_name = ? AND result = 'SUCCESS' AND (
			operation IN ('failover', 'switchover')
			OR (operation = 'recover' AND recover_mode IN ('topology', 'rollback'))
		)
		ORDER BY id DESC LIMIT 1`

	var rec SwitchRecord
	var createStr string
	err := s.db.QueryRowContext(ctx, query, clusterName).Scan(
		&rec.ID, &rec.ClusterName, &rec.Operation, &rec.RecoverMode,
		&rec.OldMaster, &rec.NewMaster, &rec.AnalysisCode, &rec.Result, &createStr)
	if err != nil {
		return nil, err
	}
	if t, pErr := parseTS(createStr); pErr == nil {
		rec.CreateTime = t
	}
	return &rec, nil
}

// InsertTopologySnapshot 保存切换前的边集合 JSON。
func (s *sqliteStore) InsertTopologySnapshot(ctx context.Context, cluster, edgesJSON string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO topology_snapshot (cluster_name, edges_json, create_time) VALUES (?, ?, ?)`,
		cluster, edgesJSON, formatTS(time.Now()))
	return err
}

// GetLatestTopologySnapshot 返回最近一次保存的边集合 JSON。
func (s *sqliteStore) GetLatestTopologySnapshot(ctx context.Context, cluster string) (string, error) {
	var edgesJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT edges_json FROM topology_snapshot WHERE cluster_name = ? ORDER BY id DESC LIMIT 1`,
		cluster).Scan(&edgesJSON)
	if err != nil {
		return "", err
	}
	return edgesJSON, nil
}

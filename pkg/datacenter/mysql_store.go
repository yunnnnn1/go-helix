// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package datacenter

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/yunnnnn1/go-helix/pkg/logging"
)

// mysqlStore 基于 MySQL 元数据数据库实现 MetaStore。
// P1：SQL 沿用重构前的 MySQL 逻辑；方言可移植性（SQLite）
// 在后续阶段处理。
type mysqlStore struct {
	db *sql.DB
}

func (s *mysqlStore) Close() error { return s.db.Close() }

// EnsureSchema 对 MySQL 为空操作：其 schema 通过
// meta_data_schema.sql 由外部管理（保持先前行为）。
func (s *mysqlStore) EnsureSchema(_ context.Context) error { return nil }

// scanNode 映射一行 meta_data 记录。
func scanNode(scanner interface {
	Scan(dest ...interface{}) error
}) (*Node, error) {
	var n Node
	err := scanner.Scan(&n.ID, &n.ClusterName, &n.Role, &n.Host, &n.Port, &n.AgentPort, &n.MasterHost, &n.MasterPort,
		&n.DbType, &n.DbVersion, &n.Zone, &n.Region, &n.IDC, &n.Vip, &n.Priority, &n.IsDeleted, &n.Env,
		&n.CreateTime, &n.UpdateTime, &n.Status)
	if err != nil {
		return nil, err
	}
	n.DC = n.IDC
	return &n, nil
}

func (s *mysqlStore) GetStaleNodes(ctx context.Context, role string, olderThan time.Duration) ([]*Node, error) {
	// 读取陈旧节点时使用整型阈值（经过校验，而非用户自由文本）。
	// 仅考虑由 Agent 心跳注册过的节点（status <> 'PENDING'）；
	// 尚未启动的种子行不得被当作死亡节点，否则 Manager 启动会
	// 触发误切换。
	seconds := int(olderThan.Seconds())
	query := fmt.Sprintf("SELECT %s FROM meta_data WHERE update_time < DATE_SUB(NOW(), INTERVAL %d SECOND) AND is_deleted = 0 AND status <> 'PENDING'", nodeSelectCols, seconds)
	args := []interface{}{}
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
		n, err := scanNode(rows)
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

func (s *mysqlStore) GetClusterNodes(ctx context.Context, clusterName string) ([]*Node, error) {
	query := fmt.Sprintf("SELECT %s FROM meta_data WHERE cluster_name = ? AND is_deleted = 0", nodeSelectCols)

	rows, err := s.db.QueryContext(ctx, query, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*Node
	for rows.Next() {
		n, err := scanNode(rows)
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

func (s *mysqlStore) ListClusters(ctx context.Context) ([]*ClusterSummary, error) {
	query := `
		SELECT cluster_name,
		       COUNT(*)                              AS node_count,
		       SUM(role = 'master')                  AS master_count,
		       SUM(role = 'slave')                   AS slave_count,
		       SUM(status = 'ONLINE')                AS online_count,
		       MAX(update_time)                      AS last_heartbeat
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
		var lastHB time.Time
		if err := rows.Scan(&name, &nodeCount, &masterCount, &slaveCount, &onlineCount, &lastHB); err != nil {
			log.Printf("GetClusters scan error: %v", err)
			continue
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

	// 合并集群级属性（优先级/注册状态/端点）。
	metas, mErr := s.ListClusterMetas(ctx)
	if mErr != nil {
		log.Printf("ListClusters: failed to read cluster_meta: %v (defaults used)", mErr)
	}
	mergeClusterMeta(out, metas)
	return out, nil
}

// clusterMetaCols 是读取 cluster_meta 时使用的标准列清单。
const clusterMetaCols = `cluster_name, cluster_priority, registered, endpoint_type, endpoint_value, remark, create_time, update_time`

// scanClusterMeta 映射一行 cluster_meta 记录。
func scanClusterMeta(scanner interface {
	Scan(dest ...interface{}) error
}) (ClusterMeta, error) {
	var m ClusterMeta
	var priority string
	if err := scanner.Scan(&m.ClusterName, &priority, &m.Registered,
		&m.EndpointType, &m.EndpointValue, &m.Remark, &m.CreateTime, &m.UpdateTime); err != nil {
		return ClusterMeta{}, err
	}
	p, err := ParseClusterPriority(priority)
	if err != nil {
		return ClusterMeta{}, err
	}
	m.ClusterPriority = p
	return m, nil
}

func (s *mysqlStore) GetClusterMeta(ctx context.Context, cluster string) (ClusterMeta, error) {
	query := `SELECT ` + clusterMetaCols + ` FROM cluster_meta WHERE cluster_name = ?`
	m, err := scanClusterMeta(s.db.QueryRowContext(ctx, query, cluster))
	if err == sql.ErrNoRows {
		return DefaultClusterMeta(cluster), nil
	}
	return m, err
}

func (s *mysqlStore) ListClusterMetas(ctx context.Context) (map[string]ClusterMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+clusterMetaCols+` FROM cluster_meta`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]ClusterMeta)
	for rows.Next() {
		m, err := scanClusterMeta(rows)
		if err != nil {
			log.Printf("ListClusterMetas scan error: %v", err)
			continue
		}
		out[m.ClusterName] = m
	}
	return out, rows.Err()
}

func (s *mysqlStore) SetClusterPriority(ctx context.Context, cluster string, p ClusterPriority) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cluster_meta
		  (cluster_name, cluster_priority, registered, endpoint_type, endpoint_value, remark, create_time, update_time)
		VALUES (?, ?, 0, 'none', '', '', NOW(), NOW())
		ON DUPLICATE KEY UPDATE cluster_priority = VALUES(cluster_priority), update_time = NOW()`,
		cluster, string(p))
	return err
}

func (s *mysqlStore) RegisterCluster(ctx context.Context, m ClusterMeta) error {
	endpointType, err := NormalizeEndpointType(m.EndpointType)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO cluster_meta
		  (cluster_name, cluster_priority, registered, endpoint_type, endpoint_value, remark, create_time, update_time)
		VALUES (?, ?, 1, ?, ?, ?, NOW(), NOW())
		ON DUPLICATE KEY UPDATE
		  cluster_priority = VALUES(cluster_priority),
		  registered = 1,
		  endpoint_type = VALUES(endpoint_type),
		  endpoint_value = VALUES(endpoint_value),
		  remark = VALUES(remark),
		  update_time = NOW()`,
		m.ClusterName, string(m.ClusterPriority), endpointType, m.EndpointValue, m.Remark)
	return err
}

// ResolveClusterName 为 SQL 后端实现集群归属：
// 自身已知 → 加入上游 → 加入指向我的下游 →
// 分配一个新的编号集群。
func (s *mysqlStore) ResolveClusterName(ctx context.Context, hb Heartbeat) (string, error) {
	rs := hb.replica()

	// 1. 自身已注册 -> 保留其集群
	if name := s.lookupClusterName(ctx, "SELECT cluster_name FROM meta_data WHERE ip = ? AND port = ? AND is_deleted = 0 LIMIT 1", hb.Host, hb.Port); name != "" {
		return name, nil
	}

	// 2. 上游已注册的从节点 -> 归属到上游的集群
	if rs.UpstreamHost != "" {
		if name := s.lookupClusterName(ctx, "SELECT cluster_name FROM meta_data WHERE ip = ? AND port = ? AND is_deleted = 0 LIMIT 1", rs.UpstreamHost, int(rs.UpstreamPort)); name != "" {
			return name, nil
		}
	}

	// 3. 已有下游从节点注册的主节点（从节点先启动）-> 归属到该集群
	if name := s.lookupClusterName(ctx, "SELECT cluster_name FROM meta_data WHERE master_host = ? AND master_port = ? AND is_deleted = 0 LIMIT 1", hb.Host, hb.Port); name != "" {
		return name, nil
	}

	// 4. 全新的复制组 -> 分配下一个 clusterN
	return s.allocateNextClusterName(ctx), nil
}

func (s *mysqlStore) lookupClusterName(ctx context.Context, query string, args ...interface{}) string {
	var name string
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&name); err == nil {
		return name
	}
	return ""
}

func (s *mysqlStore) allocateNextClusterName(ctx context.Context) string {
	rows, err := s.db.QueryContext(ctx, "SELECT cluster_name FROM meta_data WHERE cluster_name REGEXP '^cluster_?[0-9]+$'")
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

func (s *mysqlStore) UpsertHeartbeat(ctx context.Context, hb Heartbeat) error {
	rs := hb.replica()
	// 节点上报的真实 binlog 路径（@@log_bin_basename）随心跳写入 meta_data，
	// 与 binlog_file/binlog_pos 同表收敛。
	binDir, binBase := splitBinlogBasename(rs.Extra["log_bin_basename"])

	query := `UPDATE meta_data SET update_time = NOW(), status = 'ONLINE', role = ?, db_version = ?,
	disk_usage_pct = ?, load_value = ?,
	gtid_executed = ?, binlog_file = ?, binlog_pos = ?,
	binlog_dir = ?, binlog_basename = ?,
	io_thread_running = ?, sql_thread_running = ?,
	semi_sync_enabled = ?, seconds_behind = ?, using_gtid = ?,
	agent_port = ?, master_host = ?, master_port = ?
	WHERE ip = ? AND port = ? AND is_deleted = 0`

	res, err := s.db.ExecContext(ctx, query, hb.Role, hb.Version,
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

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows > 0 {
		return nil
	}

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
	 0, 'PENDING', NOW(), NOW())`
	// 全新记录以 PENDING 插入；下一次心跳（UPDATE
	// 分支，约一个周期间隔之后）将其翻转为 ONLINE。这保持了
	// 原有的一个周期注册窗口。救援策略列（rescue_*）取表默认值，
	// 仅由运维通过 UpsertNodeBinlogRescue 改写。

	_, err = s.db.ExecContext(ctx, insertQuery,
		hb.Cluster, hb.Role, hb.Host, hb.Port, rs.AgentPort, rs.UpstreamHost, rs.UpstreamPort, hb.DBType, hb.Version,
		hb.DiskUsage, hb.Load, rs.gtidExecuted(), rs.binlogFile(), rs.binlogPos(),
		binDir, binBase,
		rs.ReceiverRunning, rs.ApplierRunning, rs.SyncReplication, rs.LagSeconds, rs.usingGTID())
	return err
}

func (s *mysqlStore) InsertSwitchRecord(ctx context.Context, rec *SwitchRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO switch_history
		   (cluster_name, operation, recover_mode, old_master, new_master, analysis_code, result)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		rec.ClusterName, rec.Operation, rec.RecoverMode,
		rec.OldMaster, rec.NewMaster, rec.AnalysisCode, rec.Result)
	return err
}

func (s *mysqlStore) GetLatestSwitch(ctx context.Context, clusterName string) (*SwitchRecord, error) {
	query := `SELECT ` + switchRecordCols + ` FROM switch_history
		WHERE cluster_name = ? AND result = 'SUCCESS' AND (
			operation IN ('failover', 'switchover')
			OR (operation = 'recover' AND recover_mode IN ('topology', 'rollback'))
		)
		ORDER BY id DESC LIMIT 1`

	row := s.db.QueryRowContext(ctx, query, clusterName)

	var rec SwitchRecord
	if err := row.Scan(
		&rec.ID, &rec.ClusterName, &rec.Operation, &rec.RecoverMode,
		&rec.OldMaster, &rec.NewMaster, &rec.AnalysisCode, &rec.Result, &rec.CreateTime); err != nil {
		return nil, err
	}
	return &rec, nil
}

// InsertTopologySnapshot 保存切换前的边集合 JSON。
func (s *mysqlStore) InsertTopologySnapshot(ctx context.Context, cluster, edgesJSON string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO topology_snapshot (cluster_name, edges_json) VALUES (?, ?)`,
		cluster, edgesJSON)
	return err
}

// GetLatestTopologySnapshot 返回最近一次保存的边集合 JSON。
func (s *mysqlStore) GetLatestTopologySnapshot(ctx context.Context, cluster string) (string, error) {
	var edgesJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT edges_json FROM topology_snapshot WHERE cluster_name = ? ORDER BY id DESC LIMIT 1`,
		cluster).Scan(&edgesJSON)
	if err != nil {
		return "", err
	}
	return edgesJSON, nil
}

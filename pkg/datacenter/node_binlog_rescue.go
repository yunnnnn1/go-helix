package datacenter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// splitBinlogBasename 把心跳上报的完整 @@log_bin_basename（路径前缀）
// 拆成目录与基础名。空值得到两个空串，表示本次心跳未提供可用路径。
func splitBinlogBasename(full string) (dir, base string) {
	if full == "" {
		return "", ""
	}
	return filepath.Dir(full), filepath.Base(full)
}

// nodeRescueCols 是从节点表 meta_data 读取救援信息时使用的列清单（不含
// update_time，后者按后端类型单独扫描）：前两项是节点上报的物理路径，
// 其余是运维配置的救援策略。
const nodeRescueCols = `rescue_enabled, binlog_dir, binlog_basename,
	rescue_access_roots, rescue_max_bytes`

// decodeAccessRoots 把存储中的 JSON 数组文本解析为目录列表。
// NULL/空值或非法 JSON 一律得到空切片（语义为“读 jail 输入为空”）。
func decodeAccessRoots(raw string) []string {
	roots := []string{}
	if strings.TrimSpace(raw) == "" {
		return roots
	}
	var arr []string
	if json.Unmarshal([]byte(raw), &arr) == nil {
		return arr
	}
	return roots
}

// encodeAccessRoots 把目录列表序列化为 JSON 数组文本（nil -> []）。
func encodeAccessRoots(roots []string) string {
	if roots == nil {
		roots = []string{}
	}
	b, err := json.Marshal(roots)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// MySQL 后端
// ---------------------------------------------------------------------------

func (s *mysqlStore) GetNodeBinlogRescue(ctx context.Context, host string, port int) (NodeBinlogRescue, error) {
	var (
		out     NodeBinlogRescue
		enabled int
		roots   sql.NullString
		ut      time.Time
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT `+nodeRescueCols+`, update_time
		 FROM meta_data WHERE ip = ? AND port = ? AND is_deleted = 0`,
		host, port).Scan(&enabled, &out.BinlogDir, &out.BinlogBaseName,
		&roots, &out.MaxBytes, &ut)
	if err == sql.ErrNoRows {
		return DefaultNodeBinlogRescue(host, port), nil
	}
	if err != nil {
		return DefaultNodeBinlogRescue(host, port), err
	}
	out.Host, out.Port, out.Enabled = host, port, enabled != 0
	out.AccessRoots = decodeAccessRoots(roots.String)
	out.UpdateTime = ut
	return out, nil
}

func (s *mysqlStore) UpsertNodeBinlogRescue(ctx context.Context, n NodeBinlogRescue) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE meta_data
		SET rescue_enabled = ?, rescue_access_roots = ?, rescue_max_bytes = ?, update_time = NOW()
		WHERE ip = ? AND port = ? AND is_deleted = 0`,
		n.Enabled, encodeAccessRoots(n.AccessRoots), n.MaxBytes, n.Host, n.Port)
	if err != nil {
		return err
	}
	return requireNodeRow(res, n.Host, n.Port)
}

// ---------------------------------------------------------------------------
// SQLite 后端
// ---------------------------------------------------------------------------

func (s *sqliteStore) GetNodeBinlogRescue(ctx context.Context, host string, port int) (NodeBinlogRescue, error) {
	var (
		out     NodeBinlogRescue
		enabled int
		roots   sql.NullString
		utStr   string
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT `+nodeRescueCols+`, update_time
		 FROM meta_data WHERE ip = ? AND port = ? AND is_deleted = 0`,
		host, port).Scan(&enabled, &out.BinlogDir, &out.BinlogBaseName,
		&roots, &out.MaxBytes, &utStr)
	if err == sql.ErrNoRows {
		return DefaultNodeBinlogRescue(host, port), nil
	}
	if err != nil {
		return DefaultNodeBinlogRescue(host, port), err
	}
	out.Host, out.Port, out.Enabled = host, port, enabled != 0
	out.AccessRoots = decodeAccessRoots(roots.String)
	if utStr != "" {
		out.UpdateTime, _ = parseTS(utStr)
	}
	return out, nil
}

func (s *sqliteStore) UpsertNodeBinlogRescue(ctx context.Context, n NodeBinlogRescue) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE meta_data
		SET rescue_enabled = ?, rescue_access_roots = ?, rescue_max_bytes = ?, update_time = ?
		WHERE ip = ? AND port = ? AND is_deleted = 0`,
		n.Enabled, encodeAccessRoots(n.AccessRoots), n.MaxBytes, formatTS(time.Now()), n.Host, n.Port)
	if err != nil {
		return err
	}
	return requireNodeRow(res, n.Host, n.Port)
}

// ---------------------------------------------------------------------------
// 内存后端
// ---------------------------------------------------------------------------

func (s *memoryStore) GetNodeBinlogRescue(_ context.Context, host string, port int) (NodeBinlogRescue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if n, ok := s.nodes[nodeKey(host, port)]; ok && !n.IsDeleted {
		roots := n.RescueAccessRoots
		if roots == nil {
			roots = []string{}
		}
		return NodeBinlogRescue{
			Host: host, Port: port, Enabled: n.RescueEnabled,
			BinlogDir: n.BinlogDir, BinlogBaseName: n.BinlogBaseName,
			AccessRoots: roots, MaxBytes: n.RescueMaxBytes,
			UpdateTime: n.UpdateTime,
		}, nil
	}
	return DefaultNodeBinlogRescue(host, port), nil
}

func (s *memoryStore) UpsertNodeBinlogRescue(_ context.Context, n NodeBinlogRescue) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[nodeKey(n.Host, n.Port)]
	if !ok || node.IsDeleted {
		return fmt.Errorf("节点 %s:%d 尚未上报心跳，无法配置救援策略", n.Host, n.Port)
	}
	roots := n.AccessRoots
	if roots == nil {
		roots = []string{}
	}
	node.RescueEnabled = n.Enabled
	node.RescueAccessRoots = roots
	node.RescueMaxBytes = n.MaxBytes
	node.UpdateTime = time.Now()
	return nil
}

// requireNodeRow 检查 UPDATE 是否命中节点行；未命中（节点尚未上报）时
// 返回明确错误，避免“配置成功但实际无效”的假阳性。
func requireNodeRow(res sql.Result, host string, port int) error {
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("节点 %s:%d 尚未上报心跳，无法配置救援策略", host, port)
	}
	return nil
}

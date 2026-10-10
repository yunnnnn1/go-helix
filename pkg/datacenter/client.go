// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package datacenter

import (
	"context"
	"database/sql"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/db"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
)

// Client 是 MetaStore 之上的薄门面。它为所有调用方保持历史公开 API
// （7 个方法）稳定，而各元数据后端在 MetaStore 之后实现
// 具体的持久化细节。
type Client struct {
	store MetaStore
}

// NewClient 根据给定的元数据配置构建 Client。当未提供配置
// 或无法连接数据库时，降级为使用内存存储（行为不变）。
func NewClient(cfg *config.DatabaseConfig) *Client {
	if cfg == nil {
		log.Println("[DataCenter] No config provided. Using in-memory simulation.")
		return &Client{store: newMemoryStore()}
	}

	// 解析所配置元数据存储的方言；DSN 的构造
	// 与数据库类型相关，应由方言完成，而非此处。
	dbType := cfg.Type
	if dbType == "" {
		dbType = "mysql"
	}
	dialect, err := db.GetDialect(dbType)
	if err != nil {
		log.Printf("[DataCenter] %v. Using in-memory simulation.", err)
		return &Client{store: newMemoryStore()}
	}

	if dbType == "sqlite" {
		return newSQLiteClient(cfg, dialect)
	}
	return newMySQLClient(cfg, dialect, dbType)
}

// newSQLiteClient 打开内嵌的 SQLite 元数据文件并确保 schema 就绪。
func newSQLiteClient(cfg *config.DatabaseConfig, dialect db.Dialect) *Client {
	dsn := dialect.BuildDSN(db.ConnSpec{Path: cfg.Path, DBName: cfg.DBName})

	log.Printf("[DataCenter] Opening sqlite metadata file: %s", cfg.Path)

	conn, err := sql.Open(dialect.SQLDriverName(), dsn)
	if err != nil {
		log.Printf("[DataCenter] Failed to open sqlite connection: %v. Using in-memory simulation.", err)
		return &Client{store: newMemoryStore()}
	}

	// WAL 与 busy_timeout（通过 DSN 设置）将共享访问串行化；像
	// TCP 后端一样限制连接池规模。
	maxOpen := 10
	if cfg.MaxOpenConns > 0 {
		maxOpen = cfg.MaxOpenConns
	}
	conn.SetMaxOpenConns(maxOpen)
	maxIdle := 5
	if cfg.MaxIdleConns > 0 {
		maxIdle = cfg.MaxIdleConns
	}
	conn.SetMaxIdleConns(maxIdle)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		log.Printf("[DataCenter] Failed to open sqlite file: %v. Using in-memory simulation.", err)
		conn.Close()
		return &Client{store: newMemoryStore()}
	}

	store := &sqliteStore{db: conn}
	if err := store.EnsureSchema(ctx); err != nil {
		log.Printf("[DataCenter] %v. Using in-memory simulation.", err)
		conn.Close()
		return &Client{store: newMemoryStore()}
	}

	log.Printf("[DataCenter] sqlite metadata ready at %s.", cfg.Path)
	return &Client{store: store}
}

// newMySQLClient 打开基于 TCP 的 MySQL 元数据数据库。
func newMySQLClient(cfg *config.DatabaseConfig, dialect db.Dialect, dbType string) *Client {
	dsn := dialect.BuildDSN(db.ConnSpec{
		Host:     cfg.Host,
		Port:     int32(cfg.Port),
		User:     cfg.User,
		Password: cfg.Password,
		DBName:   cfg.DBName,
		Timeout:  cfg.Timeout,
	})

	log.Printf("[DataCenter] Connecting to %s at %s:%d/%s", dbType, cfg.Host, cfg.Port, cfg.DBName)

	conn, err := sql.Open(dialect.SQLDriverName(), dsn)
	if err != nil {
		log.Printf("[DataCenter] Failed to open %s connection: %v. Using in-memory simulation.", dbType, err)
		return &Client{store: newMemoryStore()}
	}

	maxOpen := 10
	if cfg.MaxOpenConns > 0 {
		maxOpen = cfg.MaxOpenConns
	}
	conn.SetMaxOpenConns(maxOpen)

	maxIdle := 5
	if cfg.MaxIdleConns > 0 {
		maxIdle = cfg.MaxIdleConns
	}
	conn.SetMaxIdleConns(maxIdle)

	maxLifetime := time.Minute * 5
	if cfg.ConnMaxLifetime != "" {
		if d, err := time.ParseDuration(cfg.ConnMaxLifetime); err == nil {
			maxLifetime = d
		}
	}
	conn.SetConnMaxLifetime(maxLifetime)

	if err := conn.Ping(); err != nil {
		log.Printf("[DataCenter] Failed to ping %s: %v. Using in-memory simulation.", dbType, err)
		conn.Close()
		return &Client{store: newMemoryStore()}
	}

	log.Printf("[DataCenter] Connected to %s MetaData Store at %s:%d.", dbType, cfg.Host, cfg.Port)
	return &Client{store: &mysqlStore{db: conn}}
}

// GetStaleMasterNodes 返回心跳早于阈值的主节点。
func (c *Client) GetStaleMasterNodes(threshold time.Duration) ([]*Node, error) {
	return c.store.GetStaleNodes(context.Background(), "master", threshold)
}

// GetStaleSlaveNodes 返回心跳早于阈值的从节点。
func (c *Client) GetStaleSlaveNodes(threshold time.Duration) ([]*Node, error) {
	return c.store.GetStaleNodes(context.Background(), "slave", threshold)
}

// GetFailedNodes 返回 60s 内未更新的节点。
func (c *Client) GetFailedNodes() ([]*Node, error) {
	return c.store.GetStaleNodes(context.Background(), "master", 60*time.Second)
}

// GetClusterNodes 返回指定集群的全部存活节点。
func (c *Client) GetClusterNodes(clusterName string) ([]*Node, error) {
	return c.store.GetClusterNodes(context.Background(), clusterName)
}

// GetClusters 返回所有受管集群的聚合概览。
func (c *Client) GetClusters() ([]*ClusterSummary, error) {
	return c.store.ListClusters(context.Background())
}

// RegisterOrUpdateHeartbeat 更新或插入一条节点心跳。当 clusterName
// 为空时，由存储依据复制拓扑解析集群归属。
func (c *Client) RegisterOrUpdateHeartbeat(ip string, port int, dbType, clusterName, role, version string, diskPct float64, load float64, replicaStatus *ReplicaStatus) error {
	hb := Heartbeat{
		Host: ip, Port: port, DBType: dbType, Cluster: clusterName, Role: role,
		Version: version, DiskUsage: diskPct, Load: load, Replica: replicaStatus,
	}
	ctx := context.Background()

	if hb.Cluster == "" {
		name, err := c.store.ResolveClusterName(ctx, hb)
		if err != nil {
			return err
		}
		hb.Cluster = name
	}
	return c.store.UpsertHeartbeat(ctx, hb)
}

// InsertSwitchHistory 持久化一条切换/恢复记录。
func (c *Client) InsertSwitchHistory(rec *SwitchRecord) error {
	return c.store.InsertSwitchRecord(context.Background(), rec)
}

// GetLatestSwitch 返回最近一次成功且改变拓扑的切换。
func (c *Client) GetLatestSwitch(clusterName string) (*SwitchRecord, error) {
	return c.store.GetLatestSwitch(context.Background(), clusterName)
}

// SaveTopologySnapshot 持久化切换前的复制边集合 JSON。
func (c *Client) SaveTopologySnapshot(clusterName string, edgesJSON string) error {
	return c.store.InsertTopologySnapshot(context.Background(), clusterName, edgesJSON)
}

// GetTopologySnapshot 返回最近一次保存的复制边集合 JSON。
func (c *Client) GetTopologySnapshot(clusterName string) (string, error) {
	return c.store.GetLatestTopologySnapshot(context.Background(), clusterName)
}

// GetClusterMeta 返回集群级属性（无记录时为默认值）。
func (c *Client) GetClusterMeta(cluster string) (ClusterMeta, error) {
	return c.store.GetClusterMeta(context.Background(), cluster)
}

// ListClusterMetas 返回已注册集群的属性。
func (c *Client) ListClusterMetas() (map[string]ClusterMeta, error) {
	return c.store.ListClusterMetas(context.Background())
}

// SetClusterPriority 更新或写入集群优先级。
func (c *Client) SetClusterPriority(cluster string, p ClusterPriority) error {
	return c.store.SetClusterPriority(context.Background(), cluster, p)
}

// RegisterCluster 更新或写入一条已注册的 cluster_meta 记录。
func (c *Client) RegisterCluster(m ClusterMeta) error {
	return c.store.RegisterCluster(context.Background(), m)
}

// GetNodeBinlogRescue 返回单个节点（host:port）的 binlog 救援配置
// （无记录时返回默认值 enabled=false）。
func (c *Client) GetNodeBinlogRescue(host string, port int) (NodeBinlogRescue, error) {
	return c.store.GetNodeBinlogRescue(context.Background(), host, port)
}

// UpsertNodeBinlogRescue 更新或写入节点级 binlog 救援配置。
func (c *Client) UpsertNodeBinlogRescue(n NodeBinlogRescue) error {
	return c.store.UpsertNodeBinlogRescue(context.Background(), n)
}

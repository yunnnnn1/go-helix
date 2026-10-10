-- Copyright (c) 2026 Go-Helix Authors
--
-- This file is part of go-helix.
-- Licensed under the MIT License. See the LICENSE file in the project root
-- for the full license text.

-- =============================================================================
-- go-helix DataCenter 元数据表 meta_data
-- 用途：存储所有被管数据库节点的身份、角色、复制拓扑边与运行时状态。
-- =============================================================================

CREATE TABLE IF NOT EXISTS `meta_data` (
  `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `cluster_name`       VARCHAR(128)    NOT NULL DEFAULT ''     COMMENT '所属集群（由复制拓扑推导，cluster1/cluster2…）',
  `role`               VARCHAR(16)     NOT NULL DEFAULT ''     COMMENT '角色：master/slave',
  `ip`                 VARCHAR(64)     NOT NULL DEFAULT ''     COMMENT '数据库监听地址（兼容 IPv6）',
  `port`               INT             NOT NULL DEFAULT 0      COMMENT '数据库端口',
  `agent_port`         INT             NOT NULL DEFAULT 0      COMMENT 'Agent gRPC 端口',
  `master_host`        VARCHAR(64)     NOT NULL DEFAULT ''     COMMENT '复制上游地址（Slave 有值，Master/单机为空）',
  `master_port`        INT             NOT NULL DEFAULT 0      COMMENT '复制上游端口',
  `dbtype`             VARCHAR(32)     NOT NULL DEFAULT 'mysql' COMMENT '数据库类型',
  `db_version`         VARCHAR(32)     NOT NULL DEFAULT ''     COMMENT '数据库版本',
  `zone`               VARCHAR(64)     NOT NULL DEFAULT ''     COMMENT '可用区',
  `region`             VARCHAR(64)     NOT NULL DEFAULT ''     COMMENT '区域',
  `idc`                VARCHAR(64)     NOT NULL DEFAULT ''     COMMENT '机房',
  `vip`                VARCHAR(64)     NOT NULL DEFAULT ''     COMMENT '虚拟 IP',
  `priority`           INT             NOT NULL DEFAULT 0      COMMENT '选主优先级（越大越优）',
  `is_deleted`         TINYINT         NOT NULL DEFAULT 0      COMMENT '是否删除：0否 1是',
  `env`                VARCHAR(32)     NOT NULL DEFAULT ''     COMMENT '环境：prod/staging 等',
  `status`             VARCHAR(16)     NOT NULL DEFAULT 'PENDING' COMMENT '在线状态：ONLINE/OFFLINE/FAILED/PENDING',
  `disk_usage_pct`     DOUBLE          NOT NULL DEFAULT 0      COMMENT '磁盘使用率（%）',
  `load_value`         DOUBLE          NOT NULL DEFAULT 0      COMMENT '系统负载',
  `gtid_executed`      MEDIUMTEXT      NULL                    COMMENT '已执行 GTID 集合',
  `binlog_file`        VARCHAR(128)    NOT NULL DEFAULT ''     COMMENT 'Binlog/Relay log 文件名（候选起始位点）',
  `binlog_pos`         BIGINT          NOT NULL DEFAULT 0      COMMENT 'Binlog 位点（候选起始位点）',
  `binlog_dir`         VARCHAR(255)    NOT NULL DEFAULT ''     COMMENT '本地 binlog 目录（心跳上报 @@log_bin_basename 解析）',
  `binlog_basename`    VARCHAR(128)    NOT NULL DEFAULT ''     COMMENT '本地 binlog 基础名（心跳上报 @@log_bin_basename 解析）',
  `rescue_enabled`     TINYINT(1)      NOT NULL DEFAULT 0      COMMENT 'MHA 风格 binlog 尾部救援开关（特权路径，默认关闭）',
  `rescue_access_roots` JSON           NULL                    COMMENT '允许读 binlog 的目录白名单（JSON 数组，NULL/[]=不做读 jail）',
  `rescue_max_bytes`   BIGINT          NOT NULL DEFAULT 0      COMMENT '单次尾部救援载荷上限，0 由程序默认 1 GiB',
  `io_thread_running`  TINYINT(1)      NOT NULL DEFAULT 0      COMMENT '复制 IO 线程是否运行',
  `sql_thread_running` TINYINT(1)      NOT NULL DEFAULT 0      COMMENT '复制 SQL 线程是否运行',
  `semi_sync_enabled`  TINYINT(1)      NOT NULL DEFAULT 0      COMMENT '是否半同步 Master',
  `seconds_behind`     BIGINT          NOT NULL DEFAULT 0      COMMENT '复制延迟秒数',
  `using_gtid`         TINYINT(1)      NOT NULL DEFAULT 0      COMMENT '是否使用 GTID',
  `create_time`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '心跳更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_ip_port` (`ip`, `port`),
  KEY `idx_cluster` (`cluster_name`),
  KEY `idx_role` (`role`),
  KEY `idx_update_time` (`update_time`),
  KEY `idx_master` (`master_host`, `master_port`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='go-helix 节点元数据与复制拓扑表';

-- =============================================================================
-- go-helix 切换历史表 switch_history
-- 用途：结构化记录每次主从切换/恢复前后的主库身份，为「回滚最近一次切换」
--       提供可靠依据（不依赖解析日志文本）。
-- =============================================================================

CREATE TABLE IF NOT EXISTS `switch_history` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `cluster_name`  VARCHAR(128)    NOT NULL DEFAULT ''     COMMENT '所属集群',
  `operation`     VARCHAR(16)     NOT NULL DEFAULT ''     COMMENT '操作类型：failover/switchover/recover',
  `recover_mode`  VARCHAR(16)     NOT NULL DEFAULT ''     COMMENT 'recover 子类型：replica/topology/rollback（仅 operation=recover）',
  `old_master`    VARCHAR(128)    NOT NULL DEFAULT ''     COMMENT '切换前主库 host:port',
  `new_master`    VARCHAR(128)    NOT NULL DEFAULT ''     COMMENT '切换后主库 host:port',
  `analysis_code` VARCHAR(48)     NOT NULL DEFAULT ''     COMMENT '故障分析码（如有）',
  `result`        VARCHAR(16)     NOT NULL DEFAULT ''     COMMENT '结果：SUCCESS/FAILED',
  `create_time`   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
  PRIMARY KEY (`id`),
  KEY `idx_cluster_time` (`cluster_name`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='go-helix 主从切换/恢复历史表';

-- =============================================================================
-- go-helix 拓扑快照表 topology_snapshot
-- 用途：在拓扑变更切换前持久化完整复制边集合（JSON），供回滚时精确还原
--       每个节点的上游拓扑形状，而非简单平铺到新主。
-- =============================================================================

CREATE TABLE IF NOT EXISTS `topology_snapshot` (
  `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `cluster_name` VARCHAR(128)    NOT NULL DEFAULT ''     COMMENT '所属集群',
  `edges_json`   MEDIUMTEXT      NULL                    COMMENT '切换前完整复制边 JSON（upstream/child）',
  `create_time`  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '快照时间',
  PRIMARY KEY (`id`),
  KEY `idx_cluster_time` (`cluster_name`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='go-helix 切换前拓扑快照表';

-- =============================================================================
-- go-helix 集群属性表 cluster_meta
-- 用途：集群级（非节点级）单值属性：集群优先级 P0-P3、准入注册标记、业务接入
--       端点（VIP/域名）。集群由节点心跳聚合出现，未注册时以默认值 P3 处理。
-- =============================================================================

CREATE TABLE IF NOT EXISTS `cluster_meta` (
  `cluster_name`     VARCHAR(128) NOT NULL COMMENT '集群名（与 meta_data.cluster_name 一致）',
  `cluster_priority` VARCHAR(4)   NOT NULL DEFAULT 'P3' COMMENT '集群优先级：P0/P1/P2/P3',
  `registered`       TINYINT(1)   NOT NULL DEFAULT 0 COMMENT '是否经准入注册（P0 Manager 集群要求为1）',
  `endpoint_type`    VARCHAR(8)   NOT NULL DEFAULT 'none' COMMENT '业务接入端点类型：none/vip/domain',
  `endpoint_value`   VARCHAR(128) NOT NULL DEFAULT '' COMMENT '业务接入 VIP 或域名',
  `remark`           VARCHAR(255) NOT NULL DEFAULT '' COMMENT '备注',
  `create_time`      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `update_time`      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`cluster_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='go-helix 集群级属性（优先级/准入/端点）';


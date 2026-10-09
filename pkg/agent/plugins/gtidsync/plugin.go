package gtidsync

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	gtidsql "github.com/yunnnnn1/go-helix/pkg/agent/plugins/gtidsync/sqlbuilder"
	log "github.com/yunnnnn1/go-helix/pkg/logging"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// GTIDSyncPlugin 是无状态的：它不持有任何实例专属的连接。
// 每次请求都使用 params 中携带的目标地址和凭证，从共享 ConnPool 拨号建立
// 所需的连接。
type GTIDSyncPlugin struct{}

const (
	// pluginDBType：gtid 同步按定义就是 MySQL 专属的。
	pluginDBType = "mysql"
	// actionCatchupFrom 是唯一支持的动作。
	actionCatchupFrom = "CatchupFrom"
	// syncTimeout 限定单次追平流的时长，使其永远不会挂死。
	syncTimeout = 30 * time.Second
)

func init() {
	plugins.Register(&GTIDSyncPlugin{})
}

func (p *GTIDSyncPlugin) Name() string {
	return "gtid_sync"
}

// Init 为满足接口兼容性而保留，但不再打开或存储连接；
// 连接按每次请求从共享连接池获取。
func (p *GTIDSyncPlugin) Init(ctx context.Context, cfg map[string]string) error {
	log.Printf("[Plugin:GTIDSync] ready (stateless, dynamic connections)")
	return nil
}

func (p *GTIDSyncPlugin) Execute(ctx context.Context, action string, params map[string]string) (map[string]string, error) {
	switch action {
	case actionCatchupFrom:
		// 从中拉取缺失事务的远端数据源（目标地址由 Manager 给出）。
		targetAddr := net.JoinHostPort(params["host"], params["port"])
		// 接收重放事务的本地实例（由 Server 注入）。
		localAddr := net.JoinHostPort(params["db_host"], params["db_port"])
		// 主从切换/GTID 操作优先使用专用的 manager 账号；
		// 未注入时回退使用探测账号。
		user, pass := params["manager_user"], params["manager_pass"]
		if user == "" {
			user, pass = params["db_user"], params["db_pass"]
		}

		log.Printf("[Plugin:GTIDSync] CatchupFrom %s -> local %s", targetAddr, localAddr)
		if err := p.syncMissingData(ctx, localAddr, targetAddr, user, pass); err != nil {
			log.Printf("[Plugin:GTIDSync] CatchupFrom failed: %v", err)
			return nil, err
		}

		log.Printf("[Plugin:GTIDSync] CatchupFrom success!")
		return map[string]string{"message": "Catchup completed"}, nil

	case actionReplayRescue:
		// 对 Agent 已保存到磁盘的片段进行离线重放。
		return p.replayRescue(ctx, params)

	default:
		return nil, fmt.Errorf("gtid_sync: unknown action: %s", action)
	}
}

func (p *GTIDSyncPlugin) syncMissingData(ctx context.Context, localAddr, targetAddr, user, pass string) error {
	pool := plugins.DefaultPool()

	localDB, err := pool.Get(ctx, pluginDBType, localAddr, user, pass)
	if err != nil {
		return fmt.Errorf("connect local %s: %w", localAddr, err)
	}
	targetDB, err := pool.Get(ctx, pluginDBType, targetAddr, user, pass)
	if err != nil {
		return fmt.Errorf("connect target %s: %w", targetAddr, err)
	}

	// 读取两端的复制位点。
	var localGtidStr, targetGtidStr string
	if err := localDB.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&localGtidStr); err != nil {
		return fmt.Errorf("get local GTID: %w", err)
	}
	targetDB.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&targetGtidStr)

	localGtid, err := mysql.ParseGTIDSet("mysql", localGtidStr)
	if err != nil {
		return fmt.Errorf("parse local GTID: %w", err)
	}

	// 解析同步终点。目标位点为空意味着没有明确的追平目标点：
	// 盲目重放并不安全，只会一直运行到硬性超时，因此跳过该流。
	var targetGtid mysql.GTIDSet
	if targetGtidStr == "" {
		log.Printf("[Plugin:GTIDSync] target %s reports empty gtid_executed, nothing to catch up to; skipping sync", targetAddr)
		return nil
	}
	g, err := mysql.ParseGTIDSet("mysql", targetGtidStr)
	if err != nil {
		return fmt.Errorf("parse target GTID: %w", err)
	}
	targetGtid = g
	log.Printf("[Plugin:GTIDSync] Local GTID: %s, Target GTID: %s", localGtidStr, targetGtidStr)
	if localGtid.Contain(targetGtid) {
		log.Printf("[Plugin:GTIDSync] Local already contains target GTID. No sync needed.")
		return nil
	}

	// Canal 是一次性的二进制日志流：每次调用创建、每次调用关闭。
	cfg := canal.NewDefaultConfig()
	cfg.Addr = targetAddr
	cfg.User = user
	cfg.Password = pass
	cfg.Dump.ExecutionPath = "" // 只处理数据变更，跳过初始 dump

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("create canal: %w", err)
	}
	defer c.Close()

	handler := &syncHandler{
		db:         localDB,
		targetGtid: targetGtid,
		gapTotal:   countGapTransactions(targetGtid, localGtid),
	}
	// stop 封装 canal.Close；它是一个字段，便于单元测试在没有运行中的
	// canal/服务器的情况下断言停止决策。
	handler.stop = func() { c.Close() }
	c.SetEventHandler(handler)

	// 守护：ctx 取消或触发硬性超平时取消该流，同步正常结束后立即退出
	//（不残留 goroutine）。
	done := make(chan struct{})
	defer close(done)
	go guardCanal(ctx, done, c)

	if err := c.StartFromGTID(localGtid); err != nil && !strings.Contains(err.Error(), "closed") {
		return fmt.Errorf("canal sync error: %w", err)
	}
	return nil
}

// guardCanal 在 ctx 取消或 syncTimeout 之后关闭 canal；done 触发
// （正常完成）时不做任何动作直接返回。
func guardCanal(ctx context.Context, done <-chan struct{}, c *canal.Canal) {
	timer := time.NewTimer(syncTimeout)
	defer timer.Stop()

	select {
	case <-done:
		return
	case <-ctx.Done():
		log.Printf("[Plugin:GTIDSync] context cancelled (%v), closing canal", ctx.Err())
	case <-timer.C:
		log.Printf("[Plugin:GTIDSync] hard timeout %s reached, closing canal", syncTimeout)
	}
	c.Close()
}

type syncHandler struct {
	canal.DummyEventHandler
	db         *sql.DB // 接收重放行的本地实例
	targetGtid mysql.GTIDSet

	// gapTotal 是本地集合缺失的目标事务数量；applied 统计其中已被完整
	// 重放的数量（行事件之后的 OnXID）。一旦 applied 达到 gapTotal，
	// 该流即完成使命并立即停止，而不必等满硬性超时。
	gapTotal int
	applied  int

	// txnFailed 标记当前正在重放的事务至少有一个行事件失败；此类事务
	// 不计入已应用数量。
	txnFailed bool

	// stop 关闭二进制日志流（生产环境为 canal.Close）。它是一个字段，
	// 便于单元测试在没有 canal 的情况下观测停止决策。
	stop func()
}

func (h *syncHandler) OnRow(e *canal.RowsEvent) error {
	var firstErr error
	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			s, a, buildErr := gtidsql.BuildInsertSql(e.Table, row)
			if buildErr != nil {
				log.Printf("[Plugin:GTIDSync] Build insert SQL failed: %v", buildErr)
				continue
			}
			if err := h.execute(s, a); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	case canal.UpdateAction:
		// 行以变更前/变更后成对到达。
		for i := 0; i+1 < len(e.Rows); i += 2 {
			s, a, buildErr := gtidsql.BuildUpdateSql(e.Table, e.Rows[i], e.Rows[i+1])
			if buildErr != nil {
				log.Printf("[Plugin:GTIDSync] Build update SQL failed: %v", buildErr)
				continue
			}
			if err := h.executeRowChange(s, a); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			s, a, buildErr := gtidsql.BuildDeleteSql(e.Table, row)
			if buildErr != nil {
				log.Printf("[Plugin:GTIDSync] Build delete SQL failed: %v", buildErr)
				continue
			}
			if err := h.executeRowChange(s, a); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	// 行失败意味着该事务不能计入已应用数量；完成跟踪绝不能在数据缺失的
	// 情况下宣布缺口已补齐。
	if firstErr != nil {
		h.txnFailed = true
	}
	return nil
}

func (h *syncHandler) execute(sqlStr string, args []interface{}) error {
	if sqlStr == "" {
		return nil
	}
	if _, err := h.db.Exec(sqlStr, args...); err != nil {
		log.Printf("[Plugin:GTIDSync] Failed to execute catchup SQL: %s, err: %v", sqlStr, err)
		return err
	}
	log.Printf("[Plugin:GTIDSync] Executed catchup SQL successfully")
	return nil
}

// executeRowChange 应用一条重放的 UPDATE/DELETE，并验证其最多命中一行。
// 生成的 SQL 带 LIMIT 1；影响超过一行意味着存在无唯一键的重复行，
// 此时无法安全地映射变更前/变更后镜像，该事务必须失败。
func (h *syncHandler) executeRowChange(sqlStr string, args []interface{}) error {
	res, err := h.db.Exec(sqlStr, args...)
	if err != nil {
		log.Printf("[Plugin:GTIDSync] Failed to execute catchup SQL: %s, err: %v", sqlStr, err)
		return err
	}
	if n, _ := res.RowsAffected(); n > 1 {
		err := fmt.Errorf("row change affected %d rows (expected exactly 1): duplicate rows without a unique key, SQL: %s", n, sqlStr)
		log.Printf("[Plugin:GTIDSync] %v", err)
		return err
	}
	return nil
}

// OnDDL 重放缺口中的一条变更 schema 语句。DDL 自动提交且不产生 XID
// 事件，因此事务在此直接计数。
func (h *syncHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, q *replication.QueryEvent) error {
	query := strings.TrimSpace(string(q.Query))
	if query == "" {
		return nil
	}
	log.Printf("[Plugin:GTIDSync] replaying DDL: %s", query)
	if _, err := h.db.Exec(query); err != nil {
		log.Printf("[Plugin:GTIDSync] replay DDL failed: %v", err)
		h.txnFailed = true
		return nil
	}
	h.applied++
	log.Printf("[Plugin:GTIDSync] replayed DDL transaction %d/%d", h.applied, h.gapTotal)
	if h.gapTotal > 0 && h.applied >= h.gapTotal {
		log.Printf("[Plugin:GTIDSync] target reached after DDL, stopping sync")
		h.stop()
	}
	return nil
}

func (h *syncHandler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	// 该事件暴露其 GTID_NEXT：正是当前事务的 GTID。
	current, err := gtidEvent.GTIDNext()
	if err != nil {
		log.Printf("[Plugin:GTIDSync] Resolve GTID event failed: %v", err)
		return nil
	}

	// 包含在所捕获目标快照内的事务仍然必须重放——让它的行事件正常通过。
	// 在事务边界处重置按事务记录的失败标记。
	if h.targetGtid == nil || h.targetGtid.Contain(current) {
		h.txnFailed = false
		return nil
	}

	// 第一个不在目标快照内的事务比我们需要追平的位点更新。OnGTID 在该事务
	// 的行事件之前触发，因此在此切断流既不会跳过所需数据，也不会应用任何
	// 超出目标的内容。
	log.Printf("[Plugin:GTIDSync] Passed target boundary at %v, stopping sync", current.String())
	h.stop()
	return nil
}

// OnXID 在一条被重放事务的行事件已应用并提交后触发。只统计完全成功的
// 事务；一旦每个缺失的目标事务都已应用，立即停止，而不必等待硬性超时。
func (h *syncHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	if h.txnFailed {
		log.Printf("[Plugin:GTIDSync] transaction at %v had failed row replay(s); not counted as applied (applied=%d/%d)",
			pos, h.applied, h.gapTotal)
		return nil
	}
	h.applied++
	log.Printf("[Plugin:GTIDSync] replayed %d/%d target transaction(s)", h.applied, h.gapTotal)
	if h.gapTotal > 0 && h.applied >= h.gapTotal {
		log.Printf("[Plugin:GTIDSync] All %d missing target transaction(s) replayed, stopping sync", h.gapTotal)
		h.stop()
	}
	return nil
}

// countGapTransactions 返回 targetSet 中存在而 localSet 中不存在的独立
// 事务数量。两个集合按服务器 UUID 和 tag 用半开区间减法进行比较。
func countGapTransactions(targetSet, localSet mysql.GTIDSet) int {
	target, ok1 := targetSet.(*mysql.MysqlGTIDSet)
	local, ok2 := localSet.(*mysql.MysqlGTIDSet)
	if !ok1 || !ok2 {
		return 0
	}

	total := 0
	for sid, tags := range *target {
		for tag, iv := range tags {
			have := mysql.IntervalSlice{}
			if localTags, ok := (*local)[sid]; ok {
				have = localTags[tag]
			}
			for _, gap := range subtractIntervals(iv, have) {
				total += int(gap.Stop - gap.Start)
			}
		}
	}
	return total
}

// subtractIntervals 返回存在于 a 但未被 b 覆盖的半开区间。
// 两个切片都应是已排序/已归一化的。
func subtractIntervals(a, b mysql.IntervalSlice) mysql.IntervalSlice {
	var out mysql.IntervalSlice
	for _, x := range a {
		segments := mysql.IntervalSlice{{Start: x.Start, Stop: x.Stop}}
		for _, y := range b {
			var rest mysql.IntervalSlice
			for _, seg := range segments {
				switch {
				case y.Stop <= seg.Start || y.Start >= seg.Stop:
					rest = append(rest, seg) // 无重叠
				default:
					if y.Start > seg.Start {
						rest = append(rest, mysql.Interval{Start: seg.Start, Stop: y.Start})
					}
					if y.Stop < seg.Stop {
						rest = append(rest, mysql.Interval{Start: y.Stop, Stop: seg.Stop})
					}
				}
			}
			segments = rest
		}
		out = append(out, segments...)
	}
	out.Sort()
	return out
}

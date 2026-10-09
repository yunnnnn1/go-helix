package logic

import (
	"fmt"
	"sort"
	"strings"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
)

// ReplicationPosition 是与具体数据库无关、可比较的节点数据复制
// 位点。选举框架只需借助它了解节点应用变更
// 「已经走到了多远」。
//
// 每种数据库类型都提供各自的实现：MySQL 将 GTID /
// binlog 文件+位置编码为位点，PostgreSQL 编码为 WAL LSN，依此类推。核心
// 选举逻辑从不解析特定类型的位点。
type ReplicationPosition interface {
	// Compare 在本位点领先于 other 时返回 1，落后时
	// 返回 -1，两者相等时返回 0。
	Compare(other ReplicationPosition) int
}

// Candidate 将一个节点及其运行时状态封装起来，供选举比较使用。
type Candidate struct {
	Node *datacenter.Node

	IsAlive bool

	// Position 是该节点的复制位点（MySQL 为 GTID/binlog，
	// PostgreSQL 为 LSN）。当节点未上报位点时为 nil。
	Position ReplicationPosition

	// IsSemiSync 表示该节点是否受到同步 /
	// 半同步复制的保护（这是一个中立概念：MySQL semi-sync、
	// PostgreSQL synchronous commit）。
	IsSemiSync bool

	// InstanceCount 表示与该节点共享宿主机的数据库实例数量；
	// 密度越低越优先。
	InstanceCount int
}

// comparePosition 按复制位点比较两个候选节点。
//   - 双方都上报了位点          -> 委托给 Position.Compare
//   - 仅一方上报了位点          -> 有位点的一方胜出
//   - 双方都未上报位点          -> 平局（继续套用下一条规则）
//
// 返回比较结果，以及用于决策日志的简短方法标签。
func comparePosition(a, b *Candidate) (int, string) {
	aHas, bHas := a.Position != nil, b.Position != nil

	if aHas && bHas {
		return a.Position.Compare(b.Position), "Replication Position"
	}
	if aHas {
		return 1, "Has Position"
	}
	if bHas {
		return -1, "Has Position"
	}
	return 0, "No Position"
}

// ElectionStrategy 定义选择主库的接口。
type ElectionStrategy interface {
	SelectCandidate(candidates []*Candidate, failedMaster *datacenter.Node) (*Candidate, string)
}

// DefaultElectionStrategy 实现与具体数据库无关的决策树：
//  1. 排除已宕机 / 被标记为 bad / 非 ONLINE 的节点
//  2. 复制位点（委托给特定数据库类型的 Position）
//  3. 同步 / 半同步复制保护
//  4. 优先级（越高越好）
//  5. 位置（同机房 > 同区域 > 跨区域）
//  6. 密度（同机部署的实例越少越好）
//  7. 资源 / 磁盘使用率（越低越好）
type DefaultElectionStrategy struct{}

func (s *DefaultElectionStrategy) SelectCandidate(candidates []*Candidate, failedMaster *datacenter.Node) (*Candidate, string) {
	var logBuilder strings.Builder
	logBuilder.WriteString("\n=== Election Decision Tree ===\n")

	// 1. 过滤存活且未被标记为 bad 的节点
	logBuilder.WriteString("Step 1: Filtering Candidates\n")
	var valid []*Candidate
	for _, c := range candidates {
		isStatusOk := c.Node.Status == datacenter.StatusOnline

		if c.IsAlive && !c.Node.IsBad && isStatusOk {
			valid = append(valid, c)
			logBuilder.WriteString(fmt.Sprintf(
				"  ✅ [Candidate] %s:%d (SemiSync=%v, Prio=%d, HasPos=%v)\n",
				c.Node.Host, c.Node.Port, c.IsSemiSync, c.Node.Priority, c.Position != nil))
		} else {
			reason := "Unknown"
			switch {
			case !c.IsAlive:
				reason = "Not Alive (Probe Failed)"
			case c.Node.IsBad:
				reason = "Marked Bad (Manually)"
			case !isStatusOk:
				reason = fmt.Sprintf("Status %s (Expected ONLINE)", c.Node.Status)
			}
			logBuilder.WriteString(fmt.Sprintf("  ❌ [Dropped] %s:%d - Reason: %s\n", c.Node.Host, c.Node.Port, reason))
		}
	}

	if len(valid) == 0 {
		logBuilder.WriteString("Result: No valid candidates found.\n")
		return nil, logBuilder.String()
	}

	// 2. 对候选节点排序
	logBuilder.WriteString("Step 2: Ranking Candidates (Tournament)\n")

	sort.Slice(valid, func(i, j int) bool {
		c1 := valid[i]
		c2 := valid[j]

		logBuilder.WriteString(fmt.Sprintf("  ⚖️  Comparing %s vs %s:\n", c1.Node.Host, c2.Node.Host))

		// 2.1 复制位点（特定类型）
		posCmp, posMethod := comparePosition(c1, c2)
		if posCmp != 0 {
			better := posCmp > 0
			winner := c2.Node.Host
			if better {
				winner = c1.Node.Host
			}
			logBuilder.WriteString(fmt.Sprintf("      -> %s: %s wins\n", posMethod, winner))
			return better
		}
		logBuilder.WriteString(fmt.Sprintf("      -> %s: Equal\n", posMethod))

		// 2.2 同步 / 半同步复制（优先）
		if c1.IsSemiSync != c2.IsSemiSync {
			better := c1.IsSemiSync // true 优先于 false
			winner := c2.Node.Host
			if better {
				winner = c1.Node.Host
			}
			logBuilder.WriteString(fmt.Sprintf("      -> SyncRepl: %v vs %v => Winner: %s\n", c1.IsSemiSync, c2.IsSemiSync, winner))
			return better
		}
		logBuilder.WriteString("      -> SyncRepl: Equal\n")

		// 2.3 优先级（越高越好）
		if c1.Node.Priority != c2.Node.Priority {
			better := c1.Node.Priority > c2.Node.Priority
			winner := c2.Node.Host
			if better {
				winner = c1.Node.Host
			}
			logBuilder.WriteString(fmt.Sprintf("      -> Priority: %d vs %d => Winner: %s\n", c1.Node.Priority, c2.Node.Priority, winner))
			return better
		}
		logBuilder.WriteString("      -> Priority: Equal\n")

		// 2.4 位置（同机房 > 同区域）
		score1 := locationScore(c1.Node, failedMaster)
		score2 := locationScore(c2.Node, failedMaster)
		if score1 != score2 {
			better := score1 > score2
			winner := c2.Node.Host
			if better {
				winner = c1.Node.Host
			}
			logBuilder.WriteString(fmt.Sprintf("      -> Location Score: %d vs %d => Winner: %s\n", score1, score2, winner))
			return better
		}
		logBuilder.WriteString("      -> Location: Equal\n")

		// 2.5 密度（实例数越少越好）
		if c1.InstanceCount != c2.InstanceCount {
			better := c1.InstanceCount < c2.InstanceCount
			winner := c2.Node.Host
			if better {
				winner = c1.Node.Host
			}
			logBuilder.WriteString(fmt.Sprintf("      -> Density: %d vs %d => Winner: %s\n", c1.InstanceCount, c2.InstanceCount, winner))
			return better
		}
		logBuilder.WriteString("      -> Density: Equal\n")

		// 2.6 磁盘使用率（越低越好）
		if c1.Node.DiskUsagePct != c2.Node.DiskUsagePct {
			better := c1.Node.DiskUsagePct < c2.Node.DiskUsagePct
			winner := c2.Node.Host
			if better {
				winner = c1.Node.Host
			}
			logBuilder.WriteString(fmt.Sprintf("      -> Disk: %.1f vs %.1f => Winner: %s\n", c1.Node.DiskUsagePct, c2.Node.DiskUsagePct, winner))
			return better
		}

		return false
	})

	winner := valid[0]
	logBuilder.WriteString(fmt.Sprintf("🏆 Final Winner: %s:%d\n", winner.Node.Host, winner.Node.Port))
	logBuilder.WriteString("==============================\n")

	return winner, logBuilder.String()
}

func locationScore(node, master *datacenter.Node) int {
	if master == nil {
		return 0
	}
	if node.DC == master.DC {
		return 3 // 同机房
	}
	if node.Region == master.Region {
		return 2 // 同区域
	}
	return 1 // 跨区域
}

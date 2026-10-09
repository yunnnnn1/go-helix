package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/yunnnnn1/go-helix/pkg/logic"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// mysqlPosition 是 logic.ReplicationPosition 的 MySQL 实现。优先使用
// GTID；当 GTID 不可用时，回退到 binlog 文件 + 位点。这段逻辑原来
// 位于与数据库无关的 pkg/logic 选举代码中；它是 MySQL 专有的，
// 应放在这里。
type mysqlPosition struct {
	gtid string
	file string
	pos  int64
}

// 编译期断言，确保 mysqlPosition 满足中立接口。
var _ logic.ReplicationPosition = (*mysqlPosition)(nil)

// newMySQLPosition 根据上报的状态构建位点。当 GTID 和 binlog 文件
// 都不可用时返回 nil。
func newMySQLPosition(gtid, file string, pos int64) *mysqlPosition {
	switch {
	case gtid != "":
		return &mysqlPosition{gtid: gtid}
	case file != "":
		return &mysqlPosition{file: file, pos: pos}
	}
	return nil
}

// String 渲染位点（GTID，或 binlog 文件:位点）。
func (p *mysqlPosition) String() string {
	if p.gtid != "" {
		return p.gtid
	}
	return fmt.Sprintf("%s:%d", p.file, p.pos)
}

// Compare 实现 logic.ReplicationPosition。
func (p *mysqlPosition) Compare(other logic.ReplicationPosition) int {
	o, ok := other.(*mysqlPosition)
	if !ok {
		panic("mysql: cannot compare mysqlPosition with " + fmt.Sprintf("%T", other))
	}

	switch {
	case p.gtid != "" && o.gtid != "":
		return compareGTIDSet(p.gtid, o.gtid)
	case p.gtid != "":
		return 1 // GTID 优先于 binlog 位点
	case o.gtid != "":
		return -1
	}
	return compareBinlog(p.file, p.pos, o.file, o.pos)
}

// compareGTIDSet 比较两个 MySQL GTID 集合。它先用 MySQL GTID 语法
// 解析二者，并通过包含关系决定先后；若解析失败，或历史发生分叉
// 导致无法据此判定，则回退到比较数字序号，与此前的选举行为
// 保持一致。
func compareGTIDSet(a, b string) int {
	sa, errA := gomysql.ParseGTIDSet("mysql", a)
	sb, errB := gomysql.ParseGTIDSet("mysql", b)
	if errA == nil && errB == nil {
		switch {
		case sa.Equal(sb):
			return 0
		case sa.Contain(sb):
			return 1
		case sb.Contain(sa):
			return -1
		}
	}
	return compareMaxSequence(a, b)
}

// compareMaxSequence 按最大事务序号对两个 GTID 字符串排序。
// 这是兼容 MySQL/MariaDB 的兜底方式。
func compareMaxSequence(a, b string) int {
	sa, sb := gtidMaxSequence(a), gtidMaxSequence(b)
	switch {
	case sa > sb:
		return 1
	case sa < sb:
		return -1
	}
	return 0
}

// gtidMaxSequence 提取 GTID 字符串中所有集合的最大数字序号。
// 可处理 MySQL "uuid:1-100,uuid2:1-50" 和 MariaDB
// "domain-server-sequence" 形式。
func gtidMaxSequence(gtid string) int64 {
	var max int64
	for _, set := range strings.Split(gtid, ",") {
		set = strings.TrimSpace(set)
		if set == "" {
			continue
		}
		if s := oneGTIDMaxSequence(set); s > max {
			max = s
		}
	}
	return max
}

// oneGTIDMaxSequence 解析单个 GTID 集合并返回其最大序号。
func oneGTIDMaxSequence(set string) int64 {
	// MySQL："uuid:interval"，例如 "...:1-100"
	if idx := strings.LastIndex(set, ":"); idx >= 0 {
		interval := set[idx+1:]
		if dashIdx := strings.LastIndex(interval, "-"); dashIdx >= 0 {
			if n, err := strconv.ParseInt(interval[dashIdx+1:], 10, 64); err == nil {
				return n
			}
		}
		if n, err := strconv.ParseInt(interval, 10, 64); err == nil {
			return n
		}
	}

	// MariaDB："domain-server-sequence"
	parts := strings.Split(set, "-")
	if len(parts) >= 3 {
		if n, err := strconv.ParseInt(parts[len(parts)-1], 10, 64); err == nil {
			return n
		}
	}

	if n, err := strconv.ParseInt(set, 10, 64); err == nil {
		return n
	}
	return 0
}

// compareBinlog 比较两组 binlog 文件+位点，先按文件数字后缀
// 比较，再按文件内的位点比较。
func compareBinlog(fileA string, posA int64, fileB string, posB int64) int {
	sA, sB := binlogFileSequence(fileA), binlogFileSequence(fileB)
	if sA != sB {
		if sA > sB {
			return 1
		}
		return -1
	}
	if posA != posB {
		if posA > posB {
			return 1
		}
		return -1
	}
	return 0
}

// binlogFileSequence 提取 binlog 文件名的数字后缀
// （例如 "mysql-bin.000123" -> 123）。
func binlogFileSequence(filename string) int64 {
	if idx := strings.LastIndex(filename, "."); idx >= 0 {
		if n, err := strconv.ParseInt(filename[idx+1:], 10, 64); err == nil {
			return n
		}
	}
	return 0
}

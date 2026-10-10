package mysql

import (
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/logic"
)

const sidA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
const sidB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

func TestNewMySQLPosition(t *testing.T) {
	if p := newMySQLPosition("g", "", 0); p == nil || p.gtid != "g" {
		t.Fatalf("gtid position wrong: %+v", p)
	}
	if p := newMySQLPosition("", "f", 12); p == nil || p.file != "f" || p.pos != 12 {
		t.Fatalf("binlog position wrong: %+v", p)
	}
	if p := newMySQLPosition("", "", 0); p != nil {
		t.Fatalf("empty inputs should return nil, got %+v", p)
	}
}

func TestPositionString(t *testing.T) {
	if got := newMySQLPosition("g", "", 0).String(); got != "g" {
		t.Fatalf("gtid String wrong: %q", got)
	}
	if got := newMySQLPosition("", "f", 7).String(); got != "f:7" {
		t.Fatalf("binlog String wrong: %q", got)
	}
}

func TestPositionCompare(t *testing.T) {
	mk := func(gtid, file string, pos int64) *mysqlPosition { return newMySQLPosition(gtid, file, pos) }

	if c := mk(sidA+":1-10", "", 0).Compare(mk(sidA+":1-10", "", 0)); c != 0 {
		t.Fatalf("equal gtid want 0, got %d", c)
	}
	if c := mk(sidA+":1-10", "", 0).Compare(mk(sidA+":1-5", "", 0)); c != 1 {
		t.Fatalf("containing gtid want 1, got %d", c)
	}
	if c := mk(sidA+":1-5", "", 0).Compare(mk(sidA+":1-10", "", 0)); c != -1 {
		t.Fatalf("contained gtid want -1, got %d", c)
	}
	// GTID 优先于 binlog。
	if c := mk("g", "", 0).Compare(mk("", "f", 1)); c != 1 {
		t.Fatalf("gtid-over-binlog want 1, got %d", c)
	}
	if c := mk("", "f", 1).Compare(mk("g", "", 0)); c != -1 {
		t.Fatalf("binlog-vs-gtid want -1, got %d", c)
	}
	// binlog 文件序号比较。
	if c := mk("", "mysql-bin.000002", 0).Compare(mk("", "mysql-bin.000001", 0)); c != 1 {
		t.Fatalf("binlog file seq want 1, got %d", c)
	}
	// 同文件按位点。
	if c := mk("", "mysql-bin.000001", 100).Compare(mk("", "mysql-bin.000001", 80)); c != 1 {
		t.Fatalf("binlog pos want 1, got %d", c)
	}
	if c := mk("", "mysql-bin.000001", 80).Compare(mk("", "mysql-bin.000001", 80)); c != 0 {
		t.Fatalf("equal binlog want 0, got %d", c)
	}
}

func TestPositionComparePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("comparing with foreign type must panic")
		}
	}()
	newMySQLPosition("g", "", 0).Compare(&foreignPosition{})
}

type foreignPosition struct{}

func (f *foreignPosition) String() string                          { return "" }
func (f *foreignPosition) Compare(_ logic.ReplicationPosition) int { return 0 }

func TestOneGTIDMaxSequence(t *testing.T) {
	cases := map[string]int64{
		sidA + ":1-100": 100, // MySQL 区间
		sidA + ":42":    42,  // MySQL 单号
		"0-1-55":        55,  // MariaDB
		"99":            99,  // 纯数字
		"junk":          0,
	}
	for in, want := range cases {
		if got := oneGTIDMaxSequence(in); got != want {
			t.Errorf("oneGTIDMaxSequence(%q) want %d got %d", in, want, got)
		}
	}
}

func TestGTIDMaxSequence(t *testing.T) {
	in := sidA + ":1-10, " + sidB + ":1-200, , " + sidA + ":x"
	if got := gtidMaxSequence(in); got != 200 {
		t.Fatalf("max sequence want 200, got %d", got)
	}
}

func TestCompareMaxSequence(t *testing.T) {
	if c := compareMaxSequence(sidA+":1-9", sidA+":1-3"); c != 1 {
		t.Fatalf("want 1, got %d", c)
	}
	if c := compareMaxSequence(sidA+":1-3", sidA+":1-9"); c != -1 {
		t.Fatalf("want -1, got %d", c)
	}
	if c := compareMaxSequence(sidA+":1-9", sidA+":1-9"); c != 0 {
		t.Fatalf("want 0, got %d", c)
	}
}

func TestBinlogFileSequence(t *testing.T) {
	if got := binlogFileSequence("mysql-bin.000123"); got != 123 {
		t.Fatalf("want 123, got %d", got)
	}
	if got := binlogFileSequence("no-seq"); got != 0 {
		t.Fatalf("want 0, got %d", got)
	}
}

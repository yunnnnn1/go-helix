// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package gtidsync

import (
	"errors"
	"io"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

func binlogEventFor(t *testing.T, sid string, gno int64) *replication.BinlogEvent {
	t.Helper()
	return &replication.BinlogEvent{Event: newGTIDEvent(t, sid, gno)}
}

func TestRescuePlayer_BoundaryStops(t *testing.T) {
	sid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	pl := &rescuePlayer{
		targetSet: mustGTIDSet(t, sid+":1-10"),
	}

	// GNO 11 不在所捕获目标内 -> 重放前停止。
	err := pl.onEvent(binlogEventFor(t, sid, 11))
	if !errors.Is(err, errRescueBoundary) {
		t.Fatalf("err = %v, want errRescueBoundary", err)
	}
	if pl.skipping {
		t.Error("must not mark out-of-target txn as skipping")
	}
}

func TestRescuePlayer_AlreadyExecutedSkips(t *testing.T) {
	sid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	pl := &rescuePlayer{
		targetSet:    mustGTIDSet(t, sid+":1-10"),
		candidateSet: mustGTIDSet(t, sid+":1-5"),
	}

	// GNO 3 已存在于候选实例上 -> 跳过，不访问数据库（conn 为 nil）。
	err := pl.onEvent(binlogEventFor(t, sid, 3))
	if err != nil {
		t.Fatalf("skip must not error, got %v", err)
	}
	if !pl.skipping {
		t.Error("txn already executed on candidate must be marked skipping")
	}
}

func TestRescuePlayer_SkippedXIDResets(t *testing.T) {
	pl := &rescuePlayer{skipping: true}

	ev := &replication.BinlogEvent{Event: &replication.XIDEvent{}}
	if err := pl.onEvent(ev); err != nil {
		t.Fatalf("XID in skip mode returned error: %v", err)
	}
	if pl.skipping {
		t.Error("XID must clear the skipping flag")
	}
	if pl.applied != 0 {
		t.Error("skipped txn must not be counted applied")
	}
}

func TestConvertUnsigned(t *testing.T) {
	table := &schema.Table{
		Columns:         []schema.TableColumn{{}, {}},
		UnsignedColumns: []int{0, 1},
	}
	rows := [][]interface{}{{int8(-1), int16(-2)}}

	convertUnsigned(rows, table)

	if got, ok := rows[0][0].(uint8); !ok || got != 255 {
		t.Errorf("int8(-1) = %#v, want uint8(255)", rows[0][0])
	}
	if got, ok := rows[0][1].(uint16); !ok || got != 65534 {
		t.Errorf("int16(-2) = %#v, want uint16(65534)", rows[0][1])
	}
}

func TestConvertUnsigned_NoUnsignedColumns(t *testing.T) {
	table := &schema.Table{}
	rows := [][]interface{}{{int64(-1)}}
	convertUnsigned(rows, table)
	if _, ok := rows[0][0].(int64); !ok {
		t.Error("rows must be untouched when there are no unsigned columns")
	}
}

func TestIsTruncatedTail(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"EOF", io.EOF, true},
		{"wrapped EOF text", errors.New("unexpected EOF reading event"), true},
		{"plain error", errors.New("permission denied"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isTruncatedTail(c.err); got != c.want {
				t.Errorf("isTruncatedTail = %v, want %v", got, c.want)
			}
		})
	}
}

func TestJoinPort(t *testing.T) {
	if got := joinPort("10.0.0.1", "3306"); got != "10.0.0.1:3306" {
		t.Errorf("joinPort = %q", got)
	}
	if got := joinPort("10.0.0.1", ""); got != "10.0.0.1" {
		t.Errorf("joinPort empty port = %q", got)
	}
}

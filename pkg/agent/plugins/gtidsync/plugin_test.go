package gtidsync

import (
	"context"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

func TestPlugin_Identity(t *testing.T) {
	p := &GTIDSyncPlugin{}
	if p.Name() != "gtid_sync" {
		t.Errorf("Name = %q, want gtid_sync", p.Name())
	}
	if err := p.Init(context.Background(), nil); err != nil {
		t.Errorf("Init returned error: %v", err)
	}
}

func TestPlugin_UnknownAction(t *testing.T) {
	p := &GTIDSyncPlugin{}
	if _, err := p.Execute(context.Background(), "Nope", nil); err == nil {
		t.Error("expected error for unknown action")
	}
}

func TestPlugin_CatchupFrom_BadLocalAddress(t *testing.T) {
	p := &GTIDSyncPlugin{}

	// 不可达的本地地址（loopback:1 会被拒绝）必须在第一次 pool.Get 时失败
	// 并返回错误；不会启动任何 canal。
	params := map[string]string{
		"db_host": "127.0.0.1", "db_port": "1",
		"db_user": "u", "db_pass": "p",
		"host": "127.0.0.1", "port": "2",
	}
	out, err := p.Execute(context.Background(), actionCatchupFrom, params)
	if err == nil {
		t.Fatal("expected error connecting to unreachable local instance")
	}
	if out != nil {
		t.Errorf("expected nil result on failure, got %v", out)
	}
}

func TestGuardCanal_ExitsOnDone(t *testing.T) {
	// 已关闭的 done channel 会使 guardCanal 不触碰 canal 直接返回；这验证了
	// 正常完成路径不会残留阻塞的 goroutine。
	done := make(chan struct{})
	close(done)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		guardCanal(context.Background(), done, nil)
	}()
	<-finished
}

func mustGTIDSet(t *testing.T, s string) mysql.GTIDSet {
	t.Helper()
	set, err := mysql.ParseGTIDSet("mysql", s)
	if err != nil {
		t.Fatalf("parse gtid set %q: %v", s, err)
	}
	return set
}

func newGTIDEvent(t *testing.T, sid string, gno int64) *replication.GTIDEvent {
	t.Helper()
	u, err := uuid.Parse(sid)
	if err != nil {
		t.Fatalf("parse uuid %q: %v", sid, err)
	}
	return &replication.GTIDEvent{SID: u[:], GNO: gno}
}

func TestSyncHandler_OnGTIDBoundary(t *testing.T) {
	sid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	stopped := false
	h := &syncHandler{
		targetGtid: mustGTIDSet(t, sid+":1-10"),
		stop:       func() { stopped = true },
	}

	// 目标快照内的事务必须被重放，不停止。
	if err := h.OnGTID(nil, newGTIDEvent(t, sid, 5)); err != nil {
		t.Fatalf("OnGTID in-target returned error: %v", err)
	}
	if stopped {
		t.Fatal("stream must not stop while the transaction is inside target")
	}

	// 第一个越过目标边界的事务触发停止（在其行事件被派发之前）。
	if err := h.OnGTID(nil, newGTIDEvent(t, sid, 11)); err != nil {
		t.Fatalf("OnGTID past-boundary returned error: %v", err)
	}
	if !stopped {
		t.Fatal("stream must stop once the transaction is outside target")
	}
}

func TestSyncHandler_OnGTIDUnknownServerStops(t *testing.T) {
	// 来自目标快照中不存在的服务器 UUID 的事务处于追平位点之外，必须停止
	// 该流。
	sid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	other := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	stopped := false
	h := &syncHandler{
		targetGtid: mustGTIDSet(t, sid+":1-10"),
		stop:       func() { stopped = true },
	}
	if err := h.OnGTID(nil, newGTIDEvent(t, other, 1)); err != nil {
		t.Fatalf("OnGTID unknown-server returned error: %v", err)
	}
	if !stopped {
		t.Fatal("stream must stop for a GTID from a server outside target")
	}
}

func TestSyncHandler_OnXIDCompletion(t *testing.T) {
	stopped := false
	h := &syncHandler{
		gapTotal: 3,
		stop:     func() { stopped = true },
	}

	for i := 1; i <= 2; i++ {
		if err := h.OnXID(nil, mysql.Position{}); err != nil {
			t.Fatalf("OnXID #%d returned error: %v", i, err)
		}
		if stopped {
			t.Fatalf("stream stopped early after %d transaction(s)", i)
		}
	}
	if err := h.OnXID(nil, mysql.Position{}); err != nil {
		t.Fatalf("OnXID #3 returned error: %v", err)
	}
	if !stopped {
		t.Fatal("stream must stop once applied reaches gapTotal")
	}
}

func TestSyncHandler_OnXIDSkipsFailedTxn(t *testing.T) {
	stopped := false
	h := &syncHandler{
		gapTotal:  1,
		txnFailed: true,
		stop:      func() { stopped = true },
	}
	if err := h.OnXID(nil, mysql.Position{}); err != nil {
		t.Fatalf("OnXID returned error: %v", err)
	}
	if stopped {
		t.Fatal("stream must not declare completion when the transaction had failed rows")
	}
}

func TestCountGapTransactions(t *testing.T) {
	sid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	other := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

	cases := []struct {
		name   string
		target string
		local  string
		want   int
	}{
		{"equal sets", sid + ":1-10", sid + ":1-10", 0},
		{"local behind", sid + ":1-10", sid + ":1-5", 5},
		{"local empty", sid + ":1-10", "", 10},
		{"local ahead", sid + ":1-10", sid + ":1-20", 0},
		{"gap inside interval", sid + ":1-10", sid + ":3-7", 5},
		{"diverged servers", sid + ":1-10", other + ":1-5", 10},
		{"multi server target", sid + ":1-10," + other + ":1-5", sid + ":1-10", 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := countGapTransactions(mustGTIDSet(t, c.target), mustGTIDSet(t, c.local))
			if got != c.want {
				t.Errorf("gap = %d, want %d", got, c.want)
			}
		})
	}
}

func TestSubtractIntervals(t *testing.T) {
	cases := []struct {
		name string
		a    mysql.IntervalSlice
		b    mysql.IntervalSlice
		want mysql.IntervalSlice
	}{
		{
			name: "split middle",
			a:    mysql.IntervalSlice{{Start: 1, Stop: 11}},
			b:    mysql.IntervalSlice{{Start: 4, Stop: 7}},
			want: mysql.IntervalSlice{{Start: 1, Stop: 4}, {Start: 7, Stop: 11}},
		},
		{
			name: "no overlap",
			a:    mysql.IntervalSlice{{Start: 1, Stop: 11}},
			b:    mysql.IntervalSlice{{Start: 20, Stop: 30}},
			want: mysql.IntervalSlice{{Start: 1, Stop: 11}},
		},
		{
			name: "fully covered",
			a:    mysql.IntervalSlice{{Start: 1, Stop: 11}},
			b:    mysql.IntervalSlice{{Start: 1, Stop: 20}},
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := subtractIntervals(c.a, c.b)
			if len(got) != len(c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("got %v, want %v", got, c.want)
				}
			}
		})
	}
}

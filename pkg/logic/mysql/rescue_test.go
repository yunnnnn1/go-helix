package mysql

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeTailAgent 仅为故障节点实现 StreamBinlogTail。
type fakeTailAgent struct {
	pb.UnimplementedDBAgentServer
	chunks  []*pb.BinlogChunk
	lastReq *pb.BinlogTailRequest
}

func (f *fakeTailAgent) StreamBinlogTail(req *pb.BinlogTailRequest, srv pb.DBAgent_StreamBinlogTailServer) error {
	f.lastReq = req
	for _, c := range f.chunks {
		if err := srv.Send(c); err != nil {
			return err
		}
	}
	return nil
}

// fakeRescueAgent 仅为候选节点实现 ApplyBinlogRescue。
type fakeRescueAgent struct {
	pb.UnimplementedDBAgentServer
	success    bool
	gotBytes   int64
	targetGTID string
	sessionID  string
}

func (f *fakeRescueAgent) ApplyBinlogRescue(srv pb.DBAgent_ApplyBinlogRescueServer) error {
	for {
		msg, err := srv.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		f.gotBytes += int64(len(msg.Data))
		if msg.Done {
			f.targetGTID = msg.TargetGtid
			f.sessionID = msg.SessionId
			return srv.SendAndClose(&pb.BinlogRescueResult{
				Success: f.success,
				Message: "ok",
			})
		}
	}
}

// startFakeAgent 在真实的本地 TCP 端口上提供 agent 服务
// （getAgentClient 通过 TCP 连接），并返回节点描述符和已连接的客户端。
func startFakeAgent(t *testing.T, agent pb.DBAgentServer) (*datacenter.Node, pb.DBAgentClient) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterDBAgentServer(srv, agent)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	port := lis.Addr().(*net.TCPAddr).Port
	node := &datacenter.Node{Host: "127.0.0.1", AgentPort: int32(port)}
	return node, pb.NewDBAgentClient(conn)
}

func TestRescueBinlogTail_ForwardsChunks(t *testing.T) {
	tail := &fakeTailAgent{chunks: []*pb.BinlogChunk{
		{Data: []byte("AAAA")},
		{Data: []byte("BBBBBBBB")},
		{Done: true},
	}}
	failedNode, _ := startFakeAgent(t, tail)

	rescue := &fakeRescueAgent{success: true}
	_, candidateClient := startFakeAgent(t, rescue)

	candidateStatus := map[string]string{
		"Relay_Master_Log_File": "mysql-bin.000003",
		"Exec_Master_Log_Pos":   "123",
		"gtid_executed":         "uuid:1-5",
	}

	enabledRescue := datacenter.NodeBinlogRescue{Enabled: true}
	if err := runRescueBinlogTail(context.Background(), failedNode, enabledRescue, enabledRescue, candidateStatus, "uuid:1-10", candidateClient); err != nil {
		t.Fatalf("rescueBinlogTail: %v", err)
	}

	// 所有数据字节都已转发到候选节点一侧。
	if rescue.gotBytes != 12 {
		t.Errorf("forwarded bytes = %d, want 12", rescue.gotBytes)
	}
	// 目标/会话在最终标记中送达。
	if rescue.targetGTID != "uuid:1-10" {
		t.Errorf("target GTID = %q, want uuid:1-10", rescue.targetGTID)
	}
	if rescue.sessionID == "" {
		t.Error("session id must be delivered")
	}
	// 起始坐标取自候选节点的状态快照。
	if tail.lastReq.StartFile != "mysql-bin.000003" {
		t.Errorf("start file = %q, want mysql-bin.000003", tail.lastReq.StartFile)
	}
	if tail.lastReq.StartPos != 123 {
		t.Errorf("start pos = %d, want 123", tail.lastReq.StartPos)
	}
	if tail.lastReq.StartGtid != "uuid:1-5" {
		t.Errorf("start gtid = %q, want uuid:1-5", tail.lastReq.StartGtid)
	}
}

func TestRescueBinlogTail_CandidateFailure(t *testing.T) {
	tail := &fakeTailAgent{chunks: []*pb.BinlogChunk{
		{Data: []byte("AAAA")},
		{Done: true},
	}}
	failedNode, _ := startFakeAgent(t, tail)

	rescue := &fakeRescueAgent{success: false}
	_, candidateClient := startFakeAgent(t, rescue)

	enabled := datacenter.NodeBinlogRescue{Enabled: true}
	if err := runRescueBinlogTail(context.Background(), failedNode, enabled, enabled, nil, "uuid:1-10", candidateClient); err == nil {
		t.Fatal("expected error when candidate reports unsuccessful apply")
	}
}

func TestRescueBinlogTail_NoRescueBytes(t *testing.T) {
	// 尾部流只返回完成标记，没有数据。
	tail := &fakeTailAgent{chunks: []*pb.BinlogChunk{{Done: true}}}
	failedNode, _ := startFakeAgent(t, tail)

	rescue := &fakeRescueAgent{success: true}
	_, candidateClient := startFakeAgent(t, rescue)

	enabled := datacenter.NodeBinlogRescue{Enabled: true}
	if err := runRescueBinlogTail(context.Background(), failedNode, enabled, enabled, nil, "uuid:1-10", candidateClient); err == nil {
		t.Fatal("expected error when no rescue bytes are produced")
	}
}

func TestGtidSetContains(t *testing.T) {
	sid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	cases := []struct {
		name string
		set  string
		want string
		ok   bool
	}{
		{"contained", sid + ":1-10", sid + ":3-7", true},
		{"not contained", sid + ":1-5", sid + ":1-10", false},
		{"equal", sid + ":1-10", sid + ":1-10", true},
		{"empty set", "", sid + ":1-10", false},
		{"empty other", sid + ":1-10", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := gtidSetContains(c.set, c.want); got != c.ok {
				t.Errorf("gtidSetContains(%q,%q) = %v, want %v", c.set, c.want, got, c.ok)
			}
		})
	}
}

func TestStatusFirst(t *testing.T) {
	st := map[string]string{"a": "", "b": "v", "c": "w"}
	if got := statusFirst(st, "a", "b"); got != "v" {
		t.Errorf("statusFirst = %q, want v", got)
	}
	if got := statusFirst(st, "a"); got != "" {
		t.Errorf("statusFirst missing key = %q, want empty", got)
	}
}

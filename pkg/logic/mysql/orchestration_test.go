package mysql

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/datacenter"
	"github.com/yunnnnn1/go-helix/pkg/logic"

	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

// orchAgent 是可配置的进程内 DBAgent 假实现：支持 Ping/状态队列/
// 按操作返回错误/插件结果，用于覆盖各编排函数。
type orchAgent struct {
	pb.UnimplementedDBAgentServer
	mu sync.Mutex

	alive         bool
	statusQueue   []map[string]string
	statusDefault map[string]string
	execErr       map[string]error
	pluginOK      bool
	ops           []string
}

func (a *orchAgent) Ping(context.Context, *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{Alive: a.alive}, nil
}

func (a *orchAgent) GetStatus(context.Context, *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var st map[string]string
	if len(a.statusQueue) > 0 {
		st = a.statusQueue[0]
		a.statusQueue = a.statusQueue[1:]
	} else {
		st = a.statusDefault
	}
	out := map[string]string{}
	for k, v := range st {
		out[k] = v
	}
	return &pb.GetStatusResponse{Status: out}, nil
}

func (a *orchAgent) ExecuteOperation(_ context.Context, req *pb.OperationRequest) (*pb.OperationResponse, error) {
	a.mu.Lock()
	a.ops = append(a.ops, req.Operation)
	err := a.execErr[req.Operation]
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &pb.OperationResponse{}, nil
}

func (a *orchAgent) ExecutePlugin(context.Context, *pb.PluginRequest) (*pb.PluginResponse, error) {
	return &pb.PluginResponse{Success: a.pluginOK}, nil
}

// startOrch 启动假 agent 并返回其 TCP 端口。
func startOrch(t *testing.T, a *orchAgent) int32 {
	t.Helper()
	base, _ := startFakeAgent(t, a)
	return base.AgentPort
}

func mkNode(agentPort int32, role string, dbPort int) *datacenter.Node {
	return &datacenter.Node{
		Host: "127.0.0.1", Port: int32(dbPort), AgentPort: agentPort,
		Role: role, ClusterName: "c", Status: datacenter.StatusOnline,
	}
}

func deadTCPPort(t *testing.T) int32 {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := lis.Addr().(*net.TCPAddr).Port
	lis.Close()
	return int32(p)
}

// ---------------- Elect ----------------

func TestElectPreferred(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := startOrch(t, &orchAgent{})
	port := agent

	failed := mkNode(port, "master", 3306)
	preferred := mkNode(port, "slave", 3308)
	other := mkNode(port, "slave", 3307)

	res, err := (&MySQLSwitcher{}).Elect(context.Background(), failed,
		[]*datacenter.Node{failed, preferred, other}, preferred)
	if err != nil {
		t.Fatalf("Elect: %v", err)
	}
	if res.NewMaster != preferred {
		t.Fatal("preferred node must be selected")
	}
	if len(res.Slaves) != 1 || res.Slaves[0] != other {
		t.Fatalf("remaining slaves wrong: %+v", res.Slaves)
	}
}

func TestElectAutoProbe(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := &orchAgent{alive: true, statusDefault: map[string]string{
		"gtid_executed": sidA + ":1-10",
	}}
	port := startOrch(t, agent)

	failed := mkNode(port, "master", 3306)
	s1 := mkNode(port, "slave", 3307)
	s2 := mkNode(port, "slave", 3308)

	res, err := (&MySQLSwitcher{}).Elect(context.Background(), failed,
		[]*datacenter.Node{failed, s1, s2}, nil)
	if err != nil {
		t.Fatalf("Elect auto: %v", err)
	}
	if res.NewMaster == nil || res.NewMaster.Role != "slave" {
		t.Fatalf("a slave should be elected: %+v", res.NewMaster)
	}
	if len(res.Slaves) != 1 {
		t.Fatalf("one remaining slave expected, got %d", len(res.Slaves))
	}
}

// ---------------- Failover ----------------

func TestFailoverLevel1Catchup(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := &orchAgent{
		statusDefault: map[string]string{"gtid_executed": sidA + ":1-10"},
		statusQueue: []map[string]string{
			{"gtid_executed": sidA + ":1-10"}, // 故障主：目标位点
			{"gtid_executed": sidA + ":1-5"},  // 候选初始落后
			{"gtid_executed": sidA + ":1-10"}, // Level1 WaitCatchup 后追平
		},
	}
	port := startOrch(t, agent)

	failed := mkNode(port, "master", 3306)
	candidate := mkNode(port, "slave", 3307)
	election := &logic.ElectionResult{NewMaster: candidate}

	if err := (&MySQLSwitcher{}).Failover(context.Background(), failed, election); err != nil {
		t.Fatalf("Failover happy: %v", err)
	}
}

func TestFailoverImmediateReached(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := &orchAgent{statusDefault: map[string]string{
		"gtid_executed": sidA + ":1-10",
	}}
	port := startOrch(t, agent)

	failed := mkNode(port, "master", 3306)
	candidate := mkNode(port, "slave", 3307)

	if err := (&MySQLSwitcher{}).Failover(context.Background(), failed,
		&logic.ElectionResult{NewMaster: candidate}); err != nil {
		t.Fatalf("Failover immediate: %v", err)
	}
}

func TestFailoverIncompleteAbort(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := &orchAgent{
		pluginOK:      false,
		statusDefault: map[string]string{"gtid_executed": sidA + ":1-5"}, // 候选始终落后
		statusQueue: []map[string]string{
			{"gtid_executed": sidA + ":1-10"}, // 目标位点
		},
	}
	port := startOrch(t, agent)

	failed := mkNode(port, "master", 3306)
	candidate := mkNode(port, "slave", 3307)

	err := (&MySQLSwitcher{}).Failover(context.Background(), failed,
		&logic.ElectionResult{NewMaster: candidate})
	if err == nil {
		t.Fatal("incomplete candidate must abort before promotion")
	}
}

func TestFailoverTargetUnknownProceeds(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	// 候选正常；故障节点指向无监听端口 → 目标 GTID 未知，走兜底分支。
	agent := &orchAgent{statusDefault: map[string]string{
		"gtid_executed": sidA + ":1-10",
	}}
	candPort := startOrch(t, agent)
	dead := deadTCPPort(t)

	failed := mkNode(dead, "master", 3306)
	failed.AgentPort = dead
	candidate := mkNode(candPort, "slave", 3307)

	if err := (&MySQLSwitcher{}).Failover(context.Background(), failed,
		&logic.ElectionResult{NewMaster: candidate}); err != nil {
		t.Fatalf("unknown-target failover should proceed: %v", err)
	}
}

// ---------------- GracefulMasterTakeover ----------------

func TestGracefulTakeover(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := &orchAgent{
		statusQueue: []map[string]string{
			{"super_read_only": "ON"}, // 旧主冻结回读确认
		},
	}
	port := startOrch(t, agent)

	oldMaster := mkNode(port, "master", 3306)
	candidate := mkNode(port, "slave", 3308)
	election := &logic.ElectionResult{NewMaster: candidate}

	if err := (&MySQLSwitcher{}).GracefulMasterTakeover(context.Background(),
		oldMaster, election); err != nil {
		t.Fatalf("GracefulTakeover: %v", err)
	}
}

func TestGracefulTakeoverCatchupFailRollback(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := &orchAgent{
		execErr: map[string]error{"WaitCatchup": context.DeadlineExceeded},
		statusQueue: []map[string]string{
			{"super_read_only": "ON"},
		},
	}
	port := startOrch(t, agent)

	oldMaster := mkNode(port, "master", 3306)
	candidate := mkNode(port, "slave", 3308)

	err := (&MySQLSwitcher{}).GracefulMasterTakeover(context.Background(), oldMaster,
		&logic.ElectionResult{NewMaster: candidate})
	if err == nil {
		t.Fatal("WaitCatchup failure must roll back and return error")
	}
}

// ---------------- Recover / Rebuild ----------------

func TestRecoverReplica(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	port := startOrch(t, &orchAgent{})

	master := mkNode(port, "master", 3306)
	target := mkNode(port, "slave", 3307)

	if err := (&MySQLSwitcher{}).RecoverReplica(context.Background(), master, target); err != nil {
		t.Fatalf("RecoverReplica: %v", err)
	}
}

func TestRecoverReplicaTargetIsMaster(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	port := startOrch(t, &orchAgent{})
	master := mkNode(port, "master", 3306)

	if err := (&MySQLSwitcher{}).RecoverReplica(context.Background(), master, master); err == nil {
		t.Fatal("target identical to master must error")
	}
}

func TestRebuildTopologyFromSlave(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	port := startOrch(t, &orchAgent{})

	desired := mkNode(port, "slave", 3306)
	other := mkNode(port, "slave", 3307)

	if err := (&MySQLSwitcher{}).RebuildTopology(context.Background(),
		desired, []*datacenter.Node{desired, other}); err != nil {
		t.Fatalf("RebuildTopology: %v", err)
	}
}

func TestRebuildTopologyAlreadyMaster(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	port := startOrch(t, &orchAgent{})

	desired := mkNode(port, "master", 3306)
	other := mkNode(port, "slave", 3307)

	if err := (&MySQLSwitcher{}).RebuildTopology(context.Background(),
		desired, []*datacenter.Node{desired, other}); err != nil {
		t.Fatalf("RebuildTopology master: %v", err)
	}
}

// ---------------- Intermediate / Rehome ----------------

func TestRecoverIntermediateMaster(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	agent := &orchAgent{alive: true, statusDefault: map[string]string{
		"gtid_executed": sidA + ":1-10",
	}}
	port := startOrch(t, agent)

	up := mkNode(port, "master", 3306)
	im := mkNode(port, "slave", 3307)
	im.MasterHost, im.MasterPort = "127.0.0.1", 3306
	winner := mkNode(port, "slave", 3308)
	winner.MasterHost, winner.MasterPort = "127.0.0.1", 3307
	sibling := mkNode(port, "slave", 3309)
	sibling.MasterHost, sibling.MasterPort = "127.0.0.1", 3307

	err := (&MySQLSwitcher{}).RecoverIntermediateMaster(context.Background(), im,
		[]*datacenter.Node{up, im, winner, sibling})
	if err != nil {
		t.Fatalf("RecoverIntermediate: %v", err)
	}
}

func TestRehomeToEdges(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	port := startOrch(t, &orchAgent{})

	root := mkNode(port, "master", 3306)
	child := mkNode(port, "slave", 3307)
	edges := []logic.TopologyEdge{{Upstream: root.Addr(), Child: child.Addr()}}

	if err := (&MySQLSwitcher{}).RehomeToEdges(context.Background(),
		[]*datacenter.Node{root, child}, edges); err != nil {
		t.Fatalf("Rehome: %v", err)
	}
}

func TestRehomeToEdgesUnknownChild(t *testing.T) {
	loadFenceTestConfig(t, "disabled")
	port := startOrch(t, &orchAgent{})
	root := mkNode(port, "master", 3306)
	edges := []logic.TopologyEdge{{Upstream: root.Addr(), Child: "127.0.0.1:9999"}}

	if err := (&MySQLSwitcher{}).RehomeToEdges(context.Background(),
		[]*datacenter.Node{root}, edges); err == nil {
		t.Fatal("edge referencing unknown child must error")
	}
}

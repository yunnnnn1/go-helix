package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/agent/backend"
	"github.com/yunnnnn1/go-helix/pkg/agent/plugins"
	pb "github.com/yunnnnn1/go-helix/pkg/proto"
)

// MockBackend 是 backend.Backend 的 mock 实现，用于测试
type MockBackend struct {
	pingResult  bool
	pingError   error
	status      map[string]string
	statusError error
	opError     error
}

func (m *MockBackend) Configure(host string, port int, user, pass string) error {
	return nil
}

func (m *MockBackend) SetReplicationCredentials(user, pass string) error {
	return nil
}

func (m *MockBackend) ConfigureManager(host string, port int, user, pass string) error {
	return nil
}

func (m *MockBackend) SetCluster(name string) {}

func (m *MockBackend) Ping(ctx context.Context) (bool, error) {
	return m.pingResult, m.pingError
}

func (m *MockBackend) GetStatus(ctx context.Context) (map[string]string, error) {
	return m.status, m.statusError
}

func (m *MockBackend) KillProcess(ctx context.Context, processID int64, isOsPid bool) error {
	return nil
}

func (m *MockBackend) ExecuteOperation(ctx context.Context, operation string, params map[string]string) (map[string]string, error) {
	if m.opError != nil {
		return nil, m.opError
	}
	return map[string]string{"status": "success"}, nil
}

func (m *MockBackend) Discover() ([]backend.InstanceInfo, error) {
	return nil, nil
}

func (m *MockBackend) Clone() backend.Backend {
	return &MockBackend{}
}

func TestNewServer(t *testing.T) {
	// 注册用于测试的 mock backend
	backend.Register("mock", &MockBackend{})

	// 使用 mock backend 进行测试
	server, err := NewServer("mock", "localhost", 3306, "root", "", "repl", "repl", "manager", "manager", "test-cluster")
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	if server == nil {
		t.Fatal("NewServer returned nil")
	}
}

func TestServer_Ping(t *testing.T) {
	// 使用 mock backend 创建 server
	server := &Server{
		backend: &MockBackend{
			pingResult: true,
			pingError:  nil,
		},
	}

	// 测试 Ping
	ctx := context.Background()
	response, err := server.Ping(ctx, &pb.PingRequest{})
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	if !response.Alive {
		t.Error("Expected Ping to return true")
	}
}

func TestServer_GetStatus(t *testing.T) {
	// 使用 mock backend 创建 server
	expectedStatus := map[string]string{
		"read_only":     "0",
		"gtid_executed": "0-1-100",
	}

	server := &Server{
		backend: &MockBackend{
			status:      expectedStatus,
			statusError: nil,
		},
	}

	// 测试 GetStatus
	ctx := context.Background()
	response, err := server.GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if len(response.Status) != len(expectedStatus) {
		t.Errorf("Expected status length %d, got %d", len(expectedStatus), len(response.Status))
	}

	for key, value := range expectedStatus {
		if response.Status[key] != value {
			t.Errorf("Expected status[%s] to be %s, got %s", key, value, response.Status[key])
		}
	}
}

func TestServer_ExecuteOperationPropagatesError(t *testing.T) {
	wantErr := errors.New("WaitCatchup timed out")
	server := &Server{
		backend: &MockBackend{opError: wantErr},
	}

	resp, err := server.ExecuteOperation(context.Background(), &pb.OperationRequest{
		Operation: "WaitCatchup",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected business error to be returned as gRPC error, got %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response on error, got %+v", resp)
	}
}

func TestServer_ValidateShellArgs(t *testing.T) {
	cases := []struct {
		name    string
		command string
		args    []string
		wantErr bool
	}{
		{"echo safe text", "echo", []string{"hello", "world"}, false},
		{"echo no-newline flag", "echo", []string{"-n", "done"}, false},
		{"echo disallowed flag", "echo", []string{"-e", "x"}, true},
		{"echo unsafe chars", "echo", []string{"hello;rm"}, true},
		{"echo too many args", "echo", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, true},
		{"ping single host", "ping", []string{"-c", "4", "10.0.0.1"}, false},
		{"ping hostname target", "ping", []string{"db-1.example.com"}, false},
		{"ping disallowed flag", "ping", []string{"-f", "10.0.0.1"}, true},
		{"ping non-numeric count", "ping", []string{"-c", "x", "10.0.0.1"}, true},
		{"ping two targets", "ping", []string{"10.0.0.1", "10.0.0.2"}, true},
		{"ping bad target", "ping", []string{"bad;host"}, true},
		{"df unrestricted", "df", []string{"-h"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateShellArgs(c.command, c.args)
			if c.wantErr && err == nil {
				t.Errorf("validateShellArgs(%s, %v) expected error", c.command, c.args)
			}
			if !c.wantErr && err != nil {
				t.Errorf("validateShellArgs(%s, %v) unexpected error: %v", c.command, c.args, err)
			}
		})
	}
}

func TestServer_ExecuteShell(t *testing.T) {
	server := &Server{}

	// 使用允许的命令测试
	ctx := context.Background()
	response, err := server.ExecuteShell(ctx, &pb.ShellRequest{
		Command: "echo",
		Args:    []string{"test"},
	})

	if err != nil {
		t.Fatalf("ExecuteShell failed: %v", err)
	}
	if !response.Success {
		t.Error("Expected allowed command to return success")
	}

	// 使用不允许的命令测试
	response, err = server.ExecuteShell(ctx, &pb.ShellRequest{
		Command: "rm",
		Args:    []string{"-rf", "/"},
	})

	if err != nil {
		t.Fatalf("ExecuteShell failed: %v", err)
	}

	if response.Success {
		t.Error("Expected disallowed command to return false")
	}

	if response.Stderr != "command not allowed for security reasons" {
		t.Errorf("Expected error message 'command not allowed for security reasons', got %s", response.Stderr)
	}
}

func TestServer_ExecutePlugin(t *testing.T) {
	server := &Server{}

	// 使用不存在的插件测试
	ctx := context.Background()
	response, err := server.ExecutePlugin(ctx, &pb.PluginRequest{
		PluginName: "non-existent",
		Action:     "test",
		Params:     map[string]string{},
	})

	if err != nil {
		t.Fatalf("ExecutePlugin failed: %v", err)
	}

	if response.Success {
		t.Error("Expected non-existent plugin to return false")
	}
}

// capturePlugin 记录 Server 传递给插件的参数。
type capturePlugin struct {
	got map[string]string
}

func (c *capturePlugin) Name() string                                          { return "capture_test_plugin" }
func (c *capturePlugin) Init(ctx context.Context, cfg map[string]string) error { return nil }
func (c *capturePlugin) Execute(ctx context.Context, action string, params map[string]string) (map[string]string, error) {
	c.got = params
	return map[string]string{}, nil
}

func TestServer_ExecutePluginInjectsInstanceCreds(t *testing.T) {
	capture := &capturePlugin{}
	plugins.Register(capture) // 重复注册安全

	server := &Server{
		dbHost:  "10.0.0.1",
		dbPort:  3306,
		dbUser:  "app_user",
		dbPass:  "secret",
		mgrUser: "manager",
		mgrPass: "managerpass",
	}

	// 网络传入的参数只携带目标地址，绝不携带凭据。
	orig := map[string]string{"host": "10.0.0.2", "port": "3306"}
	resp, err := server.ExecutePlugin(context.Background(), &pb.PluginRequest{
		PluginName: "capture_test_plugin",
		Action:     "run",
		Params:     orig,
	})
	if err != nil {
		t.Fatalf("ExecutePlugin: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected plugin success, got %s", resp.Message)
	}

	want := map[string]string{
		"host": "10.0.0.2", "port": "3306", // 目标参数保留
		"db_host": "10.0.0.1", "db_port": "3306",
		"db_user": "app_user", "db_pass": "secret", // 注入探测凭据
		"manager_user": "manager", "manager_pass": "managerpass", // 注入管理凭据
	}
	for k, v := range want {
		if capture.got[k] != v {
			t.Errorf("injected params[%s] = %q, want %q", k, capture.got[k], v)
		}
	}

	// 原始请求参数必须保持不变。
	if len(orig) != 2 || orig["host"] != "10.0.0.2" || orig["port"] != "3306" {
		t.Errorf("original request params were modified: %v", orig)
	}
}

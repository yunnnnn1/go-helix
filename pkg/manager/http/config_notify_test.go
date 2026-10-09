package http

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yunnnnn1/go-helix/pkg/config"
	"github.com/yunnnnn1/go-helix/pkg/raft"
)

func TestHandleConfigNotifyGet(t *testing.T) {
	config.SetNotifyOverride(&config.NotifyConfig{
		Enabled: true,
		Email: config.EmailNotifyConfig{
			Enabled: true, Host: "smtp.example.com", Password: "hidden-pw",
		},
	})
	defer config.SetNotifyOverride(nil)

	s := NewServer(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/config/notify", nil)
	w := httptest.NewRecorder()
	s.handleConfigNotify(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	var view map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if view["enabled"] != true {
		t.Fatalf("want enabled=true, got %v", view["enabled"])
	}
	email := view["email"].(map[string]interface{})
	if email["has_password"] != true {
		t.Fatal("has_password should be true")
	}
	if strings.Contains(w.Body.String(), "hidden-pw") {
		t.Fatal("GET view must not leak the SMTP password")
	}
}

// TestHandleConfigNotifyPostSuccess 通过 httptest webhook 验证“发送测试通知”
// 的完整 HTTP → notify.Dispatch 链路。
func TestHandleConfigNotifyPostSuccess(t *testing.T) {
	var received int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received++
		_, _ = io.WriteString(w, `{"errcode":0,"errmsg":"ok"}`)
	}))
	defer srv.Close()

	config.SetNotifyOverride(&config.NotifyConfig{
		Enabled: true,
		WeChat:  config.WebhookNotifyConfig{Enabled: true, URL: srv.URL},
	})
	defer config.SetNotifyOverride(nil)

	s := NewServer(nil, &raft.Node{RaftID: "node-test"})
	req := httptest.NewRequest(http.MethodPost, "/config/notify",
		strings.NewReader(`{"channel":"wechat"}`))
	w := httptest.NewRecorder()
	s.handleConfigNotify(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if received != 1 {
		t.Fatalf("webhook should be hit once, got %d", received)
	}
	var out struct {
		Results []map[string]interface{} `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(out.Results) != 1 || out.Results[0]["success"] != true {
		t.Fatalf("want one successful result, got %v", out.Results)
	}
}

func TestHandleConfigNotifyPostInvalidChannel(t *testing.T) {
	s := NewServer(nil, &raft.Node{RaftID: "node-test"})
	req := httptest.NewRequest(http.MethodPost, "/config/notify",
		strings.NewReader(`{"channel":"bogus"}`))
	w := httptest.NewRecorder()
	s.handleConfigNotify(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
}

// TestHandleConfigNotifyPostDisabled 验证总开关关闭时测试通知失败即关闭（500）。
func TestHandleConfigNotifyPostDisabled(t *testing.T) {
	config.SetNotifyOverride(&config.NotifyConfig{Enabled: false})
	defer config.SetNotifyOverride(nil)

	s := NewServer(nil, &raft.Node{RaftID: "node-test"})
	req := httptest.NewRequest(http.MethodPost, "/config/notify",
		strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	s.handleConfigNotify(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("want 500 when disabled, got %d", w.Code)
	}
}

func TestHandleConfigNotifyMethodNotAllowed(t *testing.T) {
	s := NewServer(nil, nil)
	req := httptest.NewRequest(http.MethodDelete, "/config/notify", nil)
	w := httptest.NewRecorder()
	s.handleConfigNotify(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", w.Code)
	}
}

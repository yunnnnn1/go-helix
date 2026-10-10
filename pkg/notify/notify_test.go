// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
)

// resetNotify 在测试结束后移除注入的覆盖配置与节流记录，避免污染其他用例。
func resetNotify() {
	config.SetNotifyOverride(nil)
	throttleMu.Lock()
	lastSent = map[string]time.Time{}
	throttleMu.Unlock()
}

// dispatchOne 以只投递指定渠道的方式同步分发，并返回唯一结果。
func dispatchOne(t *testing.T, ev *Event, channel string) ChannelResult {
	t.Helper()
	results, err := Dispatch(context.Background(), ev, channel)
	if err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("want exactly 1 channel result, got %d", len(results))
	}
	return results[0]
}

func sampleEvent() *Event {
	return &Event{
		Title:    "Failover完成：cluster1",
		Message:  "已切换到新主库",
		Cluster:  "cluster1",
		Code:     "DeadMaster",
		Severity: SeverityInfo,
	}
}

func TestDispatchDisabled(t *testing.T) {
	config.SetNotifyOverride(&config.NotifyConfig{Enabled: false})
	defer resetNotify()

	_, err := Dispatch(context.Background(), sampleEvent(), "")
	if err == nil || !strings.Contains(err.Error(), "notify is disabled") {
		t.Fatalf("want disabled error, got %v", err)
	}
}

func TestSendNilSafe(t *testing.T) {
	Send(nil)
	SendImmediate(nil)
}

// TestWebhookDelivered 覆盖三个群机器人渠道的成功路径：断言投递成功，
// 且请求确实到达测试服务器。
func TestWebhookDelivered(t *testing.T) {
	cases := []struct {
		name        string
		channel     string
		successBody string
		wantContent string // 正文中应包含的文本
	}{
		{"wechat", ChannelWeChat, `{"errcode":0,"errmsg":"ok"}`, "已切换到新主库"},
		{"dingtalk", ChannelDingTalk, `{"errcode":0,"errmsg":"ok"}`, "已切换到新主库"},
		{"feishu", ChannelFeishu, `{"code":0,"msg":"ok","StatusCode":0}`, "已切换到新主库"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotBody []byte
			var gotQuery string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotBody, _ = io.ReadAll(r.Body)
				gotQuery = r.URL.RawQuery
				_, _ = io.WriteString(w, tc.successBody)
			}))
			defer srv.Close()

			cfg := &config.NotifyConfig{Enabled: true}
			setWebhook(cfg, tc.channel, config.WebhookNotifyConfig{
				Enabled: true,
				URL:     srv.URL,
				Secret:  "SEC-test", // 启用加签，验证钉钉 query / 飞书 body 携带签名
			})
			config.SetNotifyOverride(cfg)
			defer resetNotify()

			r := dispatchOne(t, sampleEvent(), tc.channel)
			if !r.Success {
				t.Fatalf("expected success, got error=%q", r.Error)
			}
			if !strings.Contains(string(gotBody), tc.wantContent) {
				t.Fatalf("webhook body missing %q: %s", tc.wantContent, gotBody)
			}

			switch tc.channel {
			case ChannelDingTalk:
				// 钉钉加签参数走 URL query。
				if !strings.Contains(gotQuery, "timestamp=") || !strings.Contains(gotQuery, "sign=") {
					t.Fatalf("dingtalk signed query missing: %q", gotQuery)
				}
			case ChannelFeishu:
				var m map[string]interface{}
				if err := json.Unmarshal(gotBody, &m); err != nil {
					t.Fatalf("feishu body not json: %v", err)
				}
				if m["sign"] == "" || m["timestamp"] == "" {
					t.Fatalf("feishu signed fields missing: %s", gotBody)
				}
			}
		})
	}
}

// TestWebhookProviderError 验证机器人返回业务错误码时投递被判为失败。
func TestWebhookProviderError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"errcode":93000,"errmsg":"invalid webhook"}`)
	}))
	defer srv.Close()

	cfg := &config.NotifyConfig{Enabled: true}
	setWebhook(cfg, ChannelWeChat, config.WebhookNotifyConfig{Enabled: true, URL: srv.URL})
	config.SetNotifyOverride(cfg)
	defer resetNotify()

	r := dispatchOne(t, sampleEvent(), ChannelWeChat)
	if r.Success || !strings.Contains(r.Error, "errcode=93000") {
		t.Fatalf("want provider errcode failure, got success=%v error=%q", r.Success, r.Error)
	}
}

// TestWebhookHTTPError 验证上游返回 5xx 时投递失败。
func TestWebhookHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := &config.NotifyConfig{Enabled: true}
	setWebhook(cfg, ChannelDingTalk, config.WebhookNotifyConfig{Enabled: true, URL: srv.URL})
	config.SetNotifyOverride(cfg)
	defer resetNotify()

	r := dispatchOne(t, sampleEvent(), ChannelDingTalk)
	if r.Success || !strings.Contains(r.Error, "HTTP 500") {
		t.Fatalf("want HTTP 500 failure, got success=%v error=%q", r.Success, r.Error)
	}
}

// TestEmailIncomplete 验证邮件配置缺失时返回明确错误（不发起真实连接）。
func TestEmailIncomplete(t *testing.T) {
	cfg := &config.NotifyConfig{
		Enabled: true,
		Email:   config.EmailNotifyConfig{Enabled: true /* 缺 host/from/to */},
	}
	config.SetNotifyOverride(cfg)
	defer resetNotify()

	r := dispatchOne(t, sampleEvent(), ChannelEmail)
	if r.Success || !strings.Contains(r.Error, "邮件配置不完整") {
		t.Fatalf("want incomplete-config error, got success=%v error=%q", r.Success, r.Error)
	}
}

// TestThrottleAllow 验证相同事件在节流窗口内只放行一次。
func TestThrottleAllow(t *testing.T) {
	defer resetNotify()

	ev := sampleEvent()
	if !throttleAllow(ev) {
		t.Fatal("first identical event should be allowed")
	}
	if throttleAllow(ev) {
		t.Fatal("second identical event should be throttled")
	}

	// 不同标题（不同事件）不受影响。
	other := sampleEvent()
	other.Title = "Failover开始：cluster1"
	if !throttleAllow(other) {
		t.Fatal("a different event should still be allowed")
	}
}

// TestEffectiveViewMasksSecrets 验证返回给浏览器的视图不泄露口令与 webhook token。
func TestEffectiveViewMasksSecrets(t *testing.T) {
	cfg := &config.NotifyConfig{
		Enabled: true,
		Email: config.EmailNotifyConfig{
			Enabled: true, Host: "smtp.example.com", From: "a@example.com",
			To: []string{"b@example.com"}, Password: "super-secret",
		},
	}
	setWebhook(cfg, ChannelWeChat, config.WebhookNotifyConfig{
		Enabled: true,
		URL:     "https://qy.example.com/cgi-bin/webhook/send?key=SECRET-KEY",
	})
	config.SetNotifyOverride(cfg)
	defer resetNotify()

	view := EffectiveView()
	email := view["email"].(map[string]interface{})
	if email["has_password"] != true {
		t.Fatal("has_password should be true without revealing the password")
	}
	flat, _ := json.Marshal(view)
	if strings.Contains(string(flat), "super-secret") || strings.Contains(string(flat), "SECRET-KEY") {
		t.Fatalf("view leaked a secret: %s", flat)
	}
	wechat := view["wechat"].(map[string]interface{})
	if !strings.Contains(wechat["url"].(string), "?***") {
		t.Fatalf("webhook query should be masked, got %v", wechat["url"])
	}
}

// setWebhook 按渠道写入对应 webhook 配置，保持测试简洁。
func setWebhook(cfg *config.NotifyConfig, channel string, c config.WebhookNotifyConfig) {
	switch channel {
	case ChannelWeChat:
		cfg.WeChat = c
	case ChannelDingTalk:
		cfg.DingTalk = c
	case ChannelFeishu:
		cfg.Feishu = c
	}
}

// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

// Package notify 将告警事件投递到面向运维人员的渠道：基于 SMTP 的邮件，以及
// 企业微信 / 钉钉 / 飞书的群机器人 webhook。
//
// 它在每次投递时都会读取当前生效的通知配置（config.NotifyConfig），因此无需
// 重建任何东西即可支持配置热加载。异步入口 Send 可在故障切换的热路径上安全
// 调用；同步的 Dispatch 则用于控制台的“发送测试”端点。
package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/config"
	log "github.com/yunnnnn1/go-helix/pkg/logging"
	"github.com/yunnnnn1/go-helix/pkg/metrics"
)

// 渠道标识常量。
const (
	ChannelEmail    = "email"
	ChannelWeChat   = "wechat"
	ChannelDingTalk = "dingtalk"
	ChannelFeishu   = "feishu"
)

// Severity 表示事件的紧急程度。
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Event 表示一条待投递的告警。
type Event struct {
	Title    string
	Message  string
	Cluster  string
	Code     string
	Severity Severity
	Time     time.Time
}

// ChannelResult 报告向单个渠道的投递情况。
type ChannelResult struct {
	Channel string `json:"channel"`
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// SMTP/HTTP 超时时间，用于约束单次投递。
const (
	smtpDialTimeout = 10 * time.Second
	smtpOverall     = 15 * time.Second
	httpTimeout     = 10 * time.Second
)

// notifyCounter 按渠道和结果统计已投递的事件数量。
var notifyCounter = metrics.Counter(
	"helix_notify_events_total",
	"Number of notification events delivered by channel and result",
	"channel", "result",
)

var httpClient = &http.Client{Timeout: httpTimeout}

// ---------------------------------------------------------------------------
// 配置访问
// ---------------------------------------------------------------------------

func currentNotify() config.NotifyConfig {
	// 生效的配置优先采用 Raft 分发的覆盖项（控制台编辑），而非配置文件中的
	// notify 段落。
	return config.EffectiveNotify()
}

// EffectiveView 返回当前生效的通知配置，并移除其中的密码和 webhook token，
// 可安全返回给浏览器。
func EffectiveView() map[string]interface{} {
	c := currentNotify()
	return map[string]interface{}{
		"enabled": c.Enabled,
		"email": map[string]interface{}{
			"enabled":      c.Email.Enabled,
			"host":         c.Email.Host,
			"port":         c.Email.Port,
			"username":     c.Email.Username,
			"from":         c.Email.From,
			"to":           c.Email.To,
			"use_tls":      c.Email.UseTLS,
			"has_password": c.Email.Password != "",
		},
		"wechat": map[string]interface{}{
			"enabled":    c.WeChat.Enabled,
			"url":        maskWebhookURL(c.WeChat.URL),
			"has_secret": c.WeChat.Secret != "",
		},
		"dingtalk": map[string]interface{}{
			"enabled":    c.DingTalk.Enabled,
			"url":        maskWebhookURL(c.DingTalk.URL),
			"has_secret": c.DingTalk.Secret != "",
		},
		"feishu": map[string]interface{}{
			"enabled":    c.Feishu.Enabled,
			"url":        maskWebhookURL(c.Feishu.URL),
			"has_secret": c.Feishu.Secret != "",
		},
	}
}

// maskWebhookURL 保留 scheme/host/path，但隐藏查询字符串，因为其中携带了
// 机器人 token（例如企业微信的 key）。
func maskWebhookURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	out := u.Scheme + "://" + u.Host + u.Path
	if u.RawQuery != "" {
		out += "?***"
	}
	return out
}

// ---------------------------------------------------------------------------
// 异步投递（热路径），带节流和有界队列
// ---------------------------------------------------------------------------

// eventThrottleInterval 是经由异步路径发送的两个相同事件之间的最小间隔。
// 每个 tick 重复出现的告警（例如某个从库长期处于过期状态）会被合并为每个间隔
// 一条通知，而不是造成消息轰炸。
const eventThrottleInterval = 10 * time.Minute

var (
	eventCh = make(chan *Event, 256)

	throttleMu sync.Mutex
	lastSent   = map[string]time.Time{}
)

func init() {
	go dispatchLoop()
}

// dispatchLoop 是消费事件队列的唯一 worker。单个 worker 可使并发的外部请求
// （SMTP/webhook）保持有界且有序。
func dispatchLoop() {
	for ev := range eventCh {
		ctx, cancel := context.WithTimeout(context.Background(), smtpOverall)
		if _, err := Dispatch(ctx, ev, ""); err != nil {
			log.Printf("[notify] dispatch event %q failed: %v", ev.Title, err)
		}
		cancel()
	}
}

// Send 将事件入队，进行异步、尽力而为的投递。它绝不阻塞：当总开关关闭、
// 事件被节流或队列已满时，事件会被丢弃，而不是拖慢故障切换路径。
func Send(ev *Event) {
	if ev == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	if !currentNotify().Enabled {
		return
	}
	if !throttleAllow(ev) {
		return
	}
	enqueue(ev)
}

// SendImmediate 将事件入队进行异步投递，但不应用重复节流。用于离散的生命周期
// 事件（一次切换开始或结束），这类事件必须始终上报。总开关的启用限制仍然
// 生效，并且它同样绝不阻塞。
func SendImmediate(ev *Event) {
	if ev == nil {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	if !currentNotify().Enabled {
		return
	}
	enqueue(ev)
}

// enqueue 将 ev 放入队列，队列已满时将其丢弃（绝不阻塞）。
func enqueue(ev *Event) {
	select {
	case eventCh <- ev:
	default:
		log.Printf("[notify] event queue full, dropping event: %s", ev.Title)
	}
}

// throttleAllow 报告当前是否可以投递一个相同的事件。
func throttleAllow(ev *Event) bool {
	key := strings.Join([]string{ev.Cluster, ev.Code, ev.Title}, "|")
	throttleMu.Lock()
	defer throttleMu.Unlock()
	if t, ok := lastSent[key]; ok && time.Since(t) < eventThrottleInterval {
		return false
	}
	lastSent[key] = time.Now()
	return true
}

// ---------------------------------------------------------------------------
// 同步分发
// ---------------------------------------------------------------------------

// Dispatch 将 ev 同步投递到每个已启用的渠道；当 onlyChannel 非空时则只投递到
// 该渠道，并为每个被评估的渠道返回一个结果。
func Dispatch(ctx context.Context, ev *Event, onlyChannel string) ([]ChannelResult, error) {
	cfg := currentNotify()
	if !cfg.Enabled {
		return nil, fmt.Errorf("notify is disabled (notify.enabled=false)")
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}

	body := renderText(ev)
	results := make([]ChannelResult, 0, 4)

	run := func(channel, name string, enabled bool, fn func() error) {
		if onlyChannel != "" && onlyChannel != channel {
			return // 不是当前测试的渠道
		}
		r := ChannelResult{Channel: channel, Name: name, Enabled: enabled}
		switch {
		case !enabled:
			r.Error = "该渠道未启用"
		case fn == nil:
			r.Error = "渠道未配置"
		default:
			if err := fn(); err != nil {
				r.Error = err.Error()
				log.Printf("[notify] channel=%s delivery failed: %v", channel, err)
			} else {
				r.Success = true
				log.Printf("[notify] channel=%s delivered event=%q cluster=%s", channel, ev.Title, ev.Cluster)
			}
		}
		// 只统计实际尝试的投递（已启用且存在处理函数），并标记成功/失败。
		if enabled && fn != nil {
			res := "failed"
			if r.Success {
				res = "success"
			}
			notifyCounter.Inc(channel, res)
		}
		results = append(results, r)
	}

	run(ChannelEmail, "邮件", cfg.Email.Enabled, func() error {
		return sendEmail(cfg.Email, ev.Title, body)
	})
	run(ChannelWeChat, "企业微信", cfg.WeChat.Enabled, func() error {
		return sendWeChat(ctx, cfg.WeChat, body)
	})
	run(ChannelDingTalk, "钉钉", cfg.DingTalk.Enabled, func() error {
		return sendDingTalk(ctx, cfg.DingTalk, body)
	})
	run(ChannelFeishu, "飞书", cfg.Feishu.Enabled, func() error {
		return sendFeishu(ctx, cfg.Feishu, body)
	})

	return results, nil
}

// renderText 构建邮件和机器人消息共用的纯文本正文。
func renderText(ev *Event) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[go-helix 告警]\n")
	fmt.Fprintf(&b, "级别：%s\n", severityCN(ev.Severity))
	if ev.Cluster != "" {
		fmt.Fprintf(&b, "集群：%s\n", ev.Cluster)
	}
	if ev.Code != "" {
		fmt.Fprintf(&b, "场景：%s\n", ev.Code)
	}
	fmt.Fprintf(&b, "时间：%s\n", ev.Time.Format("2006-01-02 15:04:05"))
	if ev.Message != "" {
		fmt.Fprintf(&b, "\n%s\n", ev.Message)
	}
	return b.String()
}

func severityCN(s Severity) string {
	switch s {
	case SeverityCritical:
		return "严重"
	case SeverityWarning:
		return "警告"
	default:
		return "信息"
	}
}

// ---------------------------------------------------------------------------
// 邮件（SMTP）
// ---------------------------------------------------------------------------

func sendEmail(c config.EmailNotifyConfig, subject, body string) error {
	if strings.TrimSpace(c.Host) == "" || strings.TrimSpace(c.From) == "" || len(c.To) == 0 {
		return fmt.Errorf("邮件配置不完整（需要 host/from/to）")
	}
	port := c.Port
	if port == 0 {
		if c.UseTLS {
			port = 465
		} else {
			port = 25
		}
	}

	client, err := dialSMTPClient(c, port)
	if err != nil {
		return fmt.Errorf("smtp 连接: %w", err)
	}
	defer client.Close()

	if c.Username != "" {
		auth := smtp.PlainAuth("", c.Username, c.Password, c.Host)
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("smtp 认证: %w", err)
		}
	}
	if err := client.Mail(c.From); err != nil {
		return fmt.Errorf("smtp MAIL FROM: %w", err)
	}
	for _, to := range c.To {
		if err := client.Rcpt(to); err != nil {
			return fmt.Errorf("smtp RCPT TO %s: %w", to, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp DATA: %w", err)
	}
	if _, err := w.Write(buildEmailMessage(c.From, c.To, subject, body)); err != nil {
		return fmt.Errorf("smtp 写正文: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp 结束正文: %w", err)
	}
	_ = client.Quit()
	return nil
}

// dialSMTPClient 建立一个 SMTP 客户端。隐式 TLS（UseTLS/465）会立即对连接
// 进行包装；否则客户端先以明文启动，并在服务器提供 STARTTLS 时用其升级连接。
// 整个握手过程受同一个截止时间约束。
func dialSMTPClient(c config.EmailNotifyConfig, port int) (*smtp.Client, error) {
	addr := net.JoinHostPort(c.Host, strconv.Itoa(port))
	deadline := time.Now().Add(smtpOverall)

	conn, err := net.DialTimeout("tcp", addr, smtpDialTimeout)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(deadline)

	if c.UseTLS || port == 465 {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: c.Host})
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return nil, err
		}
		client, err := smtp.NewClient(tlsConn, c.Host)
		if err != nil {
			conn.Close()
			return nil, err
		}
		return client, nil
	}

	client, err := smtp.NewClient(conn, c.Host)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if ok, _ := client.Extension("STARTTLS"); ok {
		if err := client.StartTLS(&tls.Config{ServerName: c.Host}); err != nil {
			client.Close()
			return nil, err
		}
	}
	return client, nil
}

// buildEmailMessage 渲染一条带有 UTF-8 头部/正文的 RFC 822 消息。
func buildEmailMessage(from string, to []string, subject, body string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(to, ", "))
	enc := mime.QEncoding.Encode("utf-8", subject)
	fmt.Fprintf(&b, "Subject: %s\r\n", enc)
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	b.WriteString("\r\n")
	b.WriteString(body)
	return []byte(b.String())
}

// ---------------------------------------------------------------------------
// 群机器人 webhook
// ---------------------------------------------------------------------------

// postRobot 将 payload 以 JSON 形式 POST 到 rawURL，并检查各服务商特定响应中
// 内嵌的错误码。
func postRobot(ctx context.Context, rawURL string, payload interface{}, provider string) error {
	if strings.TrimSpace(rawURL) == "" {
		return fmt.Errorf("webhook url 为空")
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("编码请求: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("构造请求: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("发送请求: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return robotResponseError(provider, respBody)
}

// robotResponseError 解析机器人的 JSON 响应体，并映射其中的错误字段。
func robotResponseError(provider string, raw []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil // 非 JSON 的 2xx 响应：视为已接受
	}
	switch provider {
	case ChannelWeChat, ChannelDingTalk:
		if code, ok := m["errcode"].(float64); ok && code != 0 {
			return fmt.Errorf("errcode=%v errmsg=%v", code, m["errmsg"])
		}
	case ChannelFeishu:
		if code, ok := m["code"].(float64); ok && code != 0 {
			return fmt.Errorf("code=%v msg=%v", code, m["msg"])
		}
		if code, ok := m["StatusCode"].(float64); ok && code != 0 {
			return fmt.Errorf("StatusCode=%v msg=%v", code, m["StatusMessage"])
		}
	}
	return nil
}

// sendWeChat 向企业微信群机器人发送一条文本消息。
func sendWeChat(ctx context.Context, c config.WebhookNotifyConfig, text string) error {
	payload := map[string]interface{}{
		"msgtype": "text",
		"text":    map[string]string{"content": text},
	}
	return postRobot(ctx, c.URL, payload, ChannelWeChat)
}

// sendDingTalk 向钉钉机器人发送一条文本消息，并在配置了签名密钥时附加 HMAC
// 时间戳/sign 查询参数。
func sendDingTalk(ctx context.Context, c config.WebhookNotifyConfig, text string) error {
	rawURL := c.URL
	if c.Secret != "" {
		ts := time.Now().UnixMilli()
		stringToSign := strconv.FormatInt(ts, 10) + "\n" + c.Secret
		mac := hmac.New(sha256.New, []byte(stringToSign))
		sign := base64.StdEncoding.EncodeToString(mac.Sum(nil))

		u, err := url.Parse(rawURL)
		if err != nil {
			return fmt.Errorf("解析钉钉 webhook: %w", err)
		}
		q := u.Query()
		q.Set("timestamp", strconv.FormatInt(ts, 10))
		q.Set("sign", sign)
		u.RawQuery = q.Encode()
		rawURL = u.String()
	}

	payload := map[string]interface{}{
		"msgtype": "text",
		"text":    map[string]string{"content": text},
	}
	return postRobot(ctx, rawURL, payload, ChannelDingTalk)
}

// sendFeishu 向飞书机器人发送一条文本消息，并在配置了签名密钥时附加时间戳和
// HMAC-SHA256 签名。
func sendFeishu(ctx context.Context, c config.WebhookNotifyConfig, text string) error {
	payload := map[string]interface{}{
		"msg_type": "text",
		"content":  map[string]string{"text": text},
	}
	if c.Secret != "" {
		ts := time.Now().Unix()
		tsStr := strconv.FormatInt(ts, 10)
		stringToSign := tsStr + "\n" + c.Secret
		mac := hmac.New(sha256.New, []byte(stringToSign))
		sign := base64.StdEncoding.EncodeToString(mac.Sum(nil))
		payload["timestamp"] = tsStr
		payload["sign"] = sign
	}
	return postRobot(ctx, c.URL, payload, ChannelFeishu)
}

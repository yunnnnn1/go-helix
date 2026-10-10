// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package mysql

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/yunnnnn1/go-helix/pkg/db"
)

func TestMysqlDialect_Registration(t *testing.T) {
	d, err := db.GetDialect("mysql")
	if err != nil {
		t.Fatalf("mysql dialect should be registered: %v", err)
	}
	if d.Type() != "mysql" {
		t.Errorf("Type = %q, want mysql", d.Type())
	}
	if d.SQLDriverName() != "mysql" {
		t.Errorf("SQLDriverName = %q, want mysql", d.SQLDriverName())
	}
}

func TestMysqlDialect_BuildDSN(t *testing.T) {
	d, _ := db.GetDialect("mysql")

	got := d.BuildDSN(db.ConnSpec{
		Host: "10.0.0.1", Port: 3306, User: "u", Password: "p", DBName: "dc", Timeout: "2s",
	})
	for _, want := range []string{
		"u:p@tcp(10.0.0.1:3306)/dc",
		"charset=utf8mb4",
		"parseTime=true",
		"loc=Local",
		"timeout=2s",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("DSN %q missing %q", got, want)
		}
	}

	// 超时为空时回退到 5s。
	if fallback := d.BuildDSN(db.ConnSpec{Host: "h", Port: 1, User: "u"}); !strings.Contains(fallback, "timeout=5s") {
		t.Errorf("expected default timeout=5s in %q", fallback)
	}
}

func TestMysqlDialect_DeriveAgentPort(t *testing.T) {
	d, _ := db.GetDialect("mysql")
	cases := []struct {
		dbPort, want int32
	}{
		{3306, 23306},
		{3307, 23307},
		{1, 20001},
	}
	for _, c := range cases {
		if got := d.DeriveAgentPort(c.dbPort); got != c.want {
			t.Errorf("DeriveAgentPort(%d) = %d, want %d", c.dbPort, got, c.want)
		}
	}
}

// startGreetingServer 接受一个连接，写入 greeting（nil 表示什么都不写并保持
// 连接打开），并返回其可拨号的地址。
func startGreetingServer(t *testing.T, greeting []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if greeting != nil {
			_, _ = conn.Write(greeting)
		}
		// 短暂保持连接，使无问候报文的场景下客户端的读取超时能够触发，而不是
		// 立即收到 EOF。
		time.Sleep(300 * time.Millisecond)
	}()
	return ln.Addr().String()
}

func TestMysqlDialect_Probe(t *testing.T) {
	d, _ := db.GetDialect("mysql")
	ctx := context.Background()

	// 字节布局：[0..2] 包长度，[3] 序列号，[4] 协议版本
	validGreeting := []byte{0x01, 0x00, 0x00, 0x00, 0x0a}
	wrongVersion := []byte{0x01, 0x00, 0x00, 0x00, 0x09}
	wrongSequence := []byte{0x01, 0x00, 0x00, 0x01, 0x0a}

	if err := d.Probe(ctx, startGreetingServer(t, validGreeting), time.Second); err != nil {
		t.Errorf("valid MySQL greeting should probe ok: %v", err)
	}
	if err := d.Probe(ctx, startGreetingServer(t, wrongVersion), time.Second); err == nil {
		t.Error("protocol version 9 should fail the probe")
	}
	if err := d.Probe(ctx, startGreetingServer(t, wrongSequence), time.Second); err == nil {
		t.Error("non-zero sequence id should fail the probe")
	}
	if err := d.Probe(ctx, startGreetingServer(t, nil), 100*time.Millisecond); err == nil {
		t.Error("no greeting within timeout should fail the probe")
	}

	// 一个已关闭的 listener 可保证产生拨号错误。
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	refusedAddr := ln.Addr().String()
	ln.Close()
	if err := d.Probe(ctx, refusedAddr, time.Second); err == nil {
		t.Error("connection refused should fail the probe")
	}
}

// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseHookResult(t *testing.T) {
	stdout := "some human log line\n" +
		`{"command":"start","success":true,"cluster":"cluster1","current_master":"10.0.0.2:3306","message":"ok","timestamp":"2026-10-08T08:30:00+08:00"}`
	r, err := parseHookResult(stdout)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !r.Success || r.CurrentMaster != "10.0.0.2:3306" || r.Command != "start" {
		t.Fatalf("unexpected result: %+v", r)
	}

	for _, bad := range []string{
		"",
		"no json here",
		"not json\n" + `{"success":true,`,
		`{"success":"yes"}`,
	} {
		if _, err := parseHookResult(bad); err == nil {
			t.Errorf("expected error for stdout %q", bad)
		}
	}
}

func TestValidateHookPath(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "hooks")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	hook := filepath.Join(sub, "endpoint_hook")
	got, err := validateHookPath([]string{root}, hook)
	if err != nil {
		t.Fatalf("valid path: %v", err)
	}
	if got != hook {
		t.Errorf("got %q want %q", got, hook)
	}

	// 在根目录之外。
	outside := filepath.Join(t.TempDir(), "x")
	if _, err := validateHookPath([]string{root}, outside); err == nil {
		t.Error("path outside roots must be rejected")
	}

	// 未配置任何根目录。
	if _, err := validateHookPath(nil, hook); err == nil {
		t.Error("missing roots must be rejected")
	}
}

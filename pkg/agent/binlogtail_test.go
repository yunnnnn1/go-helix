// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package agent

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestReadBinlogIndex(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "mysql-bin.index")

	// 混合相对条目、绝对条目以及空行。
	abs := filepath.Join(dir, "mysql-bin.000003")
	content := "mysql-bin.000001\n\nmysql-bin.000002\n" + abs + "\n"
	if err := os.WriteFile(indexPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write index: %v", err)
	}

	files, err := readBinlogIndex(indexPath)
	if err != nil {
		t.Fatalf("readBinlogIndex: %v", err)
	}
	want := []string{
		filepath.Join(dir, "mysql-bin.000001"),
		filepath.Join(dir, "mysql-bin.000002"),
		abs,
	}
	if len(files) != len(want) {
		t.Fatalf("got %d files, want %d (%v)", len(files), len(want), files)
	}
	for i := range want {
		if files[i] != want[i] {
			t.Errorf("files[%d] = %q, want %q", i, files[i], want[i])
		}
	}
}

func TestReadBinlogIndex_EmptyIndex(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "mysql-bin.index")
	if err := os.WriteFile(indexPath, []byte("\n  \n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := readBinlogIndex(indexPath); err == nil {
		t.Fatal("expected error for an index listing no files")
	}
}

func TestIndexOfBinlog(t *testing.T) {
	files := []string{
		filepath.Join("/data", "mysql-bin.000001"),
		filepath.Join("/data", "mysql-bin.000002"),
	}
	if got := indexOfBinlog(files, "mysql-bin.000002"); got != 1 {
		t.Errorf("index = %d, want 1", got)
	}
	if got := indexOfBinlog(files, "mysql-bin.000099"); got != -1 {
		t.Errorf("index = %d, want -1", got)
	}
}

func TestFDEEndOffset(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "mysql-bin.000001")

	// 4 字节 magic + 19 字节事件头；event_size=100，位于事件头偏移量 9。
	data := make([]byte, 4+19)
	copy(data[:4], []byte{0xfe, 0x62, 0x69, 0x6e})
	binary.LittleEndian.PutUint32(data[4+9:4+13], 100)
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatalf("write fake binlog: %v", err)
	}

	end, err := fdeEndOffset(file)
	if err != nil {
		t.Fatalf("fdeEndOffset: %v", err)
	}
	// end = magic(4) + event_size(100) = 104
	if end != 104 {
		t.Errorf("fdeEnd = %d, want 104", end)
	}
}

func TestFDEEndOffset_BadHeader(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "mysql-bin.000001")
	// 长度过短，无法容纳完整的事件头。
	if err := os.WriteFile(file, []byte{0xfe, 0x62, 0x69, 0x6e, 0x00}, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := fdeEndOffset(file); err == nil {
		t.Fatal("expected error for truncated FDE header")
	}
}

func TestInsideAnyRoot(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "binlog")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if !insideAnyRoot(sub, []string{root}) {
		t.Error("sub dir must be inside root")
	}
	if !insideAnyRoot(root, []string{root}) {
		t.Error("root must match itself")
	}
	if insideAnyRoot(t.TempDir(), []string{root}) {
		t.Error("unrelated dir must not be treated as inside root")
	}
}

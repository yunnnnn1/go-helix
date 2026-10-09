package raft

import (
	"reflect"
	"testing"
)

func TestLogSink_SplitsCompleteLines(t *testing.T) {
	var got []string
	sink := newLogSink("node1", "127.0.0.1:10001", func(line string) { got = append(got, line) })

	n, err := sink.Write([]byte("line one\nline two\n"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len("line one\nline two\n") {
		t.Errorf("reported n = %d, want %d", n, len("line one\nline two\n"))
	}

	want := []string{"line one", "line two"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("emitted lines = %v, want %v", got, want)
	}
	if tail := sink.bufferedTail(); tail != "" {
		t.Errorf("expected empty tail, got %q", tail)
	}
}

// TestLogSink_RetainsPartialLine 验证未以换行符结尾的尾部内容会被保留，直到
// 后续某次 Write 将其补全，并验证单次 Write 可以携带多行。
func TestLogSink_RetainsPartialLine(t *testing.T) {
	var got []string
	sink := newLogSink("node1", "127.0.0.1:10001", func(line string) { got = append(got, line) })

	// 第一次只写入部分内容：此时不应输出任何行。
	if _, err := sink.Write([]byte("star")); err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("no line should be emitted before newline, got %v", got)
	}
	if sink.bufferedTail() != "star" {
		t.Errorf("tail = %q, want %q", sink.bufferedTail(), "star")
	}

	// 补全前一行，同时再带上一个完整行和一段新的部分内容。
	if _, err := sink.Write([]byte("t line\nsecond\nthi")); err != nil {
		t.Fatal(err)
	}
	want := []string{"start line", "second"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("emitted lines = %v, want %v", got, want)
	}
	if sink.bufferedTail() != "thi" {
		t.Errorf("tail = %q, want %q", sink.bufferedTail(), "thi")
	}
}

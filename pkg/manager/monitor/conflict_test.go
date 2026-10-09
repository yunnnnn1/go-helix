package monitor

import (
	"testing"
	"time"
)

func newConflictMonitor() *Monitor {
	return &Monitor{
		conflictCounts: make(map[string]*conflictState),
	}
}

func TestConflictCount_RecordAndReset(t *testing.T) {
	m := newConflictMonitor()
	host := "10.0.0.1"

	if got := m.conflictCount(host); got != 0 {
		t.Fatalf("initial count = %d, want 0", got)
	}

	for i := 1; i <= 3; i++ {
		m.recordConflict(host)
		if got := m.conflictCount(host); got != i {
			t.Errorf("after %d record(s) count = %d, want %d", i, got, i)
		}
	}

	m.resetConflict(host)
	if got := m.conflictCount(host); got != 0 {
		t.Errorf("after reset count = %d, want 0", got)
	}
}

// TestConflictCount_DecaysAfterWindow 验证达到或超过跳过上限的过期记录
// 会按零上报，从而使该主机重新被调查，而不是被永久跳过。
func TestConflictCount_DecaysAfterWindow(t *testing.T) {
	m := newConflictMonitor()
	host := "10.0.0.1"

	for i := 0; i < 3; i++ {
		m.recordConflict(host)
	}
	if got := m.conflictCount(host); got != 3 {
		t.Fatalf("setup count = %d, want 3", got)
	}

	// 将记录的时间老化到衰减窗口之外；下一次读取必须将其清除。
	m.mu.Lock()
	m.conflictCounts[host].lastAt = time.Now().Add(-conflictDecayAfter - time.Minute)
	m.mu.Unlock()

	if got := m.conflictCount(host); got != 0 {
		t.Errorf("decayed count = %d, want 0", got)
	}
	m.mu.Lock()
	_, remains := m.conflictCounts[host]
	m.mu.Unlock()
	if remains {
		t.Error("decayed record should have been purged")
	}
}

// TestRecordConflict_RestartsAfterDecay 验证衰减后再记录时，会从 1
// 开始一串新的计数，而不是把过期计数继续累加到 4。
func TestRecordConflict_RestartsAfterDecay(t *testing.T) {
	m := newConflictMonitor()
	host := "10.0.0.1"

	m.recordConflict(host)
	m.recordConflict(host)
	m.mu.Lock()
	m.conflictCounts[host].lastAt = time.Now().Add(-conflictDecayAfter - time.Minute)
	m.mu.Unlock()

	m.recordConflict(host)
	if got := m.conflictCount(host); got != 1 {
		t.Errorf("count after decayed record = %d, want 1", got)
	}
}

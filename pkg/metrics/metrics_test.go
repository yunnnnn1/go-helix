package metrics

import (
	"strings"
	"testing"
)

func TestCounterExposition(t *testing.T) {
	r := New()
	c := r.Counter("helix_failover_total", "failovers", "result")
	c.Inc("success")
	c.Add(2, "failed")
	c.Inc("success")

	var b strings.Builder
	if err := r.Gather(&b); err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := b.String()

	for _, want := range []string{
		"# HELP helix_failover_total failovers",
		"# TYPE helix_failover_total counter",
		`helix_failover_total{result="success"} 2`,
		`helix_failover_total{result="failed"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\nfull:\n%s", want, out)
		}
	}
}

func TestGaugeSet(t *testing.T) {
	r := New()
	g := r.Gauge("helix_is_leader", "leader")
	g.Set(1)

	var b strings.Builder
	if err := r.Gather(&b); err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := b.String()
	if !strings.Contains(out, "# TYPE helix_is_leader gauge") {
		t.Errorf("missing gauge type line:\n%s", out)
	}
	if !strings.Contains(out, "helix_is_leader 1") {
		t.Errorf("missing gauge value:\n%s", out)
	}
}

func TestLabelValueEscaping(t *testing.T) {
	r := New()
	c := r.Counter("helix_x", "x", "channel")
	c.Inc(`a"b\c`)

	var b strings.Builder
	if err := r.Gather(&b); err != nil {
		t.Fatalf("gather: %v", err)
	}
	if !strings.Contains(b.String(), `{channel="a\"b\\c"}`) {
		t.Errorf("value not escaped: %q", b.String())
	}
}

func TestIdempotentRegistration(t *testing.T) {
	r := New()
	a := r.Counter("helix_same", "h")
	b := r.Counter("helix_same", "h")
	// 即使生成了新的 Handle，被包装的指标也必须是同一个。
	if a.m != b.m {
		t.Error("re-registering same name should reuse the underlying metric")
	}
}

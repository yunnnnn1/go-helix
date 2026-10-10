package http

import (
	"github.com/yunnnnn1/go-helix/pkg/config"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecoveryHandler(t *testing.T) {
	h := recoveryHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500 after panic, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "internal server error\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestSetTLSValidation(t *testing.T) {
	s := NewServer(nil, nil)
	// 未启用：无操作。
	if err := s.SetTLS(config.TLSConfig{}); err != nil {
		t.Fatalf("disabled tls should be fine: %v", err)
	}
	// 启用但缺字段：报错。
	if err := s.SetTLS(config.TLSConfig{Enabled: true}); err == nil {
		t.Fatal("enabled tls with missing files must error")
	}
}

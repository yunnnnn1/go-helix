package http

import (
	"net/http/httptest"
	"testing"
)

func TestHandleHealth(t *testing.T) {
	s := NewServer(nil, nil)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	s.handleHealth(w, req)

	if w.Code != 200 {
		t.Fatalf("health: expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("health: expected json content-type, got %q", ct)
	}
	if body := w.Body.String(); body == "" {
		t.Fatal("health: expected a JSON body")
	}
}

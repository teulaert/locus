package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// LOC-84: the Go runtime profiles are on the API, behind the token and for
// loopback clients only.
func TestPprofNeedsTokenAndLoopback(t *testing.T) {
	a := testApp(t, "secret")
	h, err := Handler(a)
	if err != nil {
		t.Fatal(err)
	}
	get := func(path, remote, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.RemoteAddr = remote
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// A heap profile in the binary format go tool pprof reads (gzipped).
	rec := get("/debug/pprof/heap", "127.0.0.1:50000", "secret")
	if rec.Code != http.StatusOK || !bytes.HasPrefix(rec.Body.Bytes(), []byte{0x1f, 0x8b}) {
		t.Fatalf("heap profile: %d, %d bytes, starts %q", rec.Code, rec.Body.Len(), rec.Body.Bytes()[:min(8, rec.Body.Len())])
	}
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine?debug=1", "/debug/pprof/allocs?debug=1", "/debug/pprof/cmdline"} {
		if rec := get(path, "[::1]:50000", "secret"); rec.Code != http.StatusOK || rec.Body.Len() == 0 {
			t.Errorf("GET %s from ::1: %d %.200s", path, rec.Code, rec.Body)
		}
	}
	if rec := get("/debug/pprof/", "127.0.0.1:50000", "secret"); !strings.Contains(rec.Body.String(), "heap") {
		t.Errorf("index does not list the heap profile: %.300s", rec.Body)
	}

	if rec := get("/debug/pprof/heap", "127.0.0.1:50000", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("heap profile without a token: %d; want 401", rec.Code)
	}
	if rec := get("/debug/pprof/heap", "127.0.0.1:50000", "wrong"); rec.Code != http.StatusUnauthorized {
		t.Errorf("heap profile with a wrong token: %d; want 401", rec.Code)
	}
	for _, remote := range []string{"10.42.0.2:50000", "[fd00::2]:50000", "192.168.1.5:1"} {
		if rec := get("/debug/pprof/heap", remote, "secret"); rec.Code != http.StatusForbidden {
			t.Errorf("heap profile from %s: %d; want 403", remote, rec.Code)
		}
	}
}

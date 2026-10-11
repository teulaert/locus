package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
)

func testApp(t *testing.T, token string) *app.App {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "docs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = config.PathRoots(root)
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.API.Token = token
	a, err := app.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// An empty expected token compares equal to a request with no Authorization
// header, so the middleware must reject every request rather than accept all.
func TestAuthWithEmptyTokenRejectsEveryRequest(t *testing.T) {
	s := &server{app: testApp(t, "secret"), token: ""}
	reached := false
	h := s.auth(func(w http.ResponseWriter, r *http.Request) { reached = true })
	for _, hdr := range []string{"", "Bearer ", "Bearer", "Bearer secret"} {
		req := httptest.NewRequest("GET", "/api/v1/status", nil)
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status %d, want 401", hdr, rec.Code)
		}
	}
	if reached {
		t.Fatal("handler reached with an empty configured token")
	}
}

// Handler refuses to build an API it cannot protect: a token it cannot persist
// is an error, not a token only this process knows.
func TestHandlerFailsWhenTokenCannotBePersisted(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	a := testApp(t, "")
	dir := a.DataDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if h, err := Handler(a); err == nil || h != nil {
		t.Fatalf("Handler = %v, %v; want an error", h, err)
	}
}

// With a generated token, requests need exactly that token.
func TestHandlerUsesGeneratedToken(t *testing.T) {
	a := testApp(t, "")
	h, err := Handler(a)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := a.APIToken()
	if err != nil {
		t.Fatal(err)
	}
	for hdr, want := range map[string]int{
		"":              http.StatusUnauthorized,
		"Bearer ":       http.StatusUnauthorized,
		"Bearer wrong":  http.StatusUnauthorized,
		"Bearer " + tok: http.StatusOK,
	} {
		req := httptest.NewRequest("GET", "/api/v1/peers", nil)
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("Authorization %q: status %d, want %d", hdr, rec.Code, want)
		}
	}
}

// LOC-18: a refused request used to append an audit row, one synchronous
// state.db write per request. Refusals are now tallied per remote host and
// reach the audit log only as a summary when the app maintains it.
func TestUnauthenticatedRequestWritesNoAuditRow(t *testing.T) {
	a := testApp(t, "secret")
	h, err := Handler(a)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/api/v1/status", nil)
		req.Header.Set("Authorization", "Bearer wrong")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("wrong token: status %d, want 401", rec.Code)
		}
	}
	entries, err := a.Store.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unauthenticated requests wrote %d audit rows", len(entries))
	}
	a.MaintainAudit()
	entries, err = a.Store.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Action != "unauthorized" || !strings.HasPrefix(entries[0].Detail, "10 from ") {
		t.Fatalf("want one summary row for 10 refusals, got %d: %+v", len(entries), entries)
	}
}

// LOC-66: GET /api/v1/sync reports running rounds per peer (none here) and
// the last round, with progress an array rather than null.
func TestSyncStatusEndpoint(t *testing.T) {
	a := testApp(t, "secret")
	h, err := Handler(a)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/v1/sync", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/sync: %d %s", rec.Code, rec.Body)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["syncing"] != false {
		t.Fatalf("idle peer reports syncing: %s", rec.Body)
	}
	if p, ok := got["progress"].([]any); !ok || len(p) != 0 {
		t.Fatalf("progress must be an empty array: %s", rec.Body)
	}
	if _, ok := got["last_sync"]; !ok {
		t.Fatalf("no last_sync key: %s", rec.Body)
	}
}

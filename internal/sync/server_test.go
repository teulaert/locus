package sync

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/vvector"
)

// traversalRelpaths are peer-supplied paths that must never reach the
// filesystem outside the root, nor leave a record in the state database.
var traversalRelpaths = []string{
	"../victim.txt",
	"a/../../victim.txt",
	"/etc/passwd",
}

// victimOutsideRoot writes a file beside (not inside) the engine's root.
func victimOutsideRoot(t *testing.T, root string) string {
	t.Helper()
	victim := filepath.Join(filepath.Dir(root), "victim.txt")
	if err := os.WriteFile(victim, []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	return victim
}

func assertNoRecords(t *testing.T, e *engineEnv) {
	t.Helper()
	docs, err := e.Store.ListDocs(e.root, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range docs {
		t.Errorf("traversal path left a record behind: %q (deleted=%v)", d.RelPath, d.Deleted)
	}
}

// postSigned sends an authenticated peer request and returns the status and
// decoded JSON body (nil when the reply is not JSON).
func postSigned(t *testing.T, srv *httptest.Server, s *Server, path string, q url.Values, body []byte) (int, map[string]any) {
	t.Helper()
	target := path + "?" + q.Encode()
	req, err := http.NewRequest(http.MethodPost, srv.URL+target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sender := testIdentity(t)
	for k, v := range AuthHeaders(sender, http.MethodPost, target, body) {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// LOC-1: a peer must not be able to write or delete outside a root through
// /v1/put or /v1/delete. Deletes used to record a tombstone for the raw
// relpath and then os.Remove it on the next, newer delete.
func TestServerRejectsTraversalRelpaths(t *testing.T) {
	e := newEngineEnv(t)
	victim := victimOutsideRoot(t, e.root)
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	for _, rel := range traversalRelpaths {
		body := []byte("overwritten")
		q := url.Values{
			"rootname": {RootName(e.root)},
			"relpath":  {rel},
			"version":  {vvector.New().Increment(writer).String()},
			"hash":     {fswatch.HashBytes(body)},
			"size":     {"11"},
			"writer":   {writer},
		}
		if code, out := postSigned(t, srv, s, "/v1/put", q, body); code == http.StatusOK && out["result"] != string(ResultRejected) {
			t.Errorf("put %q: status %d, reply %v; want rejection", rel, code, out)
		}
		// Two deletes with increasing versions: the first used to plant a
		// tombstone, the second used to act on it.
		for i := 1; i <= 2; i++ {
			v := vvector.New()
			for j := 0; j < i; j++ {
				v = v.Increment(writer)
			}
			dq := url.Values{
				"rootname": {RootName(e.root)},
				"relpath":  {rel},
				"version":  {v.String()},
				"writer":   {writer},
			}
			if code, out := postSigned(t, srv, s, "/v1/delete", dq, nil); code == http.StatusOK && out["result"] != string(ResultRejected) {
				t.Errorf("delete %q #%d: status %d, reply %v; want rejection", rel, i, code, out)
			}
		}
	}

	got, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("file outside the root was removed: %v", err)
	}
	if string(got) != "keep me" {
		t.Fatalf("file outside the root was overwritten: %q", got)
	}
	assertNoRecords(t, e)
}

// The engine validates relpaths itself, so callers other than the HTTP
// server (the pull side of a round, which takes relpaths from a peer's
// manifest) are covered too.
func TestApplyRejectsTraversalRelpaths(t *testing.T) {
	e := newEngineEnv(t)
	victim := victimOutsideRoot(t, e.root)
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	for _, rel := range traversalRelpaths {
		v1 := vvector.New().Increment(writer)
		v2 := v1.Increment(writer)
		for _, v := range []vvector.Vector{v1, v2} {
			res, err := e.Apply(e.root, rel, Meta{Deleted: true, Version: v, LastWriter: writer}, nil, writer)
			if err == nil || res != ResultRejected {
				t.Errorf("delete %q at %s = %s, %v; want rejection", rel, v, res, err)
			}
		}
		body := "overwritten"
		meta := Meta{Hash: fswatch.HashBytes([]byte(body)), Size: int64(len(body)), Version: v1, LastWriter: writer}
		if res, err := e.Apply(e.root, rel, meta, strings.NewReader(body), writer); err == nil || res != ResultRejected {
			t.Errorf("put %q = %s, %v; want rejection", rel, res, err)
		}
	}

	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("file outside the root was touched: %v", err)
	}
	assertNoRecords(t, e)
}

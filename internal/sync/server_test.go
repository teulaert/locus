package sync

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/store"
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

func TestServerAuthorized(t *testing.T) {
	known := map[string]bool{"discovered": true}
	s := &Server{Known: func(id string) bool { return known[id] }}

	// Empty allowlist: only peers in the presence table (README: "empty
	// accepts any peer this one has discovered").
	if !s.authorized("discovered") {
		t.Fatal("discovered peer rejected with an empty allowlist")
	}
	if s.authorized("stranger") {
		t.Fatal("undiscovered peer accepted with an empty allowlist")
	}

	// Without a presence source nothing counts as discovered.
	if (&Server{}).authorized("discovered") {
		t.Fatal("nil Known must fail closed")
	}

	// An explicit allowlist is authoritative either way.
	s.AuthorizedPeers = []string{"listed"}
	if !s.authorized("listed") {
		t.Fatal("listed peer rejected")
	}
	if s.authorized("discovered") {
		t.Fatal("discovered but unlisted peer accepted")
	}
}

// putWithProtocol sends a signed /v1/put whose X-Locus-Protocol header is
// proto, or absent when proto is "" (a peer from before the header existed).
func putWithProtocol(t *testing.T, srv *httptest.Server, sender *identity.Identity, q url.Values, body []byte, proto string) *http.Response {
	t.Helper()
	target := "/v1/put?" + q.Encode()
	req, err := http.NewRequest(http.MethodPost, srv.URL+target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range AuthHeaders(sender, http.MethodPost, target, body) {
		req.Header.Set(k, v)
	}
	req.Header.Del(ProtocolHeader)
	if proto != "" {
		req.Header.Set(ProtocolHeader, proto)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

// LOC-17: a request written in another sync protocol is refused with 426
// before it reaches Apply, and the refusal names the version this peer
// speaks. A request without the header (a peer from before it existed)
// speaks version 1 and is still served.
func TestServerRejectsIncompatibleProtocol(t *testing.T) {
	e := newEngineEnv(t)
	applied := 0
	e.afterRead = func(string, string) { applied++ }
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, Known: func(string) bool { return true }}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	sender := testIdentity(t)
	body := []byte("hello")
	q := url.Values{
		"rootname": {RootName(e.root)},
		"relpath":  {"note.txt"},
		"version":  {vvector.New().Increment(sender.PeerID).String()},
		"hash":     {fswatch.HashBytes(body)},
		"size":     {"5"},
		"writer":   {sender.PeerID},
	}

	for _, proto := range []string{strconv.Itoa(ProtocolVersion + 1), "0", "two"} {
		resp := putWithProtocol(t, srv, sender, q, body, proto)
		if resp.StatusCode != http.StatusUpgradeRequired {
			t.Fatalf("protocol %q: status %d, want %d", proto, resp.StatusCode, http.StatusUpgradeRequired)
		}
		if got := resp.Header.Get(ProtocolHeader); got != strconv.Itoa(ProtocolVersion) {
			t.Fatalf("protocol %q: refusal advertises %s %q", proto, ProtocolHeader, got)
		}
	}
	if applied != 0 {
		t.Fatalf("an incompatible request reached Apply %d times", applied)
	}
	if _, err := os.Stat(filepath.Join(e.root, "note.txt")); !os.IsNotExist(err) {
		t.Fatalf("incompatible put wrote the file: %v", err)
	}
	assertNoRecords(t, e)

	// Compatibility: no header at all is the version every older peer speaks.
	if resp := putWithProtocol(t, srv, sender, q, body, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("header-less put: status %d", resp.StatusCode)
	}
	if got, err := os.ReadFile(filepath.Join(e.root, "note.txt")); err != nil || string(got) != "hello" {
		t.Fatalf("header-less put not applied: %q, %v", got, err)
	}
	if applied != 1 {
		t.Fatalf("header-less put reached Apply %d times, want 1", applied)
	}
}

// LOC-18: every refused signature used to append an audit row, so anyone who
// could reach the peer port could grow state.db without bound. Refusals are
// now only counted per remote host until the owner flushes the tally.
func TestServerRejectionsDoNotWriteAuditRows(t *testing.T) {
	e := newEngineEnv(t)
	tally := store.NewAuditTally()
	// Known is nil, so even a correctly signed request is not authorized.
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, Rejections: tally}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	for i := 0; i < 20; i++ {
		resp, err := srv.Client().Post(srv.URL+"/v1/put?rootname=docs&relpath=a.txt", "application/octet-stream", strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unsigned request returned %d, want 401", resp.StatusCode)
		}
	}
	// A put signs the content hash it declares (LOC-13).
	if status, _ := postSigned(t, srv, s, "/v1/put", url.Values{"rootname": {"docs"}, "relpath": {"a.txt"}, "hash": {fswatch.HashBytes([]byte("x"))}}, []byte("x")); status != http.StatusForbidden {
		t.Fatalf("unauthorized peer returned %d, want 403", status)
	}
	entries, err := e.Store.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("refused requests wrote %d audit rows directly: %+v", len(entries), entries[0])
	}

	if err := tally.Flush(e.Store); err != nil {
		t.Fatal(err)
	}
	entries, err = e.Store.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, en := range entries {
		got[en.Action] = en.Detail
	}
	if len(entries) != 2 || !strings.HasPrefix(got["auth-rejected"], "20 from 127.0.0.1") || !strings.HasPrefix(got["unauthorized-peer"], "1 from 127.0.0.1") {
		t.Fatalf("flush should summarize refusals per source, got %+v", got)
	}
}

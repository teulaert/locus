package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/pathguard"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// postAs sends an authenticated peer request from sender and returns the
// status and decoded JSON reply.
func postAs(t *testing.T, srv *httptest.Server, sender *identity.Identity, path string, q url.Values, body []byte) (int, map[string]any) {
	t.Helper()
	target := path + "?" + q.Encode()
	req, err := http.NewRequest(http.MethodPost, srv.URL+target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
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

func hiddenPutQuery(root, rel string, body []byte, writer string) url.Values {
	return url.Values{
		"rootname": {RootName(root)},
		"relpath":  {rel},
		"version":  {vvector.New().Increment(writer).String()},
		"hash":     {fswatch.HashBytes(body)},
		"size":     {"6"},
		"writer":   {writer},
	}
}

// A peer pushing .git/config to a root that does not sync hidden paths gets
// the same application-level rejection as a traversal: nothing lands,
// nothing is recorded, and the refusal is logged and audited once per peer
// and root rather than once per file.
func TestServerRejectsHiddenRelpaths(t *testing.T) {
	e := newEngineEnv(t)
	var logs bytes.Buffer
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, Known: func(string) bool { return true },
		Log: slog.New(slog.NewTextHandler(&logs, nil))}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	sender, other := testIdentity(t), testIdentity(t)
	body := []byte("[core]")

	for _, rel := range []string{".git/config", ".git/HEAD", "notes/.obsidian/workspace.json", ".hidden.txt"} {
		code, out := postAs(t, srv, sender, "/v1/put", hiddenPutQuery(e.root, rel, body, sender.PeerID), body)
		if code != http.StatusOK || out["result"] != string(ResultRejected) || !strings.Contains(out["error"].(string), "hidden") {
			t.Fatalf("put %q: status %d, reply %v; want a hidden-path rejection", rel, code, out)
		}
		dq := url.Values{"rootname": {RootName(e.root)}, "relpath": {rel}, "version": {vvector.New().Increment(sender.PeerID).String()}, "writer": {sender.PeerID}}
		if code, out := postAs(t, srv, sender, "/v1/delete", dq, nil); code != http.StatusOK || out["result"] != string(ResultRejected) {
			t.Fatalf("delete %q: status %d, reply %v; want rejection", rel, code, out)
		}
		if _, err := os.Stat(filepath.Join(e.root, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Fatalf("%s landed on disk: %v", rel, err)
		}
	}
	assertNoRecords(t, e)

	// The rejection is an answer the sender understands as such.
	if _, err := decodeApplyResponse([]byte(`{"result":"rejected","error":"pathguard: hidden path"}`), "put"); err == nil || IsTransport(err) {
		t.Fatalf("hidden rejection decoded as %v", err)
	}

	countLogs := func() int { return strings.Count(logs.String(), "refusing hidden paths") }
	if n := countLogs(); n != 1 {
		t.Fatalf("logged %d refusals for one peer, want 1:\n%s", n, logs.String())
	}
	if code, _ := postAs(t, srv, other, "/v1/put", hiddenPutQuery(e.root, ".git/config", body, other.PeerID), body); code != http.StatusOK {
		t.Fatalf("second peer put: %d", code)
	}
	if n := countLogs(); n != 2 {
		t.Fatalf("logged %d refusals for two peers, want 2", n)
	}
	audits, err := e.Store.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	hidden := 0
	for _, a := range audits {
		if a.Action == "hidden-rejected" {
			hidden++
		}
		if a.Action == "apply-rejected" {
			t.Fatalf("hidden path audited per file: %+v", a)
		}
	}
	if hidden != 2 {
		t.Fatalf("hidden-rejected audit rows = %d, want 2", hidden)
	}

	// With sync_hidden the same push is accepted.
	e.SyncHidden = map[string]bool{e.root: true}
	code, out := postAs(t, srv, sender, "/v1/put", hiddenPutQuery(e.root, ".git/config", body, sender.PeerID), body)
	if code != http.StatusOK || out["result"] != string(ResultApplied) {
		t.Fatalf("put with sync_hidden: status %d, reply %v", code, out)
	}
	if got, err := os.ReadFile(filepath.Join(e.root, ".git", "config")); err != nil || string(got) != "[core]" {
		t.Fatalf(".git/config with sync_hidden = %q, %v", got, err)
	}
}

// The engine refuses hidden paths itself, so the pull side and any other
// caller is covered too.
func TestApplyRejectsHiddenRelpaths(t *testing.T) {
	e := newEngineEnv(t)
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	body := "[core]"
	meta := Meta{Hash: fswatch.HashBytes([]byte(body)), Size: int64(len(body)), Version: vvector.New().Increment(writer), LastWriter: writer}
	if res, err := e.Apply(e.root, ".git/config", meta, strings.NewReader(body), writer); !errors.Is(err, pathguard.ErrHidden) || res != ResultRejected {
		t.Fatalf("put = %s, %v; want ErrHidden", res, err)
	}
	if res, err := e.Apply(e.root, ".git/config", Meta{Deleted: true, Version: meta.Version, LastWriter: writer}, nil, writer); !errors.Is(err, pathguard.ErrHidden) || res != ResultRejected {
		t.Fatalf("delete = %s, %v; want ErrHidden", res, err)
	}
	if res, err := e.PullVersion(e.root, ".git/config", &ManifestEntry{RelPath: ".git/config", Hash: meta.Hash, Version: meta.Version.String(), LastWriter: writer}); !errors.Is(err, pathguard.ErrHidden) || res != ResultRejected {
		t.Fatalf("pull version = %s, %v; want ErrHidden", res, err)
	}
	assertNoRecords(t, e)
}

// Hidden rows left by an older build (tombstoned on upgrade) never leave the
// peer: offering their tombstones would delete the files on a peer that
// syncs hidden paths, or that predates this change.
func TestManifestLeavesOutHiddenRows(t *testing.T) {
	e := newEngineEnv(t)
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	e.putFile(t, "note.txt", "note", vvector.New().Increment(writer), writer)
	if err := e.record(e.root, ".git/config", fswatch.HashBytes([]byte("x")), 0, vvector.New().Increment(writer).Increment(writer), writer, true); err != nil {
		t.Fatal(err)
	}
	m, err := e.LocalManifest(e.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || m[0].RelPath != "note.txt" {
		t.Fatalf("manifest = %+v, want only note.txt", m)
	}

	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}}
	rec := httptest.NewRecorder()
	s.manifest(rec, httptest.NewRequest(http.MethodGet, "/v1/manifest?rootname="+RootName(e.root), nil))
	var served Manifest
	if err := json.Unmarshal(rec.Body.Bytes(), &served); err != nil {
		t.Fatal(err)
	}
	if !served.SkipsHidden || len(served.Entries) != 1 {
		t.Fatalf("served manifest = %+v, want skips_hidden and one entry", served)
	}

	e.SyncHidden = map[string]bool{e.root: true}
	if m, _ := e.LocalManifest(e.root); len(m) != 2 {
		t.Fatalf("manifest with sync_hidden = %+v, want both rows", m)
	}
	rec = httptest.NewRecorder()
	s.manifest(rec, httptest.NewRequest(http.MethodGet, "/v1/manifest?rootname="+RootName(e.root), nil))
	if strings.Contains(rec.Body.String(), "skips_hidden") {
		t.Fatalf("a root that syncs hidden paths advertised skips_hidden: %s", rec.Body.String())
	}
}

// A round between a peer that syncs hidden paths and one that does not
// exchanges the working tree only, in both directions and without errors:
// the opted-in peer reads skips_hidden and does not push what would be
// refused, and the other neither pulls hidden files nor deletes them.
func TestRoundDoesNotExchangeHiddenPaths(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	a.SyncHidden = map[string]bool{a.root: true}
	wa, wb := a.Identity.PeerID, b.Identity.PeerID
	if err := os.MkdirAll(filepath.Join(a.root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	a.putFile(t, ".git/config", "[core]", vvector.New().Increment(wa), wa)
	a.putFile(t, "from-a.txt", "a", vvector.New().Increment(wa), wa)
	b.putFile(t, "from-b.txt", "b", vvector.New().Increment(wb), wb)
	// b's record of .notes/x.md, tombstoned when b was upgraded, is newer
	// than a's live copy; it must not delete a's file.
	if err := os.MkdirAll(filepath.Join(a.root, ".notes"), 0o755); err != nil {
		t.Fatal(err)
	}
	shared := vvector.New().Increment(wa)
	a.putFile(t, ".notes/x.md", "keep", shared, wa)
	if err := b.record(b.root, ".notes/x.md", fswatch.HashBytes([]byte("keep")), 0, shared.Increment(wb), wb, true); err != nil {
		t.Fatal(err)
	}

	serve := func(e *engineEnv) []*peer.Entry {
		s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, Known: func(string) bool { return true }}
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		return []*peer.Entry{{Info: peer.Info{PeerID: e.Identity.PeerID, SyncAddr: strings.TrimPrefix(srv.URL, "http://")}, Reachable: true}}
	}
	// The two roots share a name, as two peers' copies of one root do.
	b.RootNames = map[string]string{b.root: RootName(a.root)}
	a.RootNames = map[string]string{a.root: RootName(a.root)}

	for _, dir := range []struct {
		name     string
		from, to *engineEnv
	}{{"a to b", a, b}, {"b to a", b, a}} {
		for i := 0; i < 2; i++ {
			st, err := dir.from.Round(context.Background(), serve(dir.to))
			if err != nil {
				t.Fatal(err)
			}
			if len(st.Errors) != 0 {
				t.Fatalf("%s round %d errors: %v", dir.name, i, st.Errors)
			}
		}
	}
	for _, rel := range []string{"from-a.txt", "from-b.txt"} {
		for _, e := range []*engineEnv{a, b} {
			if _, err := os.Stat(filepath.Join(e.root, rel)); err != nil {
				t.Fatalf("%s missing after the rounds: %v", rel, err)
			}
		}
	}
	for _, rel := range []string{".git/config", ".notes/x.md"} {
		if _, err := os.Stat(filepath.Join(b.root, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Fatalf("%s reached the peer that does not sync hidden paths: %v", rel, err)
		}
		if _, err := os.Stat(filepath.Join(a.root, filepath.FromSlash(rel))); err != nil {
			t.Fatalf("%s removed from the peer that syncs hidden paths: %v", rel, err)
		}
		if d, err := a.Store.GetDoc(a.root, rel); err != nil || d.Deleted {
			t.Fatalf("%s record on a = %+v, %v; want live", rel, d, err)
		}
	}
}

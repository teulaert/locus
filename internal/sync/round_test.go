package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// roundProbe records the per-peer outcome callbacks of a round.
type roundProbe struct {
	failed, ok []string
}

func (p *roundProbe) attach(e *Engine) {
	e.OnPeerError = func(peerID string, err error) { p.failed = append(p.failed, peerID) }
	e.OnPeerSuccess = func(peerID string) { p.ok = append(p.ok, peerID) }
}

// fakePeer serves a manifest with one file for the root named shared and
// answers every other root the way a real server does for an unknown root.
// refuse, when set, answers the manifest of refusedRoot with that status.
func fakePeer(t *testing.T, shared string, refusedRoot string, refuse int) (*httptest.Server, string) {
	t.Helper()
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	body := "content offered by the peer"
	entry := ManifestEntry{
		RelPath:    "remote.txt",
		Size:       int64(len(body)),
		Hash:       fswatch.HashBytes([]byte(body)),
		Version:    vvector.New().Increment(writer).String(),
		LastWriter: writer,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("rootname")
		if name == refusedRoot && refuse != 0 {
			http.Error(w, http.StatusText(refuse), refuse)
			return
		}
		if name != shared {
			http.Error(w, "unknown root", http.StatusNotFound)
			return
		}
		switch r.URL.Path {
		case "/v1/manifest":
			_ = json.NewEncoder(w).Encode(Manifest{PeerID: writer, RootName: shared, Entries: []ManifestEntry{entry}})
		case "/v1/file":
			_, _ = w.Write([]byte(body))
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, body
}

// twoRootEngine gives the engine a second root, listed first, that the fake
// peer does not have.
func twoRootEngine(t *testing.T) (*engineEnv, string) {
	t.Helper()
	e := newEngineEnv(t)
	other := filepath.Join(filepath.Dir(e.root), "private")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	e.Roots = []string{other, e.root}
	return e, other
}

func peerAt(srv *httptest.Server) []*peer.Entry {
	return []*peer.Entry{{Info: peer.Info{PeerID: "dddd", SyncAddr: strings.TrimPrefix(srv.URL, "http://")}, Reachable: true}}
}

// LOC-14: peers with different root sets answer 404 for the roots they do
// not have. That is not a transport failure: no backoff, nothing in the
// round's errors, a single log line, and the shared root still syncs in the
// same round.
func TestRoundSkipsRootPeerDoesNotShare(t *testing.T) {
	e, _ := twoRootEngine(t)
	var logs bytes.Buffer
	e.Log = slog.New(slog.NewTextHandler(&logs, nil))
	var probe roundProbe
	probe.attach(e.Engine)
	srv, body := fakePeer(t, RootName(e.root), "", 0)

	for i := 0; i < 2; i++ {
		st, err := e.Round(context.Background(), peerAt(srv))
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Errors) != 0 {
			t.Fatalf("round %d errors = %v; an unshared root is not an error", i, st.Errors)
		}
	}
	if len(probe.failed) != 0 {
		t.Fatalf("OnPeerError called for a peer that answered: %v", probe.failed)
	}
	if len(probe.ok) != 2 {
		t.Fatalf("OnPeerSuccess calls = %v, want one per round", probe.ok)
	}
	got, err := os.ReadFile(filepath.Join(e.root, "remote.txt"))
	if err != nil || string(got) != body {
		t.Fatalf("shared root did not sync: %q, %v", got, err)
	}
	if n := strings.Count(logs.String(), "does not share root"); n != 1 {
		t.Fatalf("unshared root logged %d times over two rounds, want once:\n%s", n, logs.String())
	}
	if strings.Contains(logs.String(), "sync round failed") {
		t.Fatalf("unshared root logged as a failure:\n%s", logs.String())
	}
}

// Other refusals (here 403, as a peer that has not yet discovered us answers)
// are recorded for that root but do not back the peer off either.
func TestRoundRecordsRefusalWithoutBackoff(t *testing.T) {
	e, private := twoRootEngine(t)
	var probe roundProbe
	probe.attach(e.Engine)
	srv, _ := fakePeer(t, RootName(e.root), RootName(private), http.StatusForbidden)

	st, err := e.Round(context.Background(), peerAt(srv))
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Errors) != 1 || !strings.Contains(st.Errors[0], "403") {
		t.Fatalf("errors = %v, want the 403 recorded once", st.Errors)
	}
	if len(probe.failed) != 0 || len(probe.ok) != 1 {
		t.Fatalf("callbacks failed=%v ok=%v; a refusal must not back off", probe.failed, probe.ok)
	}
	if st.Pulled != 1 {
		t.Fatalf("pulled = %d, want the shared root to sync in the same round", st.Pulled)
	}
}

// A peer failing server-side (5xx) is treated like an unreachable one.
func TestRoundBacksOffOnServerError(t *testing.T) {
	e, private := twoRootEngine(t)
	var probe roundProbe
	probe.attach(e.Engine)
	srv, _ := fakePeer(t, RootName(e.root), RootName(private), http.StatusServiceUnavailable)

	st, err := e.Round(context.Background(), peerAt(srv))
	if err != nil {
		t.Fatal(err)
	}
	if len(probe.failed) != 1 || len(probe.ok) != 0 {
		t.Fatalf("callbacks failed=%v ok=%v; a 5xx must back off", probe.failed, probe.ok)
	}
	if st.Pulled != 1 {
		t.Fatalf("pulled = %d, want the other root to still sync", st.Pulled)
	}
}

func TestIsTransport(t *testing.T) {
	e := newEngineEnv(t)
	// A listener that is closed again: connecting is refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	_, err = e.FetchManifest(context.Background(), "http://"+addr, e.root)
	if err == nil || !IsTransport(err) {
		t.Fatalf("connection refused: IsTransport(%v) = false", err)
	}
	for code, want := range map[int]bool{400: false, 401: false, 403: false, 404: false, 500: true, 503: true} {
		if got := IsTransport(&StatusError{Code: code}); got != want {
			t.Errorf("IsTransport(%d) = %v, want %v", code, got, want)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.FetchManifest(ctx, "http://"+addr, e.root); err == nil || IsTransport(err) {
		t.Fatalf("our own cancellation classified as transport: %v", err)
	}
}

// LOC-54: a conflict found while pulling is recorded against the peer it was
// pulled from, by peer ID. Pull used to hand Apply the peer's base URL, so the
// audit row read peer=http://1.
func TestPullConflictNamesPeerID(t *testing.T) {
	local, remote := newEngineEnv(t), newEngineEnv(t)
	remoteID := remote.Identity.PeerID
	base := vvector.New().Increment(remoteID)
	local.putFile(t, "f.txt", "base\n", base, remoteID)
	remote.putFile(t, "f.txt", "edited on remote\n", base.Increment(remoteID), remoteID)
	// A local edit the watcher has not recorded yet: the pull finds it on
	// disk and must keep both sides.
	if err := os.WriteFile(filepath.Join(local.root, "f.txt"), []byte("edited locally\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := &Server{Engine: remote.Engine, Identity: remote.Identity, Roots: []string{remote.root}, Known: func(string) bool { return true }}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	peers := []*peer.Entry{{Info: peer.Info{PeerID: remoteID, SyncAddr: strings.TrimPrefix(srv.URL, "http://")}, Reachable: true}}
	st, err := local.Round(context.Background(), peers)
	if err != nil {
		t.Fatal(err)
	}
	if st.Pulled != 1 || st.Conflicted != 1 || len(st.Errors) != 0 {
		t.Fatalf("round stats %+v, want one conflicted pull", st)
	}

	conflicts, err := local.Store.ListConflicts()
	if err != nil {
		t.Fatal(err)
	}
	if len(conflicts) != 1 {
		t.Fatalf("%d conflicts recorded, want 1", len(conflicts))
	}
	c := conflicts[0]
	writers := []string{c.LocalWriter, c.ConflictWriter}
	if !(writers[0] == remoteID || writers[1] == remoteID) || strings.Contains(writers[0]+writers[1], "http") {
		t.Fatalf("conflict writers %q, want one of them to be peer %s", writers, remoteID)
	}

	audit, err := local.Store.ListAudit(0)
	if err != nil {
		t.Fatal(err)
	}
	var detail string
	for _, a := range audit {
		if a.Action == "conflict" {
			detail = a.Detail
		}
	}
	if !strings.Contains(detail, "peer="+identity.Short(remoteID)) || strings.Contains(detail, "http") {
		t.Fatalf("conflict audit %q, want peer=%s", detail, identity.Short(remoteID))
	}
}

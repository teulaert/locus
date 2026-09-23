package sync

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"locus/internal/fswatch"
	"locus/internal/identity"
	"locus/internal/peer"
	"locus/internal/store"
	"locus/internal/vvector"
)

// engineEnv is a standalone engine over one root, with no HTTP peer attached.
type engineEnv struct {
	*Engine
	root string
}

func newEngineEnv(t *testing.T) *engineEnv {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	id, err := identity.Load(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "docs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	return &engineEnv{root: root, Engine: &Engine{Store: st, Identity: id, Roots: []string{root}, MaxFile: 1 << 20}}
}

func TestRoundReportsUnreachablePeer(t *testing.T) {
	e := newEngineEnv(t)
	var failed, ok []string
	e.OnPeerError = func(peerID string, err error) {
		if err == nil {
			t.Error("a failed peer must be reported with an error")
		}
		failed = append(failed, peerID)
	}
	e.OnPeerSuccess = func(peerID string) { ok = append(ok, peerID) }
	// Port 1 is closed: the manifest fetch cannot connect.
	peers := []*peer.Entry{{Info: peer.Info{PeerID: "dddd", SyncAddr: "127.0.0.1:1"}, Reachable: true}}
	st, err := e.Round(context.Background(), peers)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Errors) == 0 {
		t.Fatal("round reported no error for an unreachable peer")
	}
	if len(failed) != 1 || failed[0] != "dddd" {
		t.Fatalf("OnPeerError called with %v", failed)
	}
	if len(ok) != 0 {
		t.Fatalf("OnPeerSuccess called for a failing peer: %v", ok)
	}
}

// The provenance of an incoming file is written before the file lands and
// removed once its version record is durable, so the window in which a crash
// could misattribute the file is closed at both ends.
func TestApplyWritesThenClearsPendingProvenance(t *testing.T) {
	e := newEngineEnv(t)
	const remote = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	body := "incoming content"
	meta := Meta{Hash: fswatch.HashBytes([]byte(body)), Size: int64(len(body)), Version: vvector.New().Increment(remote), LastWriter: remote}
	res, err := e.Apply(e.root, "incoming.txt", meta, strings.NewReader(body), remote)
	if err != nil || res != ResultApplied {
		t.Fatalf("apply = %s, %v", res, err)
	}
	if _, err := e.Store.GetDoc(e.root, "incoming.txt"); err != nil {
		t.Fatalf("document was not recorded: %v", err)
	}
	if _, err := e.Store.GetPendingArrival(e.root, "incoming.txt"); err != store.ErrNotFound {
		t.Fatalf("provenance outlived the record: %v", err)
	}

	// A crash between the rename and the record leaves the provenance behind
	// for reconciliation to adopt (see the reconcile package's tests).
	e.notePending(e.root, "half.txt", meta)
	if _, _, err := Install(e.root, "half.txt", strings.NewReader(body), meta.Hash, e.MaxFile); err != nil {
		t.Fatal(err)
	}
	p, err := e.Store.GetPendingArrival(e.root, "half.txt")
	if err != nil || p.Hash != meta.Hash || p.Writer != remote {
		t.Fatalf("provenance missing after a simulated crash: %+v, %v", p, err)
	}
}

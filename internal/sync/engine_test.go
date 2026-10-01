package sync

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
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

// manifestEntry returns the engine's manifest row for one path.
func (e *engineEnv) manifestEntry(t *testing.T, rel string) ManifestEntry {
	t.Helper()
	m, err := e.LocalManifest(e.root)
	if err != nil {
		t.Fatal(err)
	}
	for _, me := range m {
		if me.RelPath == rel {
			return me
		}
	}
	t.Fatalf("%s missing from manifest", rel)
	return ManifestEntry{}
}

// putFile writes content and records it as a local edit with the given vector.
func (e *engineEnv) putFile(t *testing.T, rel, body string, ver vvector.Vector, writer string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(e.root, rel), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := e.record(e.root, rel, fswatch.HashBytes([]byte(body)), int64(len(body)), ver, writer, false); err != nil {
		t.Fatal(err)
	}
}

// After an incoming-wins conflict only the receiver records the merged
// vector. The pusher must adopt it from the manifest, otherwise its next
// ordinary edit is divergent and conflicts again (LOC-6).
func TestIncomingWinsConflictVectorsConverge(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	const winner, loser = "ffffffffffffffffffffffffffffffff", "11111111111111111111111111111111"
	a.putFile(t, "f.txt", "edited on a", vvector.New().Increment(winner), winner)
	b.putFile(t, "f.txt", "edited on b", vvector.New().Increment(loser), loser)

	// a pushes to b; the incoming side wins the normal path on b.
	ae := a.manifestEntry(t, "f.txt")
	av, _ := vvector.Parse(ae.Version)
	res, err := b.Apply(b.root, "f.txt", Meta{Hash: ae.Hash, Size: ae.Size, Version: av, LastWriter: ae.LastWriter}, strings.NewReader("edited on a"), winner)
	if err != nil || res != ResultConflicted {
		t.Fatalf("apply = %s, %v", res, err)
	}

	// Same content, different vectors: a adopts b's vector, b has nothing to do.
	be := b.manifestEntry(t, "f.txt")
	acts := Plan([]ManifestEntry{a.manifestEntry(t, "f.txt")}, []ManifestEntry{be})
	if acts[0].Kind != ActionPullVersion {
		t.Fatalf("pusher plan = %s, want %s", acts[0].Kind, ActionPullVersion)
	}
	if res, err := a.PullVersion(a.root, "f.txt", acts[0].Remote); err != nil || res != ResultApplied {
		t.Fatalf("pull version = %s, %v", res, err)
	}
	ae = a.manifestEntry(t, "f.txt")
	if ae.Version != be.Version || ae.LastWriter != be.LastWriter || ae.Hash != be.Hash {
		t.Fatalf("records did not converge: a=%+v b=%+v", ae, be)
	}
	if acts := Plan([]ManifestEntry{be}, []ManifestEntry{ae}); acts[0].Kind != ActionNoop {
		t.Fatalf("receiver plan = %s, want noop", acts[0].Kind)
	}

	// The winner's next ordinary edit is a plain descendant on b.
	av, _ = vvector.Parse(ae.Version)
	next := av.Increment(winner)
	a.putFile(t, "f.txt", "follow-up on a", next, winner)
	ae = a.manifestEntry(t, "f.txt")
	res, err = b.Apply(b.root, "f.txt", Meta{Hash: ae.Hash, Size: ae.Size, Version: next, LastWriter: winner}, strings.NewReader("follow-up on a"), winner)
	if err != nil || res != ResultApplied {
		t.Fatalf("follow-up edit = %s, %v; want applied without a new conflict", res, err)
	}
	if list, _ := b.Store.ListConflicts(); len(list) != 1 {
		t.Fatalf("conflicts on b = %d, want only the original one", len(list))
	}
}

// Concurrent vectors over identical content merge to the same record on
// both sides without incrementing any counter.
func TestPullVersionMergesConcurrentVectors(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	const pa, pb = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	a.putFile(t, "f.txt", "same", vvector.Vector{pa: 2, pb: 1}, pa)
	b.putFile(t, "f.txt", "same", vvector.Vector{pa: 1, pb: 3}, pb)
	ae, be := a.manifestEntry(t, "f.txt"), b.manifestEntry(t, "f.txt")
	if res, err := a.PullVersion(a.root, "f.txt", &be); err != nil || res != ResultApplied {
		t.Fatalf("a pull version = %s, %v", res, err)
	}
	if res, err := b.PullVersion(b.root, "f.txt", &ae); err != nil || res != ResultApplied {
		t.Fatalf("b pull version = %s, %v", res, err)
	}
	ae, be = a.manifestEntry(t, "f.txt"), b.manifestEntry(t, "f.txt")
	want := vvector.Vector{pa: 2, pb: 3}.String()
	if ae.Version != want || be.Version != want {
		t.Fatalf("versions = %s / %s, want %s on both", ae.Version, be.Version, want)
	}
	if ae.LastWriter != be.LastWriter {
		t.Fatalf("last writers differ: %s / %s", ae.LastWriter, be.LastWriter)
	}

	// Content that changed since the plan is left alone.
	stale := be
	stale.Hash = "other"
	stale.Version = vvector.Vector{pa: 9}.String()
	if res, err := a.PullVersion(a.root, "f.txt", &stale); err != nil || res != ResultNoop {
		t.Fatalf("stale pull version = %s, %v; want noop", res, err)
	}
}

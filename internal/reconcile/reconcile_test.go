package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/jobs"
	"github.com/teulaert/locus/internal/pipeline"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

type env struct {
	root string
	rec  *Reconciler
	st   *store.Store
	q    *jobs.Queue
	ctx  context.Context
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	id, err := identity.Load(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	q := jobs.NewQueue(st.Index)
	e := &env{root: root, st: st, q: q, ctx: context.Background()}
	e.rec = &Reconciler{Store: st, Queue: q, Identity: id, Roots: []string{root}, TombstoneAge: 30 * 24 * time.Hour}
	return e
}

func (e *env) write(t *testing.T, rel, content string) {
	t.Helper()
	full := filepath.Join(e.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDetectsCreateModifyDelete(t *testing.T) {
	e := newEnv(t)
	e.write(t, "a.txt", "one")
	e.write(t, "sub/b.md", "# B\n\ncontent")
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Created != 2 {
		t.Fatalf("created = %d, want 2 (%+v)", reports[0].Created, reports[0])
	}
	// Modify.
	e.write(t, "a.txt", "two")
	reports, _ = e.rec.Pass(e.ctx)
	if reports[0].Modified != 1 {
		t.Fatalf("modified = %d (%+v)", reports[0].Modified, reports[0])
	}
	// Delete.
	if err := os.Remove(filepath.Join(e.root, "sub", "b.md")); err != nil {
		t.Fatal(err)
	}
	reports, _ = e.rec.Pass(e.ctx)
	if reports[0].Deleted != 1 {
		t.Fatalf("deleted = %d (%+v)", reports[0].Deleted, reports[0])
	}
	// A clean pass has nothing further to record (index work may still be
	// queued for the files created above).
	reports, _ = e.rec.Pass(e.ctx)
	if reports[0].Created != 0 || reports[0].Modified != 0 || reports[0].Deleted != 0 {
		t.Fatalf("steady state not detected: %+v", reports[0])
	}
}

func TestVersionVectorIncrementsOnlyForRealChanges(t *testing.T) {
	e := newEnv(t)
	e.write(t, "a.txt", "v1")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	first, err := e.st.GetDoc(e.root, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if first.Version.String() == "" || first.Deleted {
		t.Fatalf("unexpected doc: %+v", first)
	}
	// Unchanged pass must not bump the version.
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	same, _ := e.st.GetDoc(e.root, "a.txt")
	if same.Version.String() != first.Version.String() {
		t.Fatalf("unchanged file bumped version: %s -> %s", first.Version, same.Version)
	}
	// Real change bumps exactly one counter.
	e.write(t, "a.txt", "v2")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	next, _ := e.st.GetDoc(e.root, "a.txt")
	if vvector.Compare(first.Version, next.Version) != vvector.Before {
		t.Fatalf("modification must produce a strict descendant: %s -> %s", first.Version, next.Version)
	}
	if next.LastWriter != e.rec.Identity.PeerID {
		t.Fatalf("last_writer = %s", next.LastWriter)
	}
}

func TestMissedEventsAreCaughtByReconciliation(t *testing.T) {
	// No watcher is running at all: this models events lost to sleep, crash
	// or an external process editing files.
	e := newEnv(t)
	e.write(t, "outside.txt", "written by another program")
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Created != 1 {
		t.Fatalf("periodic reconciliation missed external changes: %+v", reports[0])
	}
	if _, err := e.st.GetDoc(e.root, "outside.txt"); err != nil {
		t.Fatalf("external change not recorded: %v", err)
	}
}

func TestRapidWritesCoalesceIntoOneJob(t *testing.T) {
	e := newEnv(t)
	e.write(t, "burst.txt", "v1")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	// Create, modify twice and delete before the queue is ever drained.
	for i := 0; i < 3; i++ {
		e.write(t, "burst.txt", "intermediate")
	}
	full := filepath.Join(e.root, "burst.txt")
	if err := os.Remove(full); err != nil {
		t.Fatal(err)
	}
	e.write(t, "burst.txt", "final")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	// Only the newest version may remain queued for this path.
	list, err := e.q.List(e.ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, j := range list {
		if j.RelPath == "burst.txt" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected one coalesced job for burst.txt, got %d (%+v)", n, list)
	}
}

func TestDeletedFilesQueueUnindexWork(t *testing.T) {
	e := newEnv(t)
	e.write(t, "doomed.txt", "indexed then deleted")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(e.root, "doomed.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	list, _ := e.q.List(e.ctx, 100)
	var found bool
	for _, j := range list {
		if j.Kind == jobs.KindUnindex && j.RelPath == "doomed.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("deletion must queue index removal, jobs = %+v", list)
	}
	doc, err := e.st.GetDoc(e.root, "doomed.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !doc.Deleted {
		t.Fatal("deletion must leave a tombstone record")
	}
}

func TestSymlinksAreNotInventoried(t *testing.T) {
	e := newEnv(t)
	e.write(t, "real.txt", "content")
	if err := os.Symlink("/etc/passwd", filepath.Join(e.root, "escape")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.st.GetDoc(e.root, "escape"); err == nil {
		t.Fatal("symlinked path must not be inventoried")
	}
	if _, err := e.st.GetDoc(e.root, "real.txt"); err != nil {
		t.Fatal("regular files must still be recorded")
	}
}

func TestEnsureRoots(t *testing.T) {
	e := newEnv(t)
	if err := e.rec.EnsureRoots(); err != nil {
		t.Fatalf("valid roots rejected: %v", err)
	}
	e.rec.Roots = nil
	if err := e.rec.EnsureRoots(); err == nil {
		t.Fatal("empty roots accepted")
	}
	fileRoot := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(fileRoot, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.rec.Roots = []string{fileRoot}
	if err := e.rec.EnsureRoots(); err == nil {
		t.Fatal("non-directory root accepted")
	}
}

// A process that dies after the atomic rename but before the document record
// must not come back claiming it authored the received file: that corrupts the
// version vector and the author used for deterministic conflict resolution.
func TestReceivedFileWithoutRecordKeepsRemoteAuthor(t *testing.T) {
	e := newEnv(t)
	const remote = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	body := "received over the wire"
	hash := fswatch.HashBytes([]byte(body))
	ver := vvector.New().Increment(remote)
	if err := e.st.PutPendingArrival(&store.PendingArrival{
		Root: e.root, RelPath: "recv.txt", Hash: hash, Version: ver.String(), Writer: remote,
	}); err != nil {
		t.Fatal(err)
	}
	e.write(t, "recv.txt", body)
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Created != 1 {
		t.Fatalf("created = %d", reports[0].Created)
	}
	doc, err := e.st.GetDoc(e.root, "recv.txt")
	if err != nil {
		t.Fatal(err)
	}
	if doc.LastWriter != remote {
		t.Fatalf("last_writer = %s, want the remote author", doc.LastWriter)
	}
	if doc.Version.String() != ver.String() {
		t.Fatalf("version = %s, want the remote %s", doc.Version.String(), ver.String())
	}
	if _, err := e.st.GetPendingArrival(e.root, "recv.txt"); err != store.ErrNotFound {
		t.Fatalf("pending provenance was not consumed: %v", err)
	}
}

func TestUpdatedFileWithoutRecordKeepsRemoteAuthor(t *testing.T) {
	e := newEnv(t)
	const remote = "cccccccccccccccccccccccccccccccc"
	e.write(t, "edit.txt", "local original")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	local, err := e.st.GetDoc(e.root, "edit.txt")
	if err != nil {
		t.Fatal(err)
	}
	body := "the remote rewrite that landed but was never recorded"
	ver := local.Version.Increment(remote)
	if err := e.st.PutPendingArrival(&store.PendingArrival{
		Root: e.root, RelPath: "edit.txt", Hash: fswatch.HashBytes([]byte(body)), Version: ver.String(), Writer: remote,
	}); err != nil {
		t.Fatal(err)
	}
	e.write(t, "edit.txt", body)
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	doc, err := e.st.GetDoc(e.root, "edit.txt")
	if err != nil {
		t.Fatal(err)
	}
	if doc.LastWriter != remote || doc.Version.String() != ver.String() {
		t.Fatalf("doc = %+v version %s, want writer %s version %s", doc, doc.Version.String(), remote, ver.String())
	}
}

// An edit made here is still ours: provenance for a different hash is ignored.
func TestLocalEditIsNotAttributedToStaleProvenance(t *testing.T) {
	e := newEnv(t)
	e.write(t, "mine.txt", "one")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.st.PutPendingArrival(&store.PendingArrival{
		Root: e.root, RelPath: "mine.txt", Hash: "sha256:deadbeef", Version: "7:somepeer", Writer: "somepeer",
	}); err != nil {
		t.Fatal(err)
	}
	e.write(t, "mine.txt", "two")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	doc, err := e.st.GetDoc(e.root, "mine.txt")
	if err != nil {
		t.Fatal(err)
	}
	if doc.LastWriter != e.rec.Identity.PeerID {
		t.Fatalf("last_writer = %s, want this peer", doc.LastWriter)
	}
}

// drain runs every queued job through the real pipeline.
func (e *env) drain(t *testing.T) {
	t.Helper()
	p := &pipeline.Pipeline{Store: e.st, Queue: e.q, Embedder: &embed.Hashing{}, Gate: &pipeline.Gate{}}
	for {
		job, err := e.q.Claim(e.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			return
		}
		if err := p.Process(e.ctx, job); err != nil {
			t.Fatalf("process %s: %v", job.RelPath, err)
		}
		if err := e.q.Complete(e.ctx, job); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNonExtractableFilesAreNotRequeuedEveryPass(t *testing.T) {
	e := newEnv(t)
	e.write(t, "photo.jpg", "\xff\xd8\xff\xe0 not really a jpeg")
	e.write(t, "notes.txt", "plain text")
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Jobs != 2 {
		t.Fatalf("first pass jobs = %d, want 2 (%+v)", reports[0].Jobs, reports[0])
	}
	e.drain(t)

	// With the queue drained and nothing changed on disk, the root is in
	// sync: a file without text must not be queued (and re-hashed and
	// re-extracted) again on every pass.
	for i := 0; i < 2; i++ {
		reports, err = e.rec.Pass(e.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if reports[0].Jobs != 0 || !reports[0].InSync {
			t.Fatalf("pass %d after drain: %+v", i+2, reports[0])
		}
	}

	// A new version of the file is real work again.
	e.write(t, "photo.jpg", "\xff\xd8\xff\xe0 a different picture")
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Modified != 1 || reports[0].Jobs != 1 {
		t.Fatalf("changed non-extractable file not re-queued: %+v", reports[0])
	}
	e.drain(t)
	reports, _ = e.rec.Pass(e.ctx)
	if reports[0].Jobs != 0 || !reports[0].InSync {
		t.Fatalf("not in sync after re-processing changed file: %+v", reports[0])
	}

	// Lost derived state (e.g. a rebuilt index) is still noticed.
	if err := e.st.DropDerived(); err != nil {
		t.Fatal(err)
	}
	reports, _ = e.rec.Pass(e.ctx)
	if reports[0].Jobs != 2 {
		t.Fatalf("dropped derived state not re-queued: %+v", reports[0])
	}
}

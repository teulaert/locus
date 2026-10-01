package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/teulaert/locus/internal/jobs"
)

// flakyQueue fronts the real index queue and fails every Enqueue while fail
// is set, as a broken index.db would.
type flakyQueue struct {
	q     *jobs.Queue
	fail  atomic.Bool
	calls atomic.Int32
}

var errIndexDown = errors.New("index.db: disk I/O error")

func (f *flakyQueue) Enqueue(ctx context.Context, kind, root, relpath, hash string, priority int) error {
	f.calls.Add(1)
	if f.fail.Load() {
		return errIndexDown
	}
	return f.q.Enqueue(ctx, kind, root, relpath, hash, priority)
}

func queued(t *testing.T, e *env, kind, relpath string) bool {
	t.Helper()
	list, err := e.q.List(e.ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range list {
		if j.Kind == kind && j.RelPath == relpath {
			return true
		}
	}
	return false
}

func TestIndexQueueFailureStillRecordsState(t *testing.T) {
	e := newEnv(t)
	fq := &flakyQueue{q: e.q}
	fq.fail.Store(true)
	e.rec.Queue = fq
	e.write(t, "a.txt", "first")
	e.write(t, "b.txt", "second")

	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatalf("an index.db failure must not fail the pass: %v", err)
	}
	rep := reports[0]
	if rep.Created != 2 {
		t.Fatalf("created = %d, want 2 (%+v)", rep.Created, rep)
	}
	if rep.IndexErrors != 2 || !strings.Contains(rep.IndexError, errIndexDown.Error()) {
		t.Fatalf("report must carry the queue failures, got %d %q", rep.IndexErrors, rep.IndexError)
	}
	if rep.Jobs != 0 || rep.InSync {
		t.Fatalf("nothing was queued, so the pass is neither queued nor in sync: %+v", rep)
	}
	// Every file reached state.db although none reached the queue.
	for _, rel := range []string{"a.txt", "b.txt"} {
		d, err := e.st.GetDoc(e.root, rel)
		if err != nil {
			t.Fatalf("%s not recorded in state.db: %v", rel, err)
		}
		if d.Deleted || d.Hash == "" {
			t.Fatalf("%s recorded wrongly: %+v", rel, d)
		}
	}
	if fq.calls.Load() != 2 {
		t.Fatalf("pass must keep going after the first failure: %d enqueue attempts", fq.calls.Load())
	}

	// index.db recovers: the next pass queues the missing index work.
	fq.fail.Store(false)
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	rep = reports[0]
	if rep.IndexErrors != 0 || rep.IndexError != "" || rep.Jobs != 2 || rep.Unchanged != 2 {
		t.Fatalf("recovery pass: %+v", rep)
	}
	for _, rel := range []string{"a.txt", "b.txt"} {
		if !queued(t, e, jobs.KindIndex, rel) {
			t.Fatalf("recovery pass must queue indexing of %s", rel)
		}
	}
}

func TestLostUnindexIsRequeued(t *testing.T) {
	e := newEnv(t)
	fq := &flakyQueue{q: e.q}
	e.rec.Queue = fq
	e.write(t, "gone.txt", "searchable until deleted")
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	d, err := e.st.GetDoc(e.root, "gone.txt")
	if err != nil {
		t.Fatal(err)
	}
	// Stand in for the pipeline having indexed it.
	if err := e.st.IndexNoText(e.root, "gone.txt", d.Hash, d.Size, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.q.DropFor(e.ctx, e.root, "gone.txt"); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(e.root, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	fq.fail.Store(true)
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep := reports[0]; rep.Deleted != 1 || rep.IndexErrors != 1 {
		t.Fatalf("deletion pass: %+v", rep)
	}
	if d, err := e.st.GetDoc(e.root, "gone.txt"); err != nil || !d.Deleted {
		t.Fatalf("deletion must still leave a tombstone: %+v %v", d, err)
	}

	fq.fail.Store(false)
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep := reports[0]; rep.IndexErrors != 0 || rep.Jobs != 1 {
		t.Fatalf("recovery pass: %+v", rep)
	}
	if !queued(t, e, jobs.KindUnindex, "gone.txt") {
		t.Fatal("recovery pass must queue removal of the deleted file's index entry")
	}

	// Once the index entry is gone the tombstone needs no more work.
	if err := e.st.DeleteIndexDocument(e.root, "gone.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.q.DropFor(e.ctx, e.root, "gone.txt"); err != nil {
		t.Fatal(err)
	}
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reports[0].InSync {
		t.Fatalf("tombstone without index entry must be in sync: %+v", reports[0])
	}
}

package pipeline

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/reconcile"
)

// countHashes makes the pipeline count the files it reads to hash.
func (f *fixture) countHashes() *int {
	n := new(int)
	f.p.hashFile = func(path string) (string, int64, error) {
		*n++
		return fswatch.HashFile(path)
	}
	return n
}

func TestPipelineReusesHashFromReconciliation(t *testing.T) {
	f := newFixture(t)
	old := time.Now().Add(-time.Hour)
	for _, rel := range []string{"a.txt", "b.txt"} {
		f.write(t, rel, "the content of "+rel)
		if err := os.Chtimes(filepath.Join(f.root, rel), old, old); err != nil {
			t.Fatal(err)
		}
	}
	rec := &reconcile.Reconciler{Store: f.st, Queue: f.q, Roots: []string{f.root}}
	reports, err := rec.Pass(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Hashed != 2 || reports[0].Jobs != 2 {
		t.Fatalf("reconcile: %+v", reports[0])
	}

	// b.txt changes after reconciliation queued it: its job is superseded,
	// and only a fresh hash can tell.
	f.write(t, "b.txt", "rewritten after the pass")

	hashes := f.countHashes()
	f.runQueue(t)
	if *hashes != 1 {
		t.Fatalf("pipeline hashed %d files, want 1 (only the one changed since reconciliation)", *hashes)
	}
	var indexed int
	if err := f.st.Index.QueryRow(`SELECT COUNT(*) FROM documents`).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if indexed != 1 {
		t.Fatalf("indexed documents = %d, want 1 (a.txt; b.txt's job was superseded)", indexed)
	}
}

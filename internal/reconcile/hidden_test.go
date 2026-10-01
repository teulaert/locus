package reconcile

import (
	"os"
	"path/filepath"
	"testing"
)

// TestHiddenRowsAreRetiredOnUpgrade simulates an install upgraded from a
// build that inventoried hidden paths: their records and index entries exist
// already. A pass on a root that does not sync hidden paths treats them as
// deleted (tombstoned, unindexed) while leaving the files on disk alone.
func TestHiddenRowsAreRetiredOnUpgrade(t *testing.T) {
	e := newEnv(t)
	e.write(t, "README.md", "working tree readme")
	e.write(t, ".git/config", "[core] repositoryformatversion")
	e.write(t, ".hidden.txt", "a dotfile with searchable words")
	e.write(t, "notes/.obsidian/workspace.json", `{"open": "readme"}`)

	// The old behaviour: everything is inventoried and indexed.
	e.rec.SyncHidden = map[string]bool{e.root: true}
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Created != 4 {
		t.Fatalf("created = %d, want 4 (%+v)", reports[0].Created, reports[0])
	}
	e.drain(t)
	hidden := []string{".git/config", ".hidden.txt", "notes/.obsidian/workspace.json"}
	for _, rel := range hidden {
		if ok, err := e.rec.indexed(e.root, rel); err != nil || !ok {
			t.Fatalf("%s not indexed before the upgrade (%v)", rel, err)
		}
	}

	// The upgrade: the root no longer syncs hidden paths.
	e.rec.SyncHidden = nil
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Deleted != 3 || reports[0].Created != 0 || reports[0].Modified != 0 {
		t.Fatalf("upgrade pass = %+v, want the 3 hidden records deleted", reports[0])
	}
	e.drain(t)
	for _, rel := range hidden {
		doc, err := e.st.GetDoc(e.root, rel)
		if err != nil {
			t.Fatal(err)
		}
		if !doc.Deleted {
			t.Fatalf("%s still live after the upgrade pass: %+v", rel, doc)
		}
		if ok, err := e.rec.indexed(e.root, rel); err != nil || ok {
			t.Fatalf("%s still indexed after the upgrade pass (%v)", rel, err)
		}
		// Retiring a record never touches the file: it is the user's.
		if _, err := os.Stat(filepath.Join(e.root, filepath.FromSlash(rel))); err != nil {
			t.Fatalf("%s removed from disk: %v", rel, err)
		}
	}
	if doc, err := e.st.GetDoc(e.root, "README.md"); err != nil || doc.Deleted {
		t.Fatalf("README.md = %+v, %v; want live", doc, err)
	}
	if ok, err := e.rec.indexed(e.root, "README.md"); err != nil || !ok {
		t.Fatalf("README.md dropped from the index (%v)", err)
	}

	// Converged: the next pass has nothing to do, and edits under .git are
	// not noticed at all.
	e.write(t, ".git/config", "[core] changed")
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reports[0].InSync {
		t.Fatalf("pass after upgrade not in sync: %+v", reports[0])
	}

	// Opting back in brings them back as fresh versions.
	e.rec.SyncHidden = map[string]bool{e.root: true}
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Created != 3 {
		t.Fatalf("opt-in pass = %+v, want the 3 hidden files re-created", reports[0])
	}
}

package reconcile

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// tombstone records rel as deleted, the way a synced deletion leaves it.
func (e *env) tombstone(t *testing.T, rel string) {
	t.Helper()
	const writer = "ffffffffffffffffffffffffffffffff"
	if err := e.st.UpsertDoc(&store.Doc{Root: e.root, RelPath: rel, Hash: "dead", Version: vvector.New().Increment(writer), LastWriter: writer, Deleted: true}); err != nil {
		t.Fatal(err)
	}
}

func (e *env) mkdir(t *testing.T, rel string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(e.root, filepath.FromSlash(rel)), 0o755); err != nil {
		t.Fatal(err)
	}
}

func (e *env) exists(rel string) bool {
	_, err := os.Lstat(filepath.Join(e.root, filepath.FromSlash(rel)))
	return err == nil
}

// A pass removes the empty skeleton a synced rename left behind (every file
// below it tombstoned), and only that: an empty folder the user made, a
// folder holding just a hidden file, a folder with a live file and the root
// itself all stay (LOC-74).
func TestPassPrunesDirectoriesSyncEmptied(t *testing.T) {
	e := newEnv(t)
	var logs bytes.Buffer
	e.rec.Log = slog.New(slog.NewTextHandler(&logs, nil))

	// The skeleton of a folder renamed on another peer.
	e.mkdir(t, "Postvak In/Meivakantie 2026/Visa Indonesië")
	e.mkdir(t, "Postvak In/Meivakantie 2026/Vliegtickets/Archief")
	e.tombstone(t, "Postvak In/Meivakantie 2026/Visa Indonesië/visum.pdf")
	e.tombstone(t, "Postvak In/Meivakantie 2026/Vliegtickets/Archief/ticket.pdf")
	e.tombstone(t, "Postvak In/readme.txt")
	// An empty folder the user created: no tombstones below it.
	e.mkdir(t, "Nieuw/leeg")
	// Emptied by sync, but Finder left a .DS_Store in it.
	e.tombstone(t, "finder/doc.txt")
	e.write(t, "finder/.DS_Store", "finder state")
	// A subtree emptied next to a live file: the subtree goes, the parent
	// stays.
	e.tombstone(t, "mixed/old/gone.txt")
	e.mkdir(t, "mixed/old")
	e.write(t, "mixed/live.txt", "still here")
	// A hidden directory on a root that does not sync hidden paths is never
	// looked into, even with tombstones left by a build that did.
	e.tombstone(t, ".trash/old/x.txt")
	e.mkdir(t, ".trash/old")
	// A symlink is not a directory to prune, even when it points at one.
	e.tombstone(t, "link/f.txt")
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(e.root, "link")); err != nil {
		t.Fatal(err)
	}

	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := reports[0].DirsPruned; got != 6 {
		t.Fatalf("pruned %d directories, want 6 (the five of Postvak In and mixed/old): %+v\n%s", got, reports[0], logs.String())
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("pruning warned:\n%s", logs.String())
	}
	if e.exists("Postvak In") {
		t.Fatal("the emptied skeleton survived the pass")
	}
	if e.exists("mixed/old") {
		t.Fatal("mixed/old survived the pass")
	}
	for _, rel := range []string{"Nieuw/leeg", "finder/.DS_Store", "mixed/live.txt", ".trash/old", "link"} {
		if !e.exists(rel) {
			t.Fatalf("%s was removed", rel)
		}
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("symlink target removed: %v", err)
	}
	if _, err := os.Stat(e.root); err != nil {
		t.Fatalf("root removed: %v", err)
	}
	if n := strings.Count(logs.String(), `msg="pruned empty directory"`); n != 6 {
		t.Fatalf("logged %d pruned directories, want 6:\n%s", n, logs.String())
	}
	for _, rel := range []string{`path="Postvak In"`, `path="Postvak In/Meivakantie 2026/Vliegtickets/Archief"`, "path=mixed/old"} {
		if !strings.Contains(logs.String(), rel) {
			t.Fatalf("pruning %s was not logged:\n%s", rel, logs.String())
		}
	}

	// With nothing left to prune, the next pass removes nothing.
	reports, err = e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].DirsPruned != 0 {
		t.Fatalf("second pass pruned %d", reports[0].DirsPruned)
	}

	// A root that syncs hidden paths treats a hidden shell like any other.
	e.rec.SyncHidden = map[string]bool{e.root: true}
	if _, err := e.rec.Pass(e.ctx); err != nil {
		t.Fatal(err)
	}
	if e.exists(".trash") {
		t.Fatal(".trash survived a pass on a root that syncs hidden paths")
	}
}

// A directory holding a single hidden file is never removed, whatever the
// records say: os.Remove refuses a directory with any entry.
func TestPassKeepsDirectoryWithOnlyAHiddenFile(t *testing.T) {
	e := newEnv(t)
	e.rec.SyncHidden = map[string]bool{e.root: true}
	e.tombstone(t, "a/b/doc.txt")
	e.tombstone(t, "a/b/.DS_Store")
	e.write(t, "a/b/.DS_Store", "x")
	// The records the pass reads say everything below a/ is deleted, so
	// a/b is a candidate; only the file in it keeps it.
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !e.exists("a/b/.DS_Store") {
		t.Fatal("hidden file removed")
	}
	if reports[0].DirsPruned != 0 {
		t.Fatalf("pruned %d directories", reports[0].DirsPruned)
	}
}

// Tombstones only at the top level never put the root up for removal.
func TestPassNeverPrunesRoot(t *testing.T) {
	e := newEnv(t)
	e.tombstone(t, "gone.txt")
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].DirsPruned != 0 {
		t.Fatalf("pruned %d", reports[0].DirsPruned)
	}
	if _, err := os.Stat(e.root); err != nil {
		t.Fatalf("root removed: %v", err)
	}
}

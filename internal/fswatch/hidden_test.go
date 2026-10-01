package fswatch

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

// hiddenTree lays out a root holding a git repository, editor state and a
// hidden file next to ordinary documents.
func hiddenTree(t *testing.T, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "README.md"), "working tree")
	writeFile(t, filepath.Join(root, "src", "main.go"), "package main")
	writeFile(t, filepath.Join(root, ".git", "config"), "[core]")
	writeFile(t, filepath.Join(root, ".git", "HEAD"), "ref: refs/heads/main")
	writeFile(t, filepath.Join(root, ".git", "objects", "ab", "cdef"), "blob")
	writeFile(t, filepath.Join(root, ".hidden.txt"), "dotfile")
	writeFile(t, filepath.Join(root, "notes", ".obsidian", "workspace.json"), "{}")
}

func relPaths(es []Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.RelPath
	}
	return out
}

func TestScanSkipsHiddenPathsByDefault(t *testing.T) {
	root := t.TempDir()
	hiddenTree(t, root)

	got, err := Scan(root, ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"README.md", "src/main.go"}
	if !reflect.DeepEqual(relPaths(got), want) {
		t.Fatalf("inventory = %v, want %v", relPaths(got), want)
	}
}

func TestScanNeverLooksAtHiddenFiles(t *testing.T) {
	root := t.TempDir()
	hiddenTree(t, root)
	known := func(rel string, _ Stat) (string, bool) {
		if rel != "README.md" && rel != "src/main.go" {
			t.Errorf("scan consulted known for hidden path %q", rel)
		}
		return "", false
	}
	if _, err := Scan(root, ScanOptions{Known: known}); err != nil {
		t.Fatal(err)
	}
}

func TestScanIncludesHiddenPathsWhenEnabled(t *testing.T) {
	root := t.TempDir()
	hiddenTree(t, root)
	writeFile(t, filepath.Join(root, ".git", TempPrefix+"x"), "internal temps stay out")

	got, err := Scan(root, ScanOptions{Hidden: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".git/HEAD", ".git/config", ".git/objects/ab/cdef", ".hidden.txt", "README.md", "notes/.obsidian/workspace.json", "src/main.go"}
	if !reflect.DeepEqual(relPaths(got), want) {
		t.Fatalf("inventory = %v, want %v", relPaths(got), want)
	}
}

func TestScanOfHiddenNamedRoot(t *testing.T) {
	// A root may itself be a dot directory (~/.notes); only components
	// below it count.
	root := filepath.Join(t.TempDir(), ".notes")
	writeFile(t, filepath.Join(root, "a.md"), "a")
	writeFile(t, filepath.Join(root, ".b.md"), "b")
	got, err := Scan(root, ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a.md"}; !reflect.DeepEqual(relPaths(got), want) {
		t.Fatalf("inventory = %v, want %v", relPaths(got), want)
	}
}

func TestSweepTempsSkipsHiddenDirsUnlessEnabled(t *testing.T) {
	root := t.TempDir()
	stale := filepath.Join(root, ".git", TempPrefix+"stale")
	writeFile(t, stale, "x")
	old := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	if n, err := SweepTemps(root, time.Hour, false); err != nil || n != 0 {
		t.Fatalf("sweep without hidden = %d, %v; want 0", n, err)
	}
	if n, err := SweepTemps(root, time.Hour, true); err != nil || n != 1 {
		t.Fatalf("sweep with hidden = %d, %v; want 1", n, err)
	}
}

// watched returns the watched directories relative to root.
func watched(t *testing.T, w *Watcher, root string) []string {
	t.Helper()
	var out []string
	for _, p := range w.w.WatchList() {
		rel, err := filepath.Rel(root, p)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, filepath.ToSlash(rel))
	}
	sort.Strings(out)
	return out
}

func TestWatcherDoesNotRegisterHiddenDirs(t *testing.T) {
	root := t.TempDir()
	hiddenTree(t, root)

	w := NewWatcher([]string{root}, 50*time.Millisecond, nil)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if got, want := watched(t, w, root), []string{".", "notes", "src"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("watched = %v, want %v", got, want)
	}

	all := NewWatcher([]string{root}, 50*time.Millisecond, nil)
	all.SyncHidden = map[string]bool{root: true}
	if err := all.Start(); err != nil {
		t.Fatal(err)
	}
	defer all.Stop()
	want := []string{".", ".git", ".git/objects", ".git/objects/ab", "notes", "notes/.obsidian", "src"}
	if got := watched(t, all, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("watched with sync_hidden = %v, want %v", got, want)
	}
}

func TestWatcherIgnoresHiddenChanges(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "seed.txt"), "seed")
	notified := make(chan string, 32)
	w := NewWatcher([]string{root}, 50*time.Millisecond, func(reason string) { notified <- reason })
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// A hidden directory created while running is neither registered nor
	// reported, and neither is a hidden file in a watched directory.
	writeFile(t, filepath.Join(root, ".git", "index.lock"), "lock")
	writeFile(t, filepath.Join(root, ".DS_Store"), "finder")
	select {
	case r := <-notified:
		t.Fatalf("hidden change was reported (%s)", r)
	case <-time.After(400 * time.Millisecond):
	}
	if got := watched(t, w, root); !reflect.DeepEqual(got, []string{"."}) {
		t.Fatalf("watched = %v, want only the root", got)
	}

	writeFile(t, filepath.Join(root, "seed.txt"), "changed")
	select {
	case <-notified:
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary change was not reported")
	}
}

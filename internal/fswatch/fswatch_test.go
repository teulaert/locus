package fswatch

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestScanInventoriesRegularFilesOnly(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "note.txt"), "hello")
	writeFile(t, filepath.Join(root, "Finance", "invoice.md"), "# invoice")
	writeFile(t, filepath.Join(root, ".locus-tmp-abc123"), "half-written transfer")

	// A symlink must never enter the inventory: it could name a path outside
	// the root, which synchronization would then write to on every peer.
	if err := os.Symlink(filepath.Join(root, "note.txt"), filepath.Join(root, "link.txt")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	outside := filepath.Join(t.TempDir(), "secret.txt")
	writeFile(t, outside, "not in the root")
	if err := os.Symlink(outside, filepath.Join(root, "escape.txt")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}

	got, err := Scan(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("inventory = %+v, want the two regular files only", got)
	}
	if got[0].RelPath != "Finance/invoice.md" || got[1].RelPath != "note.txt" {
		t.Fatalf("unexpected inventory %+v", got)
	}
	// Relative paths are slash-form so they mean the same thing on every peer.
	for _, e := range got {
		if strings.ContainsRune(e.RelPath, '\\') {
			t.Fatalf("relpath %q is not slash-form", e.RelPath)
		}
		if e.Size == 0 || len(e.Hash) != 64 {
			t.Fatalf("entry %q lacks size or hash: %+v", e.RelPath, e)
		}
	}
}

func TestScanMissingRootIsEmpty(t *testing.T) {
	// A configured root on a path that does not exist yet (a folder that lives
	// on a disk which is not mounted) must not fail the whole inventory.
	got, err := Scan(filepath.Join(t.TempDir(), "absent"))
	if err != nil {
		t.Fatalf("scan of missing root: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty inventory, got %+v", got)
	}
}

func TestHashFileMatchesHashBytes(t *testing.T) {
	root := t.TempDir()
	body := "the file content is the hash\n"
	path := filepath.Join(root, "a.txt")
	writeFile(t, path, body)

	sum, size, err := HashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(body)) {
		t.Fatalf("size = %d, want %d", size, len(body))
	}
	if want := HashBytes([]byte(body)); sum != want {
		t.Fatalf("HashFile = %s, HashBytes = %s", sum, want)
	}

	// A changed file must hash differently, otherwise updates are invisible.
	writeFile(t, path, body+"changed")
	changed, _, err := HashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if changed == sum {
		t.Fatal("hash did not change after the file changed")
	}
}

func TestWatcherReportsChangesAfterDebounce(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "seed.txt"), "seed")

	notified := make(chan string, 32)
	w := NewWatcher([]string{root}, 60*time.Millisecond, func(reason string) { notified <- reason })
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Give the watcher a moment to register the tree.
	time.Sleep(150 * time.Millisecond)
	writeFile(t, filepath.Join(root, "sub", "new.txt"), "created while running")

	select {
	case reason := <-notified:
		if reason == "" {
			t.Fatal("notify called with an empty reason")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher never reported the new file")
	}
}

func TestEnsureRoots(t *testing.T) {
	dir := t.TempDir()
	fresh := filepath.Join(dir, "fresh", "Documents")
	blocker := filepath.Join(dir, "afile")
	writeFile(t, blocker, "not a directory")

	if err := EnsureRoots([]string{fresh}); err != nil {
		t.Fatalf("EnsureRoots should create missing roots: %v", err)
	}
	if fi, err := os.Stat(fresh); err != nil || !fi.IsDir() {
		t.Fatalf("root %s was not created", fresh)
	}
	// A root that cannot be a directory must be reported, not silently
	// accepted: synchronization against it would be meaningless.
	if err := EnsureRoots([]string{blocker}); err == nil {
		t.Fatal("expected an error when a root is an existing regular file")
	}
}

func TestWaitContext(t *testing.T) {
	// It waits for the interval and returns early when the context is done,
	// which is how reconcile loops shut down promptly.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if WaitContext(ctx, time.Minute) {
		t.Fatal("WaitContext returned as if the interval had elapsed")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("WaitContext ignored cancellation")
	}
	if !WaitContext(context.Background(), 10*time.Millisecond) {
		t.Fatal("WaitContext did not report the elapsed interval")
	}
}

func TestSweepTemps(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "real.txt"), "keep me")
	stale := filepath.Join(root, TempPrefix+"stale")
	fresh := filepath.Join(root, "sub", TempPrefix+"fresh")
	writeFile(t, stale, "left over from a killed transfer")
	writeFile(t, fresh, "still in flight")

	old := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	n, err := SweepTemps(root, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("removed %d temps, want 1 (stale=%v fresh=%v)", n, fileExists(stale), fileExists(fresh))
	}
	if fileExists(stale) {
		t.Fatal("stale temporary still present")
	}
	// A transfer in progress must never be removed.
	if !fileExists(fresh) {
		t.Fatal("SweepTemps removed a temporary that is still in flight")
	}
	// Documents are never touched.
	if !fileExists(filepath.Join(root, "real.txt")) {
		t.Fatal("SweepTemps removed a real file")
	}
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

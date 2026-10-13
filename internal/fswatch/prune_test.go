package fswatch

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveEmptyDirRemovesOnlyEmptyDirectories(t *testing.T) {
	root := t.TempDir()
	mk := func(rel string) {
		if err := os.MkdirAll(filepath.Join(root, rel), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mk("empty")
	mk("hidden")
	if err := os.WriteFile(filepath.Join(root, "hidden/.DS_Store"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	mk("inside")
	if err := os.Symlink(filepath.Join(root, "inside"), filepath.Join(root, "inlink")); err != nil {
		t.Fatal(err)
	}

	if ok, err := RemoveEmptyDir(root, "empty"); !ok || err != nil {
		t.Fatalf("empty: %v, %v; want removed", ok, err)
	}
	for _, rel := range []string{"hidden", "file.txt", "inlink", "link", "", "."} {
		if ok, err := RemoveEmptyDir(root, rel); ok || err != nil {
			t.Fatalf("%q: %v, %v; want kept without error", rel, ok, err)
		}
	}
	if _, err := RemoveEmptyDir(root, "missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing: %v, want not-exist", err)
	}
	if ok, err := RemoveEmptyDir(root, "../escape"); ok || err == nil {
		t.Fatalf("../escape: %v, %v; want refused", ok, err)
	}
	for _, p := range []string{root, filepath.Join(root, "hidden/.DS_Store"), filepath.Join(root, "file.txt"), filepath.Join(root, "link"), target, filepath.Join(root, "inlink"), filepath.Join(root, "inside")} {
		if _, err := os.Lstat(p); err != nil {
			t.Fatalf("%s removed: %v", p, err)
		}
	}
}

func TestPruneEmptyParentsStopsAtFirstKeptDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "a/b/c/d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a/keep.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// d is already gone, as after an interrupted prune.
	if err := os.Remove(filepath.Join(root, "a/b/c/d")); err != nil {
		t.Fatal(err)
	}
	removed, err := PruneEmptyParents(root, "a/b/c/d/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 || removed[0] != "a/b/c" || removed[1] != "a/b" {
		t.Fatalf("removed %v, want [a/b/c a/b]", removed)
	}
	if _, err := os.Stat(filepath.Join(root, "a/keep.txt")); err != nil {
		t.Fatal(err)
	}
	if removed, err := PruneEmptyParents(root, "top.txt"); err != nil || len(removed) != 0 {
		t.Fatalf("top-level path: %v, %v", removed, err)
	}
}

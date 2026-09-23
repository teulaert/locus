package pathguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCleanRel(t *testing.T) {
	good := map[string]string{
		"notes.txt":    "notes.txt",
		"a/b.txt":      "a/b.txt",
		"./a/b.txt":    "a/b.txt",
		"a//b.txt":     "a/b.txt",
		"a/b/../c.txt": "a/c.txt",
		"dir/":         "dir",
	}
	for in, want := range good {
		got, err := CleanRel(in)
		if err != nil {
			t.Errorf("CleanRel(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("CleanRel(%q) = %q, want %q", in, got, want)
		}
	}

	bad := []string{
		"", "/", "..", "../etc/passwd", "a/../../b", "/etc/passwd", "a/../..",
		"C:\\evil", "a/./../../b", "x\x00y",
	}
	for _, in := range bad {
		if got, err := CleanRel(in); err == nil {
			t.Errorf("CleanRel(%q) = %q, want error", in, got)
		}
	}
}

func TestJoinStaysInsideRoot(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	full, err := Join(root, "sub/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(full, root) {
		t.Fatalf("escaped: %s", full)
	}
	for _, bad := range []string{"../escape", "sub/../../escape", "/etc/passwd"} {
		if _, err := Join(root, bad); err == nil {
			t.Errorf("Join(%q) accepted traversal", bad)
		}
	}
}

func TestJoinRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if _, err := Join(root, filepath.Join("link", "secret.txt")); err == nil {
		t.Fatal("symlink escape accepted")
	}
	inner := filepath.Join(root, "inner")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "ok.txt"), filepath.Join(inner, "ok")); err != nil {
		t.Fatal(err)
	}
	if _, err := Join(root, filepath.Join("inner", "ok")); err != nil {
		t.Fatalf("in-root symlink rejected: %v", err)
	}
}

func TestJoinAllowsNewFilesInNewDirs(t *testing.T) {
	root := t.TempDir()
	full, err := Join(root, "new/dir/file.txt")
	if err != nil {
		t.Fatalf("new nested file rejected: %v", err)
	}
	if filepath.Base(full) != "file.txt" {
		t.Fatalf("unexpected path %s", full)
	}
}

func TestRootsContains(t *testing.T) {
	a, b := "/srv/a", "/srv/b"
	if !RootsContains([]string{a, b}, filepath.Join(b, "x", "y")) {
		t.Fatal("expected containment")
	}
	if RootsContains([]string{a}, filepath.Join("/srv/ab", "x")) {
		t.Fatal("prefix-only match must fail")
	}
}

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

// LOC-3: os.Lstat follows intermediate symlinks, so a symlinked *directory*
// inside the root used to let existing files, subdirectories and new files
// beneath it through.
func TestJoinRejectsSymlinkedDirectoryEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outside, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"secret.txt", "sub/deep.txt"} {
		if err := os.WriteFile(filepath.Join(outside, filepath.FromSlash(f)), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(root, "linkdir")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	// A relative link that climbs out, nested below a real directory.
	if err := os.MkdirAll(filepath.Join(root, "real"), 0o755); err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(filepath.Join(root, "real"), outside)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(rel, filepath.Join(root, "real", "rel")); err != nil {
		t.Fatal(err)
	}
	// A dangling link whose target only escapes once ".." is applied after
	// the symlinked directory, not lexically.
	if err := os.Symlink("linkdir"+string(filepath.Separator)+".."+string(filepath.Separator)+"not-yet", filepath.Join(root, "sneaky")); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"linkdir",
		"linkdir/secret.txt",
		"linkdir/sub",
		"linkdir/sub/deep.txt",
		"linkdir/sub/newfile.txt",
		"linkdir/new/dir/file.txt",
		"real/rel/secret.txt",
		"real/rel/newfile.txt",
		"sneaky",
		"sneaky/child.txt",
	} {
		if got, err := Join(root, bad); err == nil {
			t.Errorf("Join(%q) = %q, want escape error", bad, got)
		}
	}
}

func TestJoinAllowsSymlinkedDirectoryInsideRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "target", "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "target", "sub", "f.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(root, "alias")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	// A dangling in-root link is fine too: a create would land inside.
	if err := os.Symlink("later.txt", filepath.Join(root, "pending")); err != nil {
		t.Fatal(err)
	}
	for _, ok := range []string{"alias/sub/f.txt", "alias/sub/new.txt", "alias/fresh/dir/x.txt", "pending"} {
		if _, err := Join(root, ok); err != nil {
			t.Errorf("Join(%q) rejected in-root symlink: %v", ok, err)
		}
	}
}

// The root itself may sit behind a symlink (e.g. /tmp on macOS); that alone
// must not make everything look like an escape.
func TestJoinThroughSymlinkedRoot(t *testing.T) {
	base := t.TempDir()
	realDir := filepath.Join(base, "real")
	if err := os.MkdirAll(filepath.Join(realDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "rootlink")
	if err := os.Symlink(realDir, root); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	for _, ok := range []string{"sub", "sub/new.txt", "new/file.txt"} {
		if _, err := Join(root, ok); err != nil {
			t.Errorf("Join(%q) under symlinked root rejected: %v", ok, err)
		}
	}
}

func TestJoinSymlinkLoop(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink("b", filepath.Join(root, "a")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if err := os.Symlink("a", filepath.Join(root, "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := Join(root, "a/x.txt"); err == nil {
		t.Fatal("symlink loop accepted")
	}
}

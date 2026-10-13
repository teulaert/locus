//go:build unix

package app

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/config"
)

// poseAs makes this process report uid as its effective user.
func poseAs(t *testing.T, uid int) {
	t.Helper()
	old := geteuid
	geteuid = func() int { return uid }
	t.Cleanup(func() { geteuid = old })
}

func TestCheckDataDirOwner(t *testing.T) {
	dir := t.TempDir()
	if err := CheckDataDirOwner(dir); err != nil {
		t.Fatalf("own directory refused: %v", err)
	}
	if err := CheckDataDirOwner(filepath.Join(dir, "missing")); err != nil {
		t.Fatalf("missing directory refused: %v", err)
	}
	poseAs(t, os.Geteuid()+1)
	err := CheckDataDirOwner(dir)
	var oe *DataDirOwnerError
	if !errors.As(err, &oe) || !errors.Is(err, ErrForeignDataDir) {
		t.Fatalf("foreign directory: got %v, want a DataDirOwnerError", err)
	}
	if oe.Owner.UID != os.Geteuid() || oe.Self.UID != os.Geteuid()+1 {
		t.Fatalf("owner %v, self %v", oe.Owner, oe.Self)
	}
	if !strings.Contains(err.Error(), dir) || !strings.Contains(err.Error(), "run it as that user") {
		t.Fatalf("message does not say what to do: %v", err)
	}
}

// A command run as another user (root, typically) against a service's data
// directory is refused before it creates databases, WAL files or a token
// there (LOC-64).
func TestNewRefusesDataDirOfAnotherUser(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "docs")
	data := filepath.Join(dir, "data")
	for _, d := range []string{root, data} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config.Default()
	cfg.Discovery.Gateways = new(bool) // never probe this machine's gateways (LOC-80)
	cfg.Roots = config.PathRoots(root)
	cfg.DataDir = data
	poseAs(t, os.Geteuid()+1)
	a, err := New(cfg, nil)
	if err == nil {
		a.Close()
		t.Fatal("New opened a data directory owned by another user")
	}
	if !errors.Is(err, ErrForeignDataDir) {
		t.Fatalf("got %v, want ErrForeignDataDir", err)
	}
	entries, _ := os.ReadDir(data)
	if len(entries) != 0 {
		t.Fatalf("refused New still created files: %v", entries)
	}
}

// When the lock file belongs to someone else, serve says whose it is and who
// it runs as, so the fix is obvious instead of a bare "permission denied".
func TestLockErrorNamesOwner(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root opens read-only files for writing")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, daemonLockName)
	if err := os.WriteFile(path, nil, 0o400); err != nil {
		t.Fatal(err)
	}
	// Our own file stays a plain permission error...
	_, err := lockDataDir(dir)
	if err == nil || strings.Contains(err.Error(), "is owned by") {
		t.Fatalf("own unwritable lock: %v", err)
	}
	// ...one owned by another user names both.
	me := os.Geteuid()
	poseAs(t, me+1)
	_, err = lockDataDir(dir)
	if err == nil {
		t.Fatal("unwritable lock file was opened")
	}
	want := "daemon.lock is owned by " + accountOf(me).String() + ", but locus runs as " + accountOf(me+1).String()
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err, want)
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error no longer wraps the permission error: %v", err)
	}
}

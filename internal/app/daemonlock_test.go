package app

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// Probing for a daemon must not create daemon.lock: a CLI run as another user
// (root) would leave a file the daemon cannot open (LOC-64).
func TestDaemonRunningDoesNotCreateLock(t *testing.T) {
	dir := t.TempDir()
	if DaemonRunning(dir) {
		t.Fatal("DaemonRunning reported a daemon in an empty directory")
	}
	if _, err := os.Stat(filepath.Join(dir, daemonLockName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("DaemonRunning created %s (stat: %v)", daemonLockName, err)
	}

	lock, err := lockDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !DaemonRunning(dir) {
		t.Fatal("DaemonRunning missed a held lock")
	}
	lock.Release()
	if DaemonRunning(dir) {
		t.Fatal("DaemonRunning reported a released lock as held")
	}
	// The probe must not have taken the lock for itself.
	again, err := lockDataDir(dir)
	if err != nil {
		t.Fatalf("lock after probing: %v", err)
	}
	again.Release()
}

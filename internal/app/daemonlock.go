package app

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// ErrDaemonRunning is returned when an operation needs exclusive use of a data
// directory that a running `locus serve` holds: starting a second daemon, or
// rebuilding the index under a daemon whose workers are writing it.
var ErrDaemonRunning = errors.New("a locus daemon is running on this data directory")

// daemonLockName is the file in the data directory that `locus serve` holds an
// exclusive advisory lock on (flock on Unix, LockFileEx on Windows) for as
// long as it runs. The operating system drops the lock when the process
// exits, however it exits, so a crash never leaves a stale lock behind; the
// file itself is left in place and carries no meaning when unlocked.
const daemonLockName = "daemon.lock"

// dataDirLock is an exclusive lock on a data directory.
type dataDirLock struct{ f *os.File }

// lockDataDir takes the data directory's daemon lock without waiting. It
// returns an error wrapping ErrDaemonRunning when another holder (in this
// process or another) has it.
func lockDataDir(dir string) (*dataDirLock, error) {
	path := filepath.Join(dir, daemonLockName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open daemon lock: %w", lockOwnerHint(path, err))
	}
	held, err := tryLockFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	if !held {
		f.Close()
		return nil, fmt.Errorf("%w (%s); stop `locus serve` first", ErrDaemonRunning, dir)
	}
	return &dataDirLock{f: f}, nil
}

// lockOwnerHint explains a lock file this process may not open: almost
// always one created by another user, such as root running a CLI command
// against a service's data directory (LOC-64).
func lockOwnerHint(path string, err error) error {
	if !errors.Is(err, fs.ErrPermission) {
		return err
	}
	owner, ok := fileOwner(path)
	if !ok {
		return err
	}
	self := currentAccount()
	if owner.UID == self.UID {
		return err
	}
	return fmt.Errorf("%w; %s is owned by %s, but locus runs as %s: remove the file (it is recreated on start) or chown it to %s",
		err, daemonLockName, owner, self, self)
}

// DaemonRunning reports whether a `locus serve` holds dir's daemon lock. It
// probes by taking the lock and releasing it at once, so a daemon starting in
// that instant could see it held; the CLI only uses this to choose where to
// read status from, and the window is a single system call wide.
//
// It never creates the lock file: a missing file means no daemon has run
// there, and creating it would leave it owned by whoever ran the CLI, which a
// daemon running as another user then cannot open (LOC-64). The file is
// opened read-only, which is all an advisory lock needs.
func DaemonRunning(dir string) bool {
	f, err := os.Open(filepath.Join(dir, daemonLockName))
	if err != nil {
		return false
	}
	defer f.Close()
	held, err := tryLockFile(f)
	if err != nil {
		return false
	}
	if !held {
		return true
	}
	_ = unlockFile(f)
	return false
}

// Release drops the lock.
func (l *dataDirLock) Release() {
	if l == nil || l.f == nil {
		return
	}
	_ = unlockFile(l.f)
	_ = l.f.Close()
	l.f = nil
}

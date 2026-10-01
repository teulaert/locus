package app

import (
	"errors"
	"fmt"
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
		return nil, fmt.Errorf("open daemon lock: %w", err)
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

// DaemonRunning reports whether a `locus serve` holds dir's daemon lock. It
// probes by taking the lock and releasing it at once, so a daemon starting in
// that instant could see it held; the CLI only uses this to choose where to
// read status from, and the window is a single system call wide.
func DaemonRunning(dir string) bool {
	lock, err := lockDataDir(dir)
	if err != nil {
		return errors.Is(err, ErrDaemonRunning)
	}
	lock.Release()
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

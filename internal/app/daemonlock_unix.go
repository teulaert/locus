//go:build unix

package app

import (
	"errors"
	"os"
	"syscall"
)

// tryLockFile takes an exclusive flock without blocking. flock locks belong
// to the open file description, so a second open of the same file conflicts
// even within one process.
func tryLockFile(f *os.File) (bool, error) {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	}
	return err == nil, err
}

func unlockFile(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }

package update

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// ResolveExecutable is the running binary's path with symlinks followed,
// so a symlink such as /usr/local/bin/locus -> /var/lib/locus/bin/locus
// replaces the file it points to and stays a symlink.
func ResolveExecutable() (string, error) {
	p, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(p)
}

// StagePath is where a downloaded binary is written before the swap: next
// to exe, so the swap is a rename within one directory and filesystem.
func StagePath(exe string) string { return exe + ".new" }

// PreviousPath is where the binary a swap replaced is kept for a rollback.
func PreviousPath(exe string) string { return exe + ".previous" }

// ErrNoPrevious is a rollback with no previous binary to return to.
var ErrNoPrevious = errors.New("no previous binary to roll back to")

// Swap installs StagePath(exe) as exe and keeps the binary it replaces as
// PreviousPath(exe), replacing an older one.
//
// On Unix exe never goes missing: the current binary is first hard-linked
// (or copied, where links are not possible) to .previous, and the staged
// one is then renamed over exe, which is atomic. A crash at any point
// leaves either the old or the new binary at exe. The running process is
// unaffected; it runs the new binary once it re-executes.
func Swap(exe string) error {
	stage, prev := StagePath(exe), PreviousPath(exe)
	if _, err := os.Stat(stage); err != nil {
		return fmt.Errorf("swap: staged binary: %w", err)
	}
	if runtime.GOOS == "windows" {
		// A running executable cannot be replaced on Windows, only renamed.
		_ = os.Remove(prev)
		if err := os.Rename(exe, prev); err != nil {
			return fmt.Errorf("swap: keep previous: %w", err)
		}
		if err := os.Rename(stage, exe); err != nil {
			_ = os.Rename(prev, exe)
			return fmt.Errorf("swap: install: %w", err)
		}
		return nil
	}
	if err := keepCopy(exe, prev); err != nil {
		return fmt.Errorf("swap: keep previous: %w", err)
	}
	if err := os.Rename(stage, exe); err != nil {
		return fmt.Errorf("swap: install: %w", err)
	}
	return syncDir(filepath.Dir(exe))
}

// Rollback exchanges exe and PreviousPath(exe): the previous binary is
// installed and the one it replaces becomes .previous, so a second rollback
// undoes the first. exe never goes missing on Unix.
func Rollback(exe string) error {
	prev := PreviousPath(exe)
	if _, err := os.Stat(prev); errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w (%s)", ErrNoPrevious, prev)
	} else if err != nil {
		return err
	}
	hold := exe + ".rollback"
	if runtime.GOOS == "windows" {
		_ = os.Remove(hold)
		if err := os.Rename(exe, hold); err != nil {
			return fmt.Errorf("rollback: %w", err)
		}
		if err := os.Rename(prev, exe); err != nil {
			_ = os.Rename(hold, exe)
			return fmt.Errorf("rollback: %w", err)
		}
		return os.Rename(hold, prev)
	}
	if err := keepCopy(exe, hold); err != nil {
		return fmt.Errorf("rollback: %w", err)
	}
	if err := os.Rename(prev, exe); err != nil {
		_ = os.Remove(hold)
		return fmt.Errorf("rollback: %w", err)
	}
	if err := os.Rename(hold, prev); err != nil {
		return fmt.Errorf("rollback: keep replaced binary: %w", err)
	}
	return syncDir(filepath.Dir(exe))
}

// keepCopy makes dst a copy of src, replacing dst atomically: a hard link
// where the filesystem allows one, a copy otherwise.
func keepCopy(src, dst string) error {
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)
	if err := os.Link(src, tmp); err != nil {
		if err := copyFile(src, tmp); err != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fi.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// syncDir makes the renames in dir durable. Platforms that cannot sync a
// directory are not an error.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer d.Close()
	_ = d.Sync()
	return nil
}

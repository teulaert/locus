package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/pathguard"
)

// durability is the pair of flushes that make an install survive a machine
// crash, not just a killed process: file flushes the temporary file's data
// before it is renamed, dir flushes the directory entry the rename created.
// Tests swap it to observe the ordering or to simulate a failing disk.
type durability struct {
	file func(*os.File) error
	dir  func(string) error
}

var installSync = durability{file: (*os.File).Sync, dir: syncDir}

// syncDir fsyncs a directory so a rename inside it is durable. Windows cannot
// open a directory for flushing, and some filesystems reject fsync on a
// directory (EINVAL); neither is a failure of the install.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, errors.ErrUnsupported) {
		return nil
	}
	return err
}

// Install writes r to root/relpath atomically and durably: the bytes land in
// a temporary file first, the content hash is verified, the file is fsynced,
// renamed into place, and the parent directory is fsynced. Callers record the
// hash only after Install returns, so without the flushes a machine crash
// could leave a truncated file at the path while state.db claims the verified
// hash (LOC-16). A partial file never becomes visible as a valid document.
func Install(root, relpath string, r io.Reader, wantHash string, maxBytes int64) (size int64, gotHash string, err error) {
	full, err := pathguard.Join(root, relpath)
	if err != nil {
		return 0, "", err
	}
	dir := filepath.Dir(full)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, "", err
	}
	tmp, err := os.CreateTemp(dir, fswatch.TempPrefix+"*")
	if err != nil {
		return 0, "", err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		tmp.Close()
		os.Remove(tmpName)
	}
	h := sha256.New()
	var n int64
	if maxBytes <= 0 {
		maxBytes = 1 << 62
	}
	n, err = io.Copy(io.MultiWriter(tmp, h), io.LimitReader(r, maxBytes+1))
	if err != nil {
		cleanup()
		return 0, "", err
	}
	if n > maxBytes {
		cleanup()
		return 0, "", fmt.Errorf("sync: transfer exceeds size limit %d", maxBytes)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if wantHash != "" && sum != wantHash {
		cleanup()
		return 0, "", fmt.Errorf("sync: hash mismatch installing %s: want %s got %s", relpath, wantHash, sum)
	}
	_ = tmp.Chmod(0o644)
	if err := installSync.file(tmp); err != nil {
		cleanup()
		return 0, "", fmt.Errorf("sync: flushing %s: %w", relpath, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return 0, "", err
	}
	if err := os.Rename(tmpName, full); err != nil {
		os.Remove(tmpName)
		return 0, "", err
	}
	if err := installSync.dir(dir); err != nil {
		return 0, "", fmt.Errorf("sync: flushing directory of %s: %w", relpath, err)
	}
	return n, sum, nil
}

// ErrPartialTransfer signals an interrupted download.
var ErrPartialTransfer = errors.New("sync: incomplete transfer")

// DownloadTo installs content from r into root/relpath via a temp file.
func DownloadTo(root, relpath string, r io.Reader, wantHash string, maxBytes int64) error {
	_, _, err := Install(root, relpath, r, wantHash, maxBytes)
	return err
}

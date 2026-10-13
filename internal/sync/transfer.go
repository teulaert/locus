package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	return InstallStream(root, relpath, r, wantHash, -1, maxBytes)
}

// installStaged moves verified staged content to full, reporting whether it
// did. It reports false, with no error, when the rename is not possible
// (the staging directory is on another filesystem), and the caller copies
// the content instead.
func installStaged(st *Staged, full, wantHash string, wantSize, maxBytes int64) (bool, error) {
	switch {
	case wantHash != "" && st.Hash != wantHash:
		return false, fmt.Errorf("%w: hash mismatch installing %s: want %s got %s", ErrContentMismatch, full, wantHash, st.Hash)
	case wantSize >= 0 && st.Size != wantSize:
		return false, fmt.Errorf("%w: %s: expected %d bytes, have %d", ErrContentMismatch, full, wantSize, st.Size)
	case maxBytes > 0 && st.Size > maxBytes:
		return false, fmt.Errorf("%w %d", ErrTooLarge, maxBytes)
	}
	_ = st.f.Chmod(0o644)
	if err := installSync.file(st.f); err != nil {
		return false, fmt.Errorf("sync: flushing %s: %w", full, err)
	}
	err := os.Rename(st.path, full)
	if errors.Is(err, fs.ErrNotExist) {
		// The directory was pruned after InstallStream created it.
		if merr := os.MkdirAll(filepath.Dir(full), 0o755); merr == nil {
			err = os.Rename(st.path, full)
		}
	}
	if err != nil {
		if _, serr := st.f.Seek(0, io.SeekStart); serr != nil {
			return false, serr
		}
		return false, nil
	}
	st.moved = true
	if err := installSync.dir(filepath.Dir(full)); err != nil {
		return true, fmt.Errorf("sync: flushing directory of %s: %w", full, err)
	}
	return true, nil
}

// Errors InstallStream reports when the streamed content is refused. Nothing
// is installed in either case and the temporary file is removed.
var (
	// ErrTooLarge: the content exceeds maxBytes.
	ErrTooLarge = errors.New("sync: transfer exceeds size limit")
	// ErrContentMismatch: the content does not have the expected hash or
	// size.
	ErrContentMismatch = errors.New("sync: content does not match")
)

// InstallStream is Install for content of any size: r is copied to the
// temporary file in fixed-size chunks while it is hashed, so memory use does
// not depend on the size of the content (LOC-13). Reading stops one byte past
// maxBytes (unlimited when maxBytes <= 0). wantHash, when non-empty, and
// wantSize, when not negative, are checked against what was streamed before
// the rename, so content that is truncated, oversized or tampered with never
// reaches relpath.
//
// Content already staged and verified outside the root (*Staged) is renamed
// into place when it can be, so nothing is written next to the user's
// documents and the install takes no longer than a rename and two flushes.
func InstallStream(root, relpath string, r io.Reader, wantHash string, wantSize, maxBytes int64) (size int64, gotHash string, err error) {
	full, err := pathguard.Join(root, relpath)
	if err != nil {
		return 0, "", err
	}
	dir := filepath.Dir(full)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, "", err
	}
	if st, ok := r.(*Staged); ok && !st.moved {
		if done, err := installStaged(st, full, wantHash, wantSize, maxBytes); done || err != nil {
			return st.Size, st.Hash, err
		}
		// Another filesystem: copy it through a temporary file below.
	}
	tmp, err := os.CreateTemp(dir, fswatch.TempPrefix+"*")
	if errors.Is(err, fs.ErrNotExist) {
		// Emptied directories are pruned (LOC-74): one removed between
		// MkdirAll and here is simply created again. Once the temporary
		// file exists the directory is not empty and cannot be pruned.
		if err = os.MkdirAll(dir, 0o755); err == nil {
			tmp, err = os.CreateTemp(dir, fswatch.TempPrefix+"*")
		}
	}
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
	// Content longer than expected is refused as soon as one byte too many
	// has arrived; there is no point streaming the rest to disk.
	readMax := maxBytes
	if wantSize >= 0 && wantSize < readMax {
		readMax = wantSize
	}
	n, err = io.Copy(io.MultiWriter(tmp, h), io.LimitReader(r, readMax+1))
	if err != nil {
		cleanup()
		return 0, "", err
	}
	if n > maxBytes {
		cleanup()
		return 0, "", fmt.Errorf("%w %d", ErrTooLarge, maxBytes)
	}
	if wantSize >= 0 && n != wantSize {
		cleanup()
		return 0, "", fmt.Errorf("%w: %s: expected %d bytes, read %d", ErrContentMismatch, relpath, wantSize, n)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if wantHash != "" && sum != wantHash {
		cleanup()
		return 0, "", fmt.Errorf("%w: hash mismatch installing %s: want %s got %s", ErrContentMismatch, relpath, wantHash, sum)
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

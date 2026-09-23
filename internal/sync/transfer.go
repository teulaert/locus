package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"locus/internal/fswatch"
	"locus/internal/pathguard"
)

// Install writes r to root/relpath atomically: the bytes land in a temporary
// file first, the content hash is verified, and only then is the file renamed
// into place. A partial file never becomes visible as a valid document.
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
	if err := tmp.Close(); err != nil {
		cleanup()
		return 0, "", err
	}
	if err := os.Rename(tmpName, full); err != nil {
		os.Remove(tmpName)
		return 0, "", err
	}
	_ = os.Chmod(full, 0o644)
	return n, sum, nil
}

// ErrPartialTransfer signals an interrupted download.
var ErrPartialTransfer = errors.New("sync: incomplete transfer")

// DownloadTo installs content from r into root/relpath via a temp file.
func DownloadTo(root, relpath string, r io.Reader, wantHash string, maxBytes int64) error {
	_, _, err := Install(root, relpath, r, wantHash, maxBytes)
	return err
}

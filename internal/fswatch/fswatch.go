// Package fswatch inventories synchronization roots and watches them for
// changes. Watcher events are hints only: correctness always comes from the
// periodic reconciliation scan.
package fswatch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"strings"

	"github.com/teulaert/locus/internal/pathguard"
)

// TempPrefix is the name prefix used for files that are still being written
// or transferred. They are internal: never inventoried, never synchronized,
// never indexed, so a partial file can never be mistaken for a document.
const TempPrefix = ".locus-tmp-"

// IsInternal reports whether a base name belongs to Locus itself.
func IsInternal(name string) bool { return strings.HasPrefix(name, TempPrefix) }

// Entry is one file discovered under a root.
type Entry struct {
	RelPath string
	Size    int64
	Hash    string
	// Stat is the fingerprint taken before the content was hashed (or the
	// hash was reused), so a write racing the scan changes it.
	Stat Stat
	// Hashed is true when the content was read on this scan, false when
	// the hash came from Known.
	Hashed bool
}

// Known supplies a previously computed hash for a file whose current
// fingerprint is st, or ok=false when the file must be hashed. It decides
// whether the fingerprint is trustworthy; any doubt must answer false.
type Known func(relpath string, st Stat) (hash string, ok bool)

// ScanOptions tunes Scan. The zero value is the default inventory.
type ScanOptions struct {
	// Known vouches for previously computed hashes; nil hashes every file.
	Known Known
	// Hidden includes hidden paths: files and directories whose name
	// starts with "." below the root (.git, .obsidian, .DS_Store). By
	// default they are skipped, and a hidden directory is not descended
	// into at all, so .git/objects is never walked.
	Hidden bool
	// Context, when set, ends the scan early with its error: a pass over
	// a large tree, or the hashing of a large file, does not hold up a
	// shutdown (LOC-81).
	Context context.Context
}

// skipHidden reports whether path, found under root while walking, is hidden
// and must be left out. The root itself is never hidden, whatever its name.
func skipHidden(root, path string, d fs.DirEntry, hidden bool) bool {
	return !hidden && path != root && pathguard.IsHiddenName(d.Name())
}

// Scan walks root and returns every regular file. Symlinks, sockets and
// other special files are ignored, so a symlink can never smuggle a path
// outside the root into the inventory. Hidden paths are skipped unless
// opts.Hidden is set. A file is hashed unless opts.Known vouches for its
// fingerprint, so an unchanged tree is inventoried from metadata alone.
func Scan(root string, opts ScanOptions) ([]Entry, error) {
	root = filepath.Clean(root)
	known := opts.Known
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	var out []Entry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if skipHidden(root, path, d, opts.Hidden) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil
		}
		if IsInternal(d.Name()) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		st := StatOf(info)
		if known != nil {
			if hash, ok := known(rel, st); ok {
				out = append(out, Entry{RelPath: rel, Size: st.Size, Hash: hash, Stat: st})
				return nil
			}
		}
		hash, size, err := hashFile(ctx, path)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			return nil
		}
		out = append(out, Entry{RelPath: rel, Size: size, Hash: hash, Stat: st, Hashed: true})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RelPath < out[j].RelPath })
	return out, nil
}

// HashFile returns the hex SHA-256 of a file and its size.
func HashFile(path string) (string, int64, error) {
	return hashFile(context.Background(), path)
}

// hashFile is HashFile ending early, with ctx's error, once ctx is done.
func hashFile(ctx context.Context, path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	var r io.Reader = f
	if ctx.Done() != nil {
		r = ctxReader{ctx: ctx, r: f}
	}
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// ctxReader fails its reads once ctx is done. A file read never blocks
// for long, so checking between reads is prompt enough.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// HashBytes returns the hex SHA-256 of b.
func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// SweepTemps removes transfer temporary files that are older than the given
// age. A crash or SIGKILL between writing a temp file and renaming it into
// place leaves the temp behind forever; they are excluded from the inventory,
// so they would slowly fill the disk. Fresh temps are left alone: a transfer
// in progress must never be touched. Hidden directories are only searched
// when hidden is set: nothing is transferred into them otherwise.
func SweepTemps(root string, olderThan time.Duration, hidden bool) (int, error) {
	if olderThan <= 0 {
		olderThan = time.Hour
	}
	root = filepath.Clean(root)
	cutoff := time.Now().Add(-olderThan)
	removed := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() && skipHidden(root, path, d, hidden) {
			return fs.SkipDir
		}
		if d.IsDir() || !IsInternal(d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.ModTime().After(cutoff) {
			return nil
		}
		if err := os.Remove(path); err == nil {
			removed++
		}
		return nil
	})
	if err != nil {
		return removed, err
	}
	return removed, nil
}

// WaitContext is a small helper for reconcile loops.
func WaitContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ErrNoRoots is returned when a service is configured without roots.
var ErrNoRoots = errors.New("fswatch: no roots configured")

// EnsureRoots creates roots when missing and verifies they are directories.
func EnsureRoots(roots []string) error {
	if len(roots) == 0 {
		return ErrNoRoots
	}
	for _, r := range roots {
		st, err := os.Stat(r)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(r, 0o755); err != nil {
				return fmt.Errorf("create root %s: %w", r, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if !st.IsDir() {
			return fmt.Errorf("root %s is not a directory", r)
		}
	}
	return nil
}

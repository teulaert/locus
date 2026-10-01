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
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"strings"

	"github.com/fsnotify/fsnotify"

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
	var out []Entry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
		hash, size, err := HashFile(path)
		if err != nil {
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
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
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

// Watcher reports filesystem changes below a set of roots. Events are
// coalesced with a trailing debounce: the callback is invoked once activity
// has been quiet for Debounce, so a burst of intermediate states produces a
// single reconciliation pass. A burst that never settles is still reported
// after MaxDelay, so continuous writes cannot postpone reconciliation forever.
//
// Hidden directories are not watched, and events for hidden paths are
// ignored, unless SyncHidden enables them for their root.
type Watcher struct {
	Roots []string
	// SyncHidden marks the roots (by path, as listed in Roots) whose
	// hidden paths are watched too. Nil watches no hidden paths.
	SyncHidden map[string]bool
	Debounce   time.Duration
	// MaxDelay caps how long a continuous burst may postpone the callback.
	MaxDelay time.Duration
	Notify   func(reason string)
	// Log receives watcher errors at debug level; nil uses slog.Default.
	Log *slog.Logger

	w    *fsnotify.Watcher
	stop chan struct{}
	done chan struct{}
}

// NewWatcher creates a watcher. Notify is called from the watcher's own
// goroutine, never concurrently with itself.
func NewWatcher(roots []string, debounce time.Duration, notify func(string)) *Watcher {
	if debounce <= 0 {
		debounce = 300 * time.Millisecond
	}
	return &Watcher{Roots: roots, Debounce: debounce, MaxDelay: 10 * debounce, Notify: notify, stop: make(chan struct{}), done: make(chan struct{})}
}

// Start begins watching. Missing roots are tolerated and picked up on the
// next reconcile pass.
func (w *Watcher) Start() error {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	w.w = fw
	for _, r := range w.Roots {
		w.addRecursive(r)
	}
	go w.loop()
	return nil
}

// addRecursive registers dir and every directory below it, except hidden
// ones on a root that does not sync them.
func (w *Watcher) addRecursive(dir string) {
	dir = filepath.Clean(dir)
	root, ok := w.rootOf(dir)
	if !ok {
		return
	}
	hidden := w.SyncHidden[root]
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if skipHidden(root, path, d, hidden) {
			return fs.SkipDir
		}
		_ = w.w.Add(path)
		return nil
	})
}

// rootOf returns the configured root that path lies in (or is).
func (w *Watcher) rootOf(path string) (string, bool) {
	for _, r := range w.Roots {
		r = filepath.Clean(r)
		if path == r || pathguard.Contains(r, path) {
			return r, true
		}
	}
	return "", false
}

// ignored reports whether an event for path concerns only state the watcher
// does not track: a hidden path on a root that does not sync hidden ones
// (an editor's .swp file, .DS_Store), or a path outside every root.
func (w *Watcher) ignored(path string) bool {
	path = filepath.Clean(path)
	root, ok := w.rootOf(path)
	if !ok {
		return true
	}
	if w.SyncHidden[root] || path == root {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return pathguard.HasHidden(rel)
}

// loop owns all debounce state (the timer, whether a notification is pending
// and when the current burst began). The timer delivers on a channel this
// goroutine reads, so no state is shared with another goroutine.
func (w *Watcher) loop() {
	defer close(w.done)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	var (
		pending    bool
		burstStart time.Time
	)
	fire := func(reason string) {
		timer.Stop()
		pending = false
		if w.Notify != nil {
			w.Notify(reason)
		}
	}
	for {
		select {
		case <-w.stop:
			if pending {
				fire("shutdown-flush")
			}
			return
		case <-timer.C:
			if pending {
				fire("watcher")
			}
		case ev, ok := <-w.w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			if w.ignored(ev.Name) {
				continue
			}
			if ev.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
				if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
					w.addRecursive(ev.Name)
				}
			}
			now := time.Now()
			if !pending {
				pending = true
				burstStart = now
			}
			// Trailing debounce: every event pushes the deadline out, but
			// never beyond MaxDelay after the burst began.
			delay := w.Debounce
			if w.MaxDelay > 0 {
				if left := burstStart.Add(w.MaxDelay).Sub(now); left < delay {
					delay = max(left, 0)
				}
			}
			timer.Reset(delay)
		case err, ok := <-w.w.Errors:
			if !ok {
				return
			}
			// The kernel dropped events, so what changed is unknown: that is
			// exactly when a full reconciliation is needed.
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				fire("overflow")
				continue
			}
			// Other errors are hints lost, not correctness: the periodic
			// reconciliation still covers whatever they hid.
			log := w.Log
			if log == nil {
				log = slog.Default()
			}
			log.Debug("filesystem watcher error", "err", err)
		}
	}
}

// Stop terminates the watcher.
func (w *Watcher) Stop() {
	close(w.stop)
	<-w.done
	w.w.Close()
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

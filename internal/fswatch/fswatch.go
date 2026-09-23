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

	"github.com/fsnotify/fsnotify"
	"strings"
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
}

// Scan walks root and returns every regular file. Symlinks, sockets and
// other special files are ignored, so a symlink can never smuggle a path
// outside the root into the inventory.
func Scan(root string) ([]Entry, error) {
	root = filepath.Clean(root)
	var out []Entry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
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
		hash, size, err := HashFile(path)
		if err != nil {
			return nil
		}
		out = append(out, Entry{RelPath: filepath.ToSlash(rel), Size: size, Hash: hash})
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
// in progress must never be touched.
func SweepTemps(root string, olderThan time.Duration) (int, error) {
	if olderThan <= 0 {
		olderThan = time.Hour
	}
	cutoff := time.Now().Add(-olderThan)
	removed := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrPermission) || errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
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
// coalesced: the callback is invoked when activity settles, so a burst of
// intermediate states produces a single reconciliation pass.
type Watcher struct {
	Roots    []string
	Debounce time.Duration
	Notify   func(reason string)

	w        *fsnotify.Watcher
	lastFire time.Time
	pending  bool
	stop     chan struct{}
	done     chan struct{}
}

// NewWatcher creates a watcher; Notify is called from a background goroutine.
func NewWatcher(roots []string, debounce time.Duration, notify func(string)) *Watcher {
	if debounce <= 0 {
		debounce = 300 * time.Millisecond
	}
	return &Watcher{Roots: roots, Debounce: debounce, Notify: notify, stop: make(chan struct{}), done: make(chan struct{})}
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

func (w *Watcher) addRecursive(root string) {
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			_ = w.w.Add(path)
		}
		return nil
	})
}

func (w *Watcher) loop() {
	defer close(w.done)
	var timer *time.Timer
	fire := func(reason string) {
		w.lastFire = time.Now()
		if w.Notify != nil {
			w.Notify(reason)
		}
	}
	for {
		select {
		case <-w.stop:
			if timer != nil {
				timer.Stop()
			}
			if w.pending {
				w.pending = false
				fire("shutdown-flush")
			}
			return
		case ev, ok := <-w.w.Events:
			if !ok {
				return
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			if ev.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
				if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
					_ = w.w.Add(ev.Name)
					w.addRecursive(ev.Name)
				}
			}
			w.pending = true
			if timer == nil {
				timer = time.AfterFunc(w.Debounce, func() {
					w.pending = false
					fire("watcher")
				})
			}
		case err, ok := <-w.w.Errors:
			if !ok {
				return
			}
			_ = err
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

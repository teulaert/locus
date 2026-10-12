//go:build !darwin && !dragonfly && !freebsd && !netbsd && !openbsd

package fswatch

import (
	"path/filepath"
	"runtime"

	"github.com/fsnotify/fsnotify"
)

// fsnotifyBackend wraps fsnotify where its backend holds no descriptor per
// watched file: inotify on Linux, ReadDirectoryChangesW on Windows, FEN on
// illumos. (fsnotify's kqueue backend opens every file in a watched
// directory, so the kqueue platforms use kqueueDirs instead.)
type fsnotifyBackend struct{ w *fsnotify.Watcher }

func newPlatformBackend() (backend, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &fsnotifyBackend{w: w}, nil
}

func (b *fsnotifyBackend) add(dir string) error { return b.w.Add(dir) }

// watching scans the watch list; only dirs-only backends need it, so it is
// not on any hot path here.
func (b *fsnotifyBackend) watching(dir string) bool {
	dir = filepath.Clean(dir)
	for _, p := range b.w.WatchList() {
		if p == dir {
			return true
		}
	}
	return false
}

func (b *fsnotifyBackend) count() int                    { return len(b.w.WatchList()) }
func (b *fsnotifyBackend) list() []string                { return b.w.WatchList() }
func (b *fsnotifyBackend) events() <-chan fsnotify.Event { return b.w.Events }
func (b *fsnotifyBackend) errors() <-chan error          { return b.w.Errors }
func (b *fsnotifyBackend) close() error                  { return b.w.Close() }

func (b *fsnotifyBackend) traits() backendTraits {
	mode := "fsnotify"
	switch runtime.GOOS {
	case "linux":
		mode = ModeInotify
	case "windows":
		mode = "readdirectorychanges"
	case "illumos", "solaris":
		mode = "fen"
	}
	return backendTraits{mode: mode}
}

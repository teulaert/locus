//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package fswatch

import (
	"errors"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sys/unix"

	"github.com/teulaert/locus/internal/pathguard"
)

// kqueueDirs watches directories with kqueue, and nothing else.
//
// kqueue can only watch a vnode through an open descriptor of it. fsnotify's
// kqueue backend (the only one it has on macOS and the BSDs) therefore opens
// every file inside each watched directory as well, to report writes to it
// the way inotify does, so a root with N files holds N descriptors and a
// large one runs into kern.maxfilesperproc (LOC-70). That cannot be switched
// off, and FSEvents, which has no such cost, needs cgo.
//
// kqueueDirs registers EVFILT_VNODE on each directory only, so descriptors
// scale with the number of directories. A directory's vnode reports
// NOTE_WRITE when an entry in it is created, removed or renamed, which covers
// new files, deletions and the write-temporary-then-rename saves most
// editors and sync tools do. What it cannot see is a write to an existing
// file in place (an append, or an editor that truncates and rewrites): such a
// change is found by the periodic reconciliation, within reconcile_interval,
// instead of within the debounce.
//
// Events name the directory, not the entry: a change in a directory is a
// Write on it. The Watcher reacts by reconciling and by looking for new
// subdirectories to watch (backendTraits.dirsOnly).
type kqueueDirs struct {
	kq int
	// closepipe wakes the reader when the backend is closed: its read
	// end is registered with kq, and closing the write end makes it ready.
	closepipe [2]int
	evs       chan fsnotify.Event
	errs      chan error
	done      chan struct{}
	closeOnce sync.Once

	mu     sync.Mutex
	closed bool
	byFD   map[int]string
	byPath map[string]int
}

// vnodeNotes are the directory changes kqueueDirs listens for. NOTE_EXTEND
// accompanies NOTE_WRITE on some file systems when a directory grows.
const vnodeNotes = unix.NOTE_WRITE | unix.NOTE_EXTEND | unix.NOTE_DELETE | unix.NOTE_RENAME | unix.NOTE_REVOKE

var errBackendClosed = errors.New("fswatch: watcher closed")

func newPlatformBackend() (backend, error) { return newKqueueDirs() }

func newKqueueDirs() (*kqueueDirs, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(kq)
	var p [2]int
	if err := unix.Pipe(p[:]); err != nil {
		unix.Close(kq)
		return nil, err
	}
	unix.CloseOnExec(p[0])
	unix.CloseOnExec(p[1])
	var ch [1]unix.Kevent_t
	unix.SetKevent(&ch[0], p[0], unix.EVFILT_READ, unix.EV_ADD|unix.EV_ENABLE|unix.EV_ONESHOT)
	if _, err := unix.Kevent(kq, ch[:], nil, nil); err != nil {
		unix.Close(kq)
		unix.Close(p[0])
		unix.Close(p[1])
		return nil, err
	}
	k := &kqueueDirs{
		kq:        kq,
		closepipe: p,
		evs:       make(chan fsnotify.Event),
		errs:      make(chan error),
		done:      make(chan struct{}),
		byFD:      make(map[int]string),
		byPath:    make(map[string]int),
	}
	go k.read()
	return k, nil
}

func (k *kqueueDirs) traits() backendTraits {
	return backendTraits{mode: ModeKqueueDirs, descriptors: true, dirsOnly: true}
}

// add opens dir for event notification only (O_EVTONLY on macOS, so the
// open does not keep the volume from unmounting) and registers it. A symlink
// is never followed: the scan does not follow them either.
func (k *kqueueDirs) add(dir string) error {
	dir = filepath.Clean(dir)
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return errBackendClosed
	}
	if _, ok := k.byPath[dir]; ok {
		return nil
	}
	var fd int
	var err error
	for {
		fd, err = unix.Open(dir, dirOpenMode, 0)
		if err != unix.EINTR {
			break
		}
	}
	if err != nil {
		return err
	}
	var ch [1]unix.Kevent_t
	unix.SetKevent(&ch[0], fd, unix.EVFILT_VNODE, unix.EV_ADD|unix.EV_CLEAR|unix.EV_ENABLE)
	ch[0].Fflags = vnodeNotes
	if _, err := unix.Kevent(k.kq, ch[:], nil, nil); err != nil {
		unix.Close(fd)
		return err
	}
	k.byFD[fd] = dir
	k.byPath[dir] = fd
	return nil
}

func (k *kqueueDirs) watching(dir string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	_, ok := k.byPath[filepath.Clean(dir)]
	return ok
}

func (k *kqueueDirs) count() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.byPath)
}

func (k *kqueueDirs) list() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]string, 0, len(k.byPath))
	for p := range k.byPath {
		out = append(out, p)
	}
	return out
}

func (k *kqueueDirs) events() <-chan fsnotify.Event { return k.evs }
func (k *kqueueDirs) errors() <-chan error          { return k.errs }

// close releases every directory descriptor and stops the reader, which
// closes the channels.
func (k *kqueueDirs) close() error {
	k.closeOnce.Do(func() {
		k.mu.Lock()
		k.closed = true
		for fd := range k.byFD {
			unix.Close(fd)
		}
		clear(k.byFD)
		clear(k.byPath)
		k.mu.Unlock()
		close(k.done)
		unix.Close(k.closepipe[1])
	})
	return nil
}

// forgetLocked drops the watch on path. With subtree it drops every watch
// below it too: after a rename their descriptors still work but report
// paths that no longer exist, and the new location is found and watched
// again through its parent's NOTE_WRITE.
func (k *kqueueDirs) forgetLocked(path string, subtree bool) {
	if fd, ok := k.byPath[path]; ok {
		unix.Close(fd)
		delete(k.byPath, path)
		delete(k.byFD, fd)
	}
	if !subtree {
		return
	}
	for p, fd := range k.byPath {
		if pathguard.Contains(path, p) {
			unix.Close(fd)
			delete(k.byPath, p)
			delete(k.byFD, fd)
		}
	}
}

func (k *kqueueDirs) send(ev fsnotify.Event) bool {
	select {
	case k.evs <- ev:
		return true
	case <-k.done:
		return false
	}
}

// read turns kevents into events until the backend is closed or kevent
// fails, closing the channels on the way out.
func (k *kqueueDirs) read() {
	defer func() {
		close(k.evs)
		close(k.errs)
		unix.Close(k.kq)
		unix.Close(k.closepipe[0])
	}()
	buf := make([]unix.Kevent_t, 64)
	for {
		n, err := unix.Kevent(k.kq, nil, buf, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			// The queue is unusable; the Watcher sees the channels
			// close and falls back to polling.
			select {
			case k.errs <- err:
			case <-k.done:
			}
			return
		}
		for _, kev := range buf[:n] {
			fd := int(kev.Ident)
			if fd == k.closepipe[0] {
				return
			}
			var op fsnotify.Op
			fl := uint32(kev.Fflags)
			switch {
			case fl&(unix.NOTE_DELETE|unix.NOTE_REVOKE) != 0:
				op = fsnotify.Remove
			case fl&unix.NOTE_RENAME != 0:
				op = fsnotify.Rename
			case fl&(unix.NOTE_WRITE|unix.NOTE_EXTEND) != 0:
				op = fsnotify.Write
			}
			k.mu.Lock()
			path, ok := k.byFD[fd]
			if ok && op != fsnotify.Write {
				// A deleted directory's subdirectories report their
				// own NOTE_DELETE; a renamed one's do not.
				k.forgetLocked(path, op == fsnotify.Rename)
			}
			k.mu.Unlock()
			if !ok || op == 0 {
				continue
			}
			if !k.send(fsnotify.Event{Name: path, Op: op}) {
				return
			}
		}
	}
}

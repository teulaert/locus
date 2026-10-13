package fswatch

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/teulaert/locus/internal/pathguard"
)

// Watch modes, as reported by Watcher.Stats and `locus status`.
const (
	// ModeInotify is Linux inotify: one watch per directory, no file
	// descriptor per watch, and in-place writes to files are reported.
	ModeInotify = "inotify"
	// ModeKqueueDirs is kqueue on macOS and the BSDs, registered on
	// directories only: one descriptor per watched directory. Entries
	// created, removed or renamed in a directory are reported; writes to an
	// existing file in place are not, and are found by the periodic
	// reconciliation instead (see kqueueDirs).
	ModeKqueueDirs = "kqueue-dirs"
	// ModePoll means nothing is watched: changes are found by the periodic
	// reconciliation alone. The watcher falls back to it when the kernel
	// refuses more watches or the descriptor budget is spent.
	ModePoll = "poll"
)

// Stats describes what a Watcher watches, for status output.
type Stats struct {
	// Mode is the backend in use (inotify, kqueue-dirs, poll, ...).
	Mode string `json:"mode"`
	// Watches is the number of directories watched now; 0 in poll mode.
	Watches int `json:"watches"`
	// MaxWatches is the number of watches beyond which the watcher falls
	// back to polling rather than exhaust the process's file descriptors.
	// 0 when the backend holds no descriptor per watch (inotify).
	MaxWatches int `json:"max_watches,omitempty"`
	// FDLimit is the process's soft RLIMIT_NOFILE, 0 when unknown.
	FDLimit uint64 `json:"fd_limit,omitempty"`
	// Reason says why the mode is poll.
	Reason string `json:"reason,omitempty"`
}

// backend is the kernel interface below a Watcher. Every backend watches the
// directories it is given, one at a time; the Watcher decides which ones.
type backend interface {
	// add watches one directory. Adding a watched directory again is a
	// no-op.
	add(dir string) error
	// watching reports whether dir is watched now.
	watching(dir string) bool
	// count is the number of directories watched now.
	count() int
	// list returns the watched directories.
	list() []string
	events() <-chan fsnotify.Event
	errors() <-chan error
	// close releases every watch and descriptor. It is idempotent and
	// closes the event and error channels.
	close() error
	traits() backendTraits
}

// backendTraits describes how a backend behaves.
type backendTraits struct {
	mode string
	// descriptors is true when every watch holds an open file descriptor,
	// so the number of watches is bounded by RLIMIT_NOFILE.
	descriptors bool
	// dirsOnly is true when the backend reports a change in a directory
	// as a Write on the directory itself, without naming the entry, so
	// the Watcher must look for new subdirectories on its own.
	dirsOnly bool
}

// Shares of the soft descriptor limit a descriptor-holding backend may use.
// The rest is left for sockets, databases and the files being synchronized:
// a watcher that took every descriptor would make every other open fail with
// "too many open files" and take the daemon down with it.
const (
	watchWarnShare = 4 // warn once watches reach 1/4 of the limit
	watchMaxShare  = 2 // fall back to polling at 1/2 of the limit
	// assumedFDLimit stands in for an unknown descriptor limit.
	assumedFDLimit = 4096
	// maxFDLimit caps an unlimited (or absurd) descriptor limit.
	maxFDLimit = 1 << 20
)

// watchBudget is when a Watcher warns and when it gives up watching.
type watchBudget struct {
	fdLimit uint64
	warnAt  int // log a warning once the count reaches it; 0 never
	maxAt   int // fall back to polling rather than exceed it; 0 no cap
}

// budgetFor derives the budget from the soft descriptor limit. Backends that
// hold no descriptor per watch (inotify has its own kernel limit, reported
// as ENOSPC) are not capped.
func budgetFor(fdLimit uint64, t backendTraits) watchBudget {
	b := watchBudget{fdLimit: fdLimit}
	if !t.descriptors {
		return b
	}
	l := fdLimit
	if l == 0 {
		l = assumedFDLimit
	}
	l = min(l, maxFDLimit)
	b.warnAt = int(l / watchWarnShare)
	b.maxAt = int(l / watchMaxShare)
	return b
}

// exhausted reports whether err means the kernel refuses more watches or
// descriptors: the process or system is out of descriptors (EMFILE, ENFILE)
// or inotify's per-user watch limit is reached (ENOSPC).
func exhausted(err error) bool {
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) || errors.Is(err, syscall.ENOSPC)
}

// Watcher reports filesystem changes below a set of roots. Events are
// coalesced with a trailing debounce: the callback is invoked once activity
// has been quiet for Debounce, so a burst of intermediate states produces a
// single reconciliation pass. A burst that never settles is still reported
// after MaxDelay, so continuous writes cannot postpone reconciliation forever.
//
// Only directories are ever registered, never files. Hidden directories are
// not watched, and events for hidden paths and transfer temporaries
// (TempPrefix) are ignored, unless SyncHidden enables hidden paths for their
// root (temporaries are always ignored).
//
// Watching is a hint, never a requirement: when the kernel refuses a watch
// for lack of resources, or a descriptor-holding backend reaches its budget,
// the watcher releases every watch and falls back to ModePoll, leaving
// change detection to the periodic reconciliation. It never fails the
// daemon.
type Watcher struct {
	Roots []string
	// SyncHidden marks the roots (by path, as listed in Roots) whose
	// hidden paths are watched too. Nil watches no hidden paths.
	SyncHidden map[string]bool
	Debounce   time.Duration
	// MaxDelay caps how long a continuous burst may postpone the callback.
	MaxDelay time.Duration
	// Notify is called with a context that ends when Stop is called, so a
	// reconciliation pass in progress gives up instead of holding up the
	// shutdown (LOC-81).
	Notify func(ctx context.Context, reason string)
	// Log receives watcher warnings, and errors at debug level; nil uses
	// slog.Default.
	Log *slog.Logger

	// newBackend and fdLimit are replaced by tests; nil uses the
	// platform's backend and the process's soft RLIMIT_NOFILE.
	newBackend func() (backend, error)
	fdLimit    func() uint64

	be     backend
	budget watchBudget

	mu     sync.Mutex
	mode   string
	reason string
	warned bool

	started  bool // under mu: Start has run, so loop closes done
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	// ctx is passed to Notify and cancelled by Stop.
	ctx    context.Context
	cancel context.CancelFunc
}

// NewWatcher creates a watcher. Notify is called from the watcher's own
// goroutine, never concurrently with itself, with a context Stop cancels.
func NewWatcher(roots []string, debounce time.Duration, notify func(ctx context.Context, reason string)) *Watcher {
	if debounce <= 0 {
		debounce = 300 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Watcher{Roots: roots, Debounce: debounce, MaxDelay: 10 * debounce, Notify: notify, stop: make(chan struct{}), done: make(chan struct{}), ctx: ctx, cancel: cancel}
}

func (w *Watcher) log() *slog.Logger {
	if w.Log != nil {
		return w.Log
	}
	return slog.Default()
}

// Start begins watching. Missing roots are tolerated and picked up on the
// next reconcile pass. It cannot fail: a platform watcher that cannot be
// created, or runs out of resources while registering the tree, leaves the
// watcher in ModePoll.
func (w *Watcher) Start() {
	w.mu.Lock()
	if w.started {
		// Started already, or stopped before it was started.
		w.mu.Unlock()
		return
	}
	w.started = true
	w.mu.Unlock()
	limit := w.fdLimit
	if limit == nil {
		limit = descriptorLimit
	}
	newBackend := w.newBackend
	if newBackend == nil {
		newBackend = newPlatformBackend
	}
	be, err := newBackend()
	if err != nil {
		w.mu.Lock()
		w.mode, w.reason = ModePoll, fmt.Sprintf("watcher unavailable: %v", err)
		w.budget = watchBudget{fdLimit: limit()}
		w.mu.Unlock()
		w.log().Warn("filesystem watcher unavailable; changes are found by the periodic reconciliation", "err", err)
		go w.loop()
		return
	}
	w.be = be
	w.budget = budgetFor(limit(), be.traits())
	w.mu.Lock()
	w.mode = be.traits().mode
	w.mu.Unlock()
	for _, r := range w.Roots {
		w.addRecursive(r)
	}
	go w.loop()
}

// Stats reports the watch mode and count. It is safe to call concurrently
// with the watcher running.
func (w *Watcher) Stats() Stats {
	w.mu.Lock()
	mode, reason := w.mode, w.reason
	w.mu.Unlock()
	st := Stats{Mode: mode, MaxWatches: w.budget.maxAt, FDLimit: w.budget.fdLimit, Reason: reason}
	if mode != ModePoll && w.be != nil {
		st.Watches = w.be.count()
	}
	return st
}

func (w *Watcher) polling() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mode == ModePoll
}

// fallback switches to ModePoll for good, releasing every watch and the
// descriptors they hold, so a tree too large to watch costs nothing but
// latency. Change detection is left to the periodic reconciliation.
func (w *Watcher) fallback(reason string) {
	w.mu.Lock()
	if w.mode == ModePoll {
		w.mu.Unlock()
		return
	}
	w.mode, w.reason = ModePoll, reason
	w.mu.Unlock()
	w.log().Warn("filesystem watcher falls back to polling; changes are found by the periodic reconciliation", "reason", reason)
	if w.be != nil {
		_ = w.be.close()
	}
}

// addRecursive registers dir and every directory below it, except hidden
// ones on a root that does not sync them. It stops at the first watch the
// budget or the kernel refuses, having fallen back to polling.
func (w *Watcher) addRecursive(dir string) {
	dir = filepath.Clean(dir)
	root, ok := w.rootOf(dir)
	if !ok || w.polling() {
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
		if skipHidden(root, path, d, hidden) || (path != root && IsInternal(d.Name())) {
			return fs.SkipDir
		}
		if !w.addDir(path) {
			return fs.SkipAll
		}
		return nil
	})
}

// addDir watches one directory within the budget. It returns false once the
// watcher has fallen back to polling, so the caller stops adding.
func (w *Watcher) addDir(path string) bool {
	b := w.budget
	if b.maxAt > 0 && w.be.count() >= b.maxAt {
		w.fallback(fmt.Sprintf("%d directory watches reached the cap of %d (half the descriptor limit %d)", w.be.count(), b.maxAt, b.fdLimit))
		return false
	}
	if err := w.be.add(path); err != nil {
		if exhausted(err) {
			w.fallback(fmt.Sprintf("watching %s: %v", path, err))
			return false
		}
		// Gone or unreadable: the reconciliation covers it.
		w.log().Debug("filesystem watcher add", "path", path, "err", err)
		return true
	}
	if b.warnAt > 0 {
		if n := w.be.count(); n >= b.warnAt {
			w.mu.Lock()
			first := !w.warned
			w.warned = true
			w.mu.Unlock()
			if first {
				w.log().Warn("filesystem watcher holds many descriptors; it falls back to polling at the cap",
					"watches", n, "cap", b.maxAt, "fd_limit", b.fdLimit)
			}
		}
	}
	return true
}

// addNewSubdirs watches the subdirectories of dir that are not watched yet.
// A dirs-only backend reports an entry created in dir as a write to dir, so
// this is how a directory created (or renamed into place) while running is
// found.
func (w *Watcher) addNewSubdirs(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		p := filepath.Join(dir, e.Name())
		if w.ignored(p) || w.be.watching(p) {
			continue
		}
		w.addRecursive(p)
		if w.polling() {
			return
		}
	}
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
// does not track: a transfer temporary (TempPrefix), a hidden path on a root
// that does not sync hidden ones (an editor's .swp file, .DS_Store), or a
// path outside every root.
func (w *Watcher) ignored(path string) bool {
	path = filepath.Clean(path)
	root, ok := w.rootOf(path)
	if !ok {
		return true
	}
	if path == root {
		return false
	}
	if IsInternal(filepath.Base(path)) {
		return true
	}
	if w.SyncHidden[root] {
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
		evs        <-chan fsnotify.Event
		errs       <-chan error
		dirsOnly   bool
	)
	if w.be != nil {
		evs, errs, dirsOnly = w.be.events(), w.be.errors(), w.be.traits().dirsOnly
	}
	// closed handles a backend channel closing: after a fallback that is
	// expected, otherwise the backend died and polling takes over. A nil
	// channel is never ready, so the loop goes on serving stop.
	closed := func() {
		evs, errs = nil, nil
		w.fallback("watcher stopped delivering events")
	}
	fire := func(reason string) {
		timer.Stop()
		pending = false
		if w.Notify != nil && w.ctx.Err() == nil {
			w.Notify(w.ctx, reason)
		}
	}
	for {
		select {
		case <-w.stop:
			// A change still pending is left to the reconciliation the
			// next start runs: Notify would only be handed a context
			// that has already ended.
			return
		case <-timer.C:
			if pending {
				fire("watcher")
			}
		case ev, ok := <-evs:
			if !ok {
				closed()
				continue
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			if w.ignored(ev.Name) {
				continue
			}
			if ev.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
				if info, err := os.Lstat(ev.Name); err == nil && info.IsDir() {
					w.addRecursive(ev.Name)
				}
			}
			if dirsOnly && ev.Has(fsnotify.Write) {
				w.addNewSubdirs(ev.Name)
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
		case err, ok := <-errs:
			if !ok {
				closed()
				continue
			}
			// The kernel dropped events, so what changed is unknown: that is
			// exactly when a full reconciliation is needed.
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				fire("overflow")
				continue
			}
			// Other errors are hints lost, not correctness: the periodic
			// reconciliation still covers whatever they hid.
			w.log().Debug("filesystem watcher error", "err", err)
		}
	}
}

// Stop terminates the watcher and releases every watch. Stopping again is a
// no-op. It cancels the context a Notify call in progress was given and
// waits for that call to return, so it is as prompt as Notify is at
// honouring its context; nothing else it waits for can block (LOC-81).
func (w *Watcher) Stop() {
	w.stopOnce.Do(func() {
		w.cancel()
		close(w.stop)
		w.mu.Lock()
		if !w.started {
			// No loop runs to close done.
			w.started = true
			close(w.done)
		}
		w.mu.Unlock()
	})
	<-w.done
	if w.be != nil {
		_ = w.be.close()
	}
}

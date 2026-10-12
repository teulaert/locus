package fswatch

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// fakeBackend records the directories it is asked to watch and can be told
// to refuse watches the way a kernel out of resources does.
type fakeBackend struct {
	tr backendTraits
	// failAt makes add fail with failErr once that many directories are
	// watched; 0 never fails.
	failAt  int
	failErr error

	mu     sync.Mutex
	dirs   map[string]bool
	closed bool
	evs    chan fsnotify.Event
	errs   chan error
}

func newFake(tr backendTraits) *fakeBackend {
	return &fakeBackend{tr: tr, dirs: map[string]bool{}, evs: make(chan fsnotify.Event), errs: make(chan error)}
}

func (f *fakeBackend) add(dir string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errors.New("closed")
	}
	if f.failAt > 0 && len(f.dirs) >= f.failAt {
		return f.failErr
	}
	f.dirs[filepath.Clean(dir)] = true
	return nil
}

func (f *fakeBackend) watching(dir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dirs[filepath.Clean(dir)]
}

func (f *fakeBackend) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dirs)
}

func (f *fakeBackend) list() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for d := range f.dirs {
		out = append(out, d)
	}
	return out
}

func (f *fakeBackend) events() <-chan fsnotify.Event { return f.evs }
func (f *fakeBackend) errors() <-chan error          { return f.errs }
func (f *fakeBackend) traits() backendTraits         { return f.tr }

func (f *fakeBackend) close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		clear(f.dirs)
		close(f.evs)
		close(f.errs)
	}
	return nil
}

func (f *fakeBackend) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

var kqueueTraits = backendTraits{mode: ModeKqueueDirs, descriptors: true, dirsOnly: true}

// fakeWatcher is a watcher over root using fb and a descriptor limit of
// fdLimit, logging into the returned buffer.
func fakeWatcher(root string, fb *fakeBackend, fdLimit uint64, notify func(string)) (*Watcher, *bytes.Buffer) {
	var buf bytes.Buffer
	w := NewWatcher([]string{root}, 30*time.Millisecond, notify)
	w.Log = slog.New(slog.NewTextHandler(&lockedWriter{w: &buf}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w.newBackend = func() (backend, error) { return fb, nil }
	w.fdLimit = func() uint64 { return fdLimit }
	return w, &buf
}

type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// mkdirs creates n sibling directories below root.
func mkdirs(t *testing.T, root string, n int) {
	t.Helper()
	for i := range n {
		if err := os.MkdirAll(filepath.Join(root, "d", string(rune('a'+i))), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBudgetFor(t *testing.T) {
	inotify := backendTraits{mode: ModeInotify}
	if b := budgetFor(1024, inotify); b.warnAt != 0 || b.maxAt != 0 || b.fdLimit != 1024 {
		t.Fatalf("inotify budget = %+v, want no caps", b)
	}
	cases := []struct {
		limit         uint64
		warnAt, maxAt int
	}{
		{24576, 6144, 12288},        // a typical macOS kern.maxfilesperproc
		{0, 1024, 2048},             // unknown: assumedFDLimit
		{1 << 62, 1 << 18, 1 << 19}, // unlimited: maxFDLimit
	}
	for _, c := range cases {
		b := budgetFor(c.limit, kqueueTraits)
		if b.warnAt != c.warnAt || b.maxAt != c.maxAt || b.fdLimit != c.limit {
			t.Errorf("budgetFor(%d) = %+v, want warn %d max %d", c.limit, b, c.warnAt, c.maxAt)
		}
	}
}

func TestExhausted(t *testing.T) {
	for _, err := range []error{syscall.EMFILE, syscall.ENFILE, syscall.ENOSPC, &os.PathError{Op: "open", Path: "/x", Err: syscall.EMFILE}} {
		if !exhausted(err) {
			t.Errorf("exhausted(%v) = false", err)
		}
	}
	for _, err := range []error{syscall.EACCES, syscall.ENOENT, errors.New("other")} {
		if exhausted(err) {
			t.Errorf("exhausted(%v) = true", err)
		}
	}
}

// A descriptor-holding backend warns at a quarter of the descriptor limit
// and gives up watching at half of it, releasing every watch instead of
// taking the process's last descriptors (LOC-70).
func TestWatcherFallsBackToPollingAtTheDescriptorCap(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, 10)
	fb := newFake(kqueueTraits)
	w, logs := fakeWatcher(root, fb, 8, nil) // warn at 2, cap at 4
	w.Start()
	defer w.Stop()

	st := w.Stats()
	if st.Mode != ModePoll || st.Watches != 0 || st.MaxWatches != 4 || st.FDLimit != 8 {
		t.Fatalf("stats = %+v, want poll with no watches, cap 4 of 8", st)
	}
	if !strings.Contains(st.Reason, "cap of 4") {
		t.Fatalf("reason = %q", st.Reason)
	}
	if !fb.isClosed() {
		t.Fatal("backend was not closed, so its descriptors are still held")
	}
	out := logs.String()
	if !strings.Contains(out, "holds many descriptors") || !strings.Contains(out, "falls back to polling") {
		t.Fatalf("expected a warning and a fallback in the log:\n%s", out)
	}
}

// Under the cap nothing changes: every directory is watched, and the
// warning is logged once.
func TestWatcherWarnsOnceBelowTheCap(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, 4) // root, d, and 4 below it: 6 directories
	fb := newFake(kqueueTraits)
	w, logs := fakeWatcher(root, fb, 40, nil) // warn at 10, cap at 20
	w.Start()
	defer w.Stop()
	if st := w.Stats(); st.Mode != ModeKqueueDirs || st.Watches != 6 || st.Reason != "" {
		t.Fatalf("stats = %+v, want kqueue-dirs with 6 watches", st)
	}
	if strings.Contains(logs.String(), "WARN") {
		t.Fatalf("warned below the threshold:\n%s", logs)
	}

	w2, logs2 := fakeWatcher(root, newFake(kqueueTraits), 16, nil) // warn at 4, cap at 8
	w2.Start()
	defer w2.Stop()
	if st := w2.Stats(); st.Mode != ModeKqueueDirs || st.Watches != 6 {
		t.Fatalf("stats = %+v, want kqueue-dirs with 6 watches", st)
	}
	if n := strings.Count(logs2.String(), "holds many descriptors"); n != 1 {
		t.Fatalf("warned %d times, want once:\n%s", n, logs2)
	}
}

// EMFILE, ENFILE or ENOSPC from the kernel never fail the watcher: it
// falls back to polling. Other errors (a directory removed meanwhile) only
// lose that one directory.
func TestWatcherFallsBackWhenTheKernelRefusesWatches(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EMFILE, syscall.ENFILE, syscall.ENOSPC} {
		t.Run(errno.Error(), func(t *testing.T) {
			root := t.TempDir()
			mkdirs(t, root, 6)
			fb := newFake(backendTraits{mode: ModeInotify})
			fb.failAt, fb.failErr = 3, errno
			w, _ := fakeWatcher(root, fb, 1<<20, nil)
			w.Start()
			defer w.Stop()
			st := w.Stats()
			if st.Mode != ModePoll || st.Watches != 0 || !strings.Contains(st.Reason, errno.Error()) {
				t.Fatalf("stats = %+v, want poll naming %v", st, errno)
			}
			if !fb.isClosed() {
				t.Fatal("backend was not closed")
			}
		})
	}

	root := t.TempDir()
	mkdirs(t, root, 6)
	fb := newFake(backendTraits{mode: ModeInotify})
	fb.failAt, fb.failErr = 3, syscall.EACCES
	w, _ := fakeWatcher(root, fb, 1<<20, nil)
	w.Start()
	defer w.Stop()
	if st := w.Stats(); st.Mode != ModeInotify || st.Watches != 3 {
		t.Fatalf("stats after EACCES = %+v, want inotify with the 3 watches that worked", st)
	}
}

func TestWatcherPollsWhenNoBackendCanBeCreated(t *testing.T) {
	w := NewWatcher([]string{t.TempDir()}, 30*time.Millisecond, nil)
	w.Log = slog.New(slog.DiscardHandler)
	w.newBackend = func() (backend, error) { return nil, syscall.EMFILE }
	w.Start()
	st := w.Stats()
	if st.Mode != ModePoll || !strings.Contains(st.Reason, "too many open files") {
		t.Fatalf("stats = %+v", st)
	}
	w.Stop() // must not hang or panic without a backend
}

// A backend whose channels close on their own (its kernel queue failed)
// leaves the watcher polling rather than silently deaf.
func TestWatcherPollsWhenTheBackendDies(t *testing.T) {
	fb := newFake(kqueueTraits)
	w, _ := fakeWatcher(t.TempDir(), fb, 1<<20, nil)
	w.Start()
	defer w.Stop()
	fb.close()
	deadline := time.Now().Add(5 * time.Second)
	for w.Stats().Mode != ModePoll {
		if time.Now().After(deadline) {
			t.Fatalf("stats = %+v, want poll", w.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Hidden directories and transfer temporaries are never registered: a
// .locus-tmp-* entry is left out even on a root that syncs hidden paths, and
// events for one never cause a reconciliation.
func TestWatcherNeverWatchesHiddenPathsOrTemporaries(t *testing.T) {
	root := t.TempDir()
	hiddenTree(t, root)
	if err := os.MkdirAll(filepath.Join(root, TempPrefix+"staging", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	fb := newFake(kqueueTraits)
	w, _ := fakeWatcher(root, fb, 1<<20, nil)
	w.SyncHidden = map[string]bool{root: true}
	w.Start()
	defer w.Stop()
	want := []string{".", ".git", ".git/objects", ".git/objects/ab", "notes", "notes/.obsidian", "src"}
	if got := watched(t, w, root); !reflect.DeepEqual(got, want) {
		t.Fatalf("watched = %v, want %v", got, want)
	}

	for _, sync := range []bool{false, true} {
		w.SyncHidden = map[string]bool{root: sync}
		for _, p := range []string{TempPrefix + "abc", "src/" + TempPrefix + "def"} {
			if !w.ignored(filepath.Join(root, p)) {
				t.Errorf("sync_hidden=%v: event for %s is not ignored", sync, p)
			}
		}
		if w.ignored(filepath.Join(root, "src", "main.go")) {
			t.Errorf("sync_hidden=%v: ordinary file ignored", sync)
		}
		if got := w.ignored(filepath.Join(root, ".git", "HEAD")); got == sync {
			t.Errorf("sync_hidden=%v: .git/HEAD ignored = %v", sync, got)
		}
	}
}

// A dirs-only backend reports an entry created in a directory as a write to
// that directory. The watcher then registers the new subdirectories (but
// not hidden ones) and reconciles.
func TestDirsOnlyBackendDiscoversNewSubdirectories(t *testing.T) {
	root := t.TempDir()
	notified := make(chan string, 8)
	fb := newFake(kqueueTraits)
	w, _ := fakeWatcher(root, fb, 1<<20, func(r string) { notified <- r })
	w.Start()
	defer w.Stop()
	if got := watched(t, w, root); !reflect.DeepEqual(got, []string{"."}) {
		t.Fatalf("watched = %v, want only the root", got)
	}

	for _, d := range []string{"new/a/b", ".hidden/x", TempPrefix + "y"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	fb.evs <- fsnotify.Event{Name: root, Op: fsnotify.Write}
	select {
	case <-notified:
	case <-time.After(5 * time.Second):
		t.Fatal("a write to a watched directory did not reconcile")
	}
	if got, want := watched(t, w, root), []string{".", "new", "new/a", "new/a/b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("watched = %v, want %v", got, want)
	}
}

// On Linux the platform backend is inotify, one watch per directory, and
// Stats reports it.
func TestPlatformWatcherStats(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("inotify is Linux only")
	}
	root := t.TempDir()
	hiddenTree(t, root)
	w := NewWatcher([]string{root}, 30*time.Millisecond, nil)
	w.Start()
	defer w.Stop()
	st := w.Stats()
	if st.Mode != ModeInotify || st.Watches != 3 || st.MaxWatches != 0 || st.Reason != "" {
		t.Fatalf("stats = %+v, want inotify with 3 watches and no cap", st)
	}
	got := watched(t, w, root)
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{".", "notes", "src"}) {
		t.Fatalf("watched = %v", got)
	}
}

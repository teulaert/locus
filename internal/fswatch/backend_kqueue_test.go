//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package fswatch

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// openDescriptors counts this process's open descriptors.
func openDescriptors(t *testing.T) int {
	t.Helper()
	es, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Skipf("cannot list /dev/fd: %v", err)
	}
	return len(es)
}

// LOC-70: descriptors scale with directories, not files. A root of 3
// directories and 600 files costs 3 descriptors (plus the queue and its
// wake-up pipe), where fsnotify's kqueue backend held one per file.
func TestKqueueDirsHoldsOneDescriptorPerDirectory(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"a", "b"} {
		for i := range 300 {
			writeFile(t, filepath.Join(root, d, fmt.Sprintf("f%03d.txt", i)), "x")
		}
	}
	before := openDescriptors(t)
	w := NewWatcher([]string{root}, 30*time.Millisecond, nil)
	w.Start()
	defer w.Stop()
	st := w.Stats()
	if st.Mode != ModeKqueueDirs || st.Watches != 3 || st.MaxWatches == 0 {
		t.Fatalf("stats = %+v, want kqueue-dirs with 3 watches and a cap", st)
	}
	if extra := openDescriptors(t) - before; extra > 3+3 {
		t.Fatalf("watching 3 directories holds %d more descriptors", extra)
	}
	w.Stop()
	// The reader closes the queue itself on its way out.
	deadline := time.Now().Add(5 * time.Second)
	for openDescriptors(t) > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d descriptors still open after Stop", openDescriptors(t)-before)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func nextEvent(t *testing.T, k *kqueueDirs) fsnotify.Event {
	t.Helper()
	select {
	case ev := <-k.events():
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no kqueue event")
		return fsnotify.Event{}
	}
}

// Entries created, renamed and removed in a watched directory are reported
// as writes to it; a removed directory is forgotten.
func TestKqueueDirsReportsDirectoryChanges(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	k, err := newKqueueDirs()
	if err != nil {
		t.Fatal(err)
	}
	defer k.close()
	for _, d := range []string{root, sub} {
		if err := k.add(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := k.add(root); err != nil || k.count() != 2 {
		t.Fatalf("re-adding: err %v, count %d", err, k.count())
	}

	writeFile(t, filepath.Join(sub, "new.txt"), "x")
	if ev := nextEvent(t, k); ev.Name != sub || !ev.Has(fsnotify.Write) {
		t.Fatalf("create: event %v, want write on %s", ev, sub)
	}
	if err := os.Rename(filepath.Join(sub, "new.txt"), filepath.Join(sub, "renamed.txt")); err != nil {
		t.Fatal(err)
	}
	if ev := nextEvent(t, k); ev.Name != sub || !ev.Has(fsnotify.Write) {
		t.Fatalf("rename: event %v, want write on %s", ev, sub)
	}

	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for k.watching(sub) {
		select {
		case <-k.events():
		case <-deadline:
			t.Fatal("removed directory is still watched")
		}
	}
	if k.count() != 1 {
		t.Fatalf("count = %d after removing sub, want 1", k.count())
	}
}

// A directory created while running is watched through its parent's write,
// so a file created in it is reported too.
func TestKqueueWatcherFollowsNewDirectories(t *testing.T) {
	root := t.TempDir()
	notified := make(chan string, 32)
	w := NewWatcher([]string{root}, 30*time.Millisecond, func(r string) { notified <- r })
	w.Start()
	defer w.Stop()
	wait := func(what string) {
		t.Helper()
		select {
		case <-notified:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s was not reported", what)
		}
	}
	writeFile(t, filepath.Join(root, "dir", "a.txt"), "x")
	wait("a new directory")
	deadline := time.Now().Add(5 * time.Second)
	for !w.be.watching(filepath.Join(root, "dir")) {
		if time.Now().After(deadline) {
			t.Fatal("new directory never watched")
		}
		time.Sleep(10 * time.Millisecond)
	}
	writeFile(t, filepath.Join(root, "dir", "b.txt"), "y")
	wait("a file in the new directory")
}

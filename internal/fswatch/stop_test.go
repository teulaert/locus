package fswatch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// LOC-81: a restart under load reported the filesystem watcher as still
// running after the 15 s shutdown deadline. Stop waits for the Notify call
// in progress (a reconciliation pass), so it cancels that call's context
// first: a callback that runs until its context ends no longer holds Stop.
func TestStopReturnsPromptlyWhileNotifyIsBlocked(t *testing.T) {
	root := t.TempDir()
	fb := newFake(backendTraits{mode: ModeInotify})
	entered := make(chan struct{})
	returned := make(chan error, 1)
	w, _ := fakeWatcher(root, fb, 1<<20, func(ctx context.Context, _ string) {
		close(entered)
		<-ctx.Done() // a reconcile pass that runs until it is told to stop
		returned <- ctx.Err()
	})
	w.Start()
	fb.evs <- fsnotify.Event{Name: filepath.Join(root, "a.txt"), Op: fsnotify.Create}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the change was never notified")
	}

	stopped := make(chan struct{})
	start := time.Now()
	go func() { w.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return within 1s while Notify was running")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Stop took %s", took)
	}
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("Notify's context ended with %v, want context.Canceled", err)
	}
	if !fb.isClosed() {
		t.Fatal("Stop left the backend open")
	}
}

// Stop before Start returns at once, and a later Start does nothing.
func TestStopBeforeStart(t *testing.T) {
	w := NewWatcher([]string{t.TempDir()}, 30*time.Millisecond, nil)
	done := make(chan struct{})
	go func() { w.Stop(); w.Start(); w.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop of a watcher never started blocked")
	}
}

// A scan given a context ends with the context's error once it is done,
// rather than walking and hashing the rest of the tree (LOC-81).
func TestScanEndsWithItsContext(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Scan(root, ScanOptions{Context: ctx}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Scan with a cancelled context returned %v, want context.Canceled", err)
	}
	got, err := Scan(root, ScanOptions{Context: context.Background()})
	if err != nil || len(got) != 3 {
		t.Fatalf("Scan = %d entries, %v; want 3", len(got), err)
	}
}

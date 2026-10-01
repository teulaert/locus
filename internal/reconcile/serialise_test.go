package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A daemon starts passes from the watcher callback, the periodic ticker and
// `sync now`; they must run one at a time (LOC-20). The first pass is held
// inside its root; a second pass must not enter until the first finishes.
func TestConcurrentPassesAreSerialised(t *testing.T) {
	e := newEnv(t)
	e.write(t, "a.txt", "one")

	entered := make(chan int, 2)
	release := make(chan struct{})
	calls := 0
	e.rec.beforeRoot = func(string) {
		calls++ // unsynchronised on purpose: -race flags overlapping passes
		n := calls
		entered <- n
		if n == 1 {
			<-release
		}
	}

	errs := make(chan error, 2)
	pass := func() {
		_, err := e.rec.Pass(e.ctx)
		errs <- err
	}
	go pass()
	if n := <-entered; n != 1 {
		t.Fatalf("first pass entered as call %d", n)
	}
	go pass()
	// Negative check: give the second pass the time it would need to start
	// if nothing held it back. It must not enter while the first runs.
	select {
	case n := <-entered:
		t.Fatalf("pass %d entered while the first pass was still running", n)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case n := <-entered:
		if n != 2 {
			t.Fatalf("second pass entered as call %d", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second pass never ran after the first finished")
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

// A pass waiting for the slot gives up when its context ends, so an API
// request queued behind a long pass is not held past its deadline.
func TestWaitingPassHonoursContext(t *testing.T) {
	e := newEnv(t)
	release := make(chan struct{})
	entered := make(chan struct{})
	e.rec.beforeRoot = func(string) {
		close(entered)
		<-release
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = e.rec.Pass(e.ctx)
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := e.rec.Pass(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting pass returned %v, want deadline exceeded", err)
	}
	close(release)
	<-done
}

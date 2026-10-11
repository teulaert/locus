package app

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/embed"
)

// stubEmbedder lets a test control how long an embedding call takes. Embed
// signals entered on its first call, then waits for release (ignoring its
// context, like a stuck inference call) or, when release is nil, for its
// context to end plus windDown, then records that it returned.
type stubEmbedder struct {
	entered  chan struct{}
	once     sync.Once
	release  chan struct{}
	windDown time.Duration
	returned atomic.Bool
}

func (s *stubEmbedder) ID() string { return "stub" }
func (s *stubEmbedder) Dims() int  { return embed.Dims }
func (s *stubEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	defer s.returned.Store(true)
	s.once.Do(func() { close(s.entered) })
	if s.release != nil {
		<-s.release
	} else {
		<-ctx.Done()
		time.Sleep(s.windDown) // models a worker that takes a moment to unwind
	}
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = make([]float32, embed.Dims)
		out[i][0] = 1
	}
	return out, ctx.Err()
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

// servableApp is testApp on private ports, with no discovery targets and the
// given embedder (when non-nil), and one document waiting to be indexed.
func servableApp(t *testing.T, e embed.Embedder) *App {
	t.Helper()
	a := testApp(t)
	a.Cfg.Peer.Addr = "127.0.0.1"
	a.Cfg.Peer.Port = freeTCPPort(t)
	a.Cfg.Discovery.Port = freeUDPPort(t)
	a.Peers = nil
	if e != nil {
		a.Embedder = e
		a.Indexer.Embedder = e
	}
	if err := os.WriteFile(filepath.Join(a.Roots()[0], "note.txt"), []byte("the solar inverter invoice from March"), 0o600); err != nil {
		t.Fatal(err)
	}
	return a
}

// startServe runs Serve in the background and waits until it accepts peer
// connections (it holds the daemon lock from before that point).
func startServe(t *testing.T, a *App, opts ...ServeOption) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, cancelCtx := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- a.Serve(ctx, opts...) }()
	addr := net.JoinHostPort(a.Cfg.Peer.Addr, strconv.Itoa(a.Cfg.Peer.Port))
	deadline := time.Now().Add(20 * time.Second)
	for {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		select {
		case err := <-ch:
			cancelCtx()
			t.Fatalf("serve exited early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			cancelCtx()
			t.Fatal("serve never accepted peer connections")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cancelCtx, ch
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// SIGTERM must not hang on a worker stuck in an embedding call: Serve gives
// up at its shutdown deadline and names what did not stop (LOC-20).
func TestShutdownIsBoundedWhenAWorkerIsStuck(t *testing.T) {
	stub := &stubEmbedder{entered: make(chan struct{}), release: make(chan struct{})}
	a := servableApp(t, stub)
	base := runtime.NumGoroutine()
	cancel, done := startServe(t, a, WithShutdownTimeout(300*time.Millisecond))
	waitClosed(t, stub.entered, "a worker to enter the embedder")

	cancel()
	start := time.Now()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		close(stub.release)
		t.Fatal("Serve did not return while a worker was stuck")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("Serve took %s to return with a 300ms shutdown bound", took)
	}
	var se *ShutdownError
	if !errors.As(err, &se) {
		t.Fatalf("Serve returned %v, want a *ShutdownError", err)
	}
	if !slices.Contains(se.Pending, "index workers") {
		t.Fatalf("pending = %v, want it to name the index workers", se.Pending)
	}

	// Once unstuck, the worker exits; nothing else was left behind.
	close(stub.release)
	waitGoroutines(t, base)
}

// When Serve returns cleanly, every goroutine it started has finished:
// a worker still unwinding when the context is cancelled is waited for, so
// the caller can close the databases under it safely (LOC-20).
func TestServeWaitsForItsGoroutines(t *testing.T) {
	stub := &stubEmbedder{entered: make(chan struct{}), windDown: 200 * time.Millisecond}
	a := servableApp(t, stub)
	base := runtime.NumGoroutine()
	cancel, done := startServe(t, a)
	waitClosed(t, stub.entered, "a worker to enter the embedder")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Serve did not return")
	}
	if !stub.returned.Load() {
		t.Fatal("Serve returned while an index worker was still inside Embed")
	}
	waitGoroutines(t, base)
}

// waitGoroutines fails unless the goroutine count falls back to base. A
// short grace period covers goroutines owned by libraries (fsnotify's reader,
// net/http's connection teardown) that finish just after their Close.
func waitGoroutines(t *testing.T, base int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines: %d after Serve, %d before\n%s", runtime.NumGoroutine(), base, buf[:n])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// `locus index rebuild` drops the index tables; under a running daemon its
// workers would be writing them. Rebuild and a second daemon are refused
// while one serves the data directory, and allowed again once it stops.
func TestRebuildIsRefusedWhileServing(t *testing.T) {
	a := servableApp(t, nil)
	cancel, done := startServe(t, a)

	// A second process on the same data directory, as the CLI would be.
	b, err := New(a.Cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.RebuildIndex(context.Background()); !errors.Is(err, ErrDaemonRunning) {
		t.Fatalf("rebuild under a running daemon returned %v, want ErrDaemonRunning", err)
	}
	if err := b.Serve(context.Background()); !errors.Is(err, ErrDaemonRunning) {
		t.Fatalf("second daemon returned %v, want ErrDaemonRunning", err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if err := b.RebuildIndex(context.Background()); err != nil {
		t.Fatalf("rebuild after the daemon stopped: %v", err)
	}
}

// Interrupted incoming puts are kept in the data directory for their senders
// to resume, and the daemon removes the stale ones when it starts (LOC-67).
func TestServeSweepsStalePartialUploads(t *testing.T) {
	a := servableApp(t, nil)
	dir := a.Sync.Partials.Dir
	if rel, err := filepath.Rel(a.DataDir(), dir); err != nil || rel != "partials" {
		t.Fatalf("partials live at %s, want <data_dir>/partials", dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "aa.part")
	fresh := filepath.Join(dir, "bb.part")
	for _, p := range []string{stale, fresh} {
		if err := os.WriteFile(p, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-2 * a.Sync.Partials.MaxAge)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	cancel, done := startServe(t, a)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale partial survived: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh partial removed: %v", err)
	}
}

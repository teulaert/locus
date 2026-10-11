package sync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	gosync "sync"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// heldPeer answers every request only once release is closed (or the
// request is cancelled), with a 503. entered receives a value when the first
// request arrives.
func heldPeer(t *testing.T) (srv *httptest.Server, entered <-chan struct{}, release func()) {
	t.Helper()
	in := make(chan struct{}, 1)
	gate := make(chan struct{})
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case in <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-r.Context().Done():
		}
		http.Error(w, "released", http.StatusServiceUnavailable)
	}))
	closed := false
	release = func() {
		if !closed {
			closed = true
			close(gate)
		}
	}
	t.Cleanup(func() { release(); srv.Close() })
	return srv, in, release
}

func entryAt(id string, srv *httptest.Server) *peer.Entry {
	return &peer.Entry{Info: peer.Info{PeerID: id, SyncAddr: strings.TrimPrefix(srv.URL, "http://")}, Reachable: true}
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// LOC-61: a round used to walk peers one after another, so a long transfer
// with one peer held back every other peer's manifest exchange, retries and
// status for as long as it lasted. Here one peer never answers until
// released; the other must be synchronized, and reported healthy, while the
// first is still held.
func TestSlowPeerDoesNotDelayOtherPeers(t *testing.T) {
	e := newEngineEnv(t)
	slow, slowEntered, release := heldPeer(t)
	fast, body := fakePeer(t, RootName(e.root), "", 0)

	fastDone := make(chan struct{})
	slowFailed := make(chan struct{})
	var fastOnce, slowOnce gosync.Once
	e.OnPeerSuccess = func(id string) {
		if id == "bbbb" {
			fastOnce.Do(func() { close(fastDone) })
		}
	}
	e.OnPeerError = func(id string, err error) {
		if id == "aaaa" {
			slowOnce.Do(func() { close(slowFailed) })
		}
	}
	started := make(chan string, 8)
	e.OnPeerStart = func(id string) { started <- id }

	type result struct {
		st  *Stats
		err error
	}
	done := make(chan result, 1)
	go func() {
		// The slow peer sorts first: a sequential round reaches it first.
		st, err := e.Round(context.Background(), []*peer.Entry{entryAt("aaaa", slow), entryAt("bbbb", fast)})
		done <- result{st, err}
	}()
	waitFor(t, slowEntered, "the slow peer's manifest request")
	waitFor(t, fastDone, "the fast peer to synchronize while the slow one is held")

	got, err := os.ReadFile(filepath.Join(e.root, "remote.txt"))
	if err != nil || string(got) != body {
		t.Fatalf("fast peer's file not pulled while the slow peer was held: %q, %v", got, err)
	}
	if !e.BusyWith("aaaa") || e.BusyWith("bbbb") {
		t.Fatalf("busy: aaaa=%v bbbb=%v; want only the held peer", e.BusyWith("aaaa"), e.BusyWith("bbbb"))
	}
	if p := e.Progress(); len(p) != 1 || p[0].PeerID != "aaaa" || p[0].Phase != "manifest" {
		t.Fatalf("progress while the slow peer is held = %+v", p)
	}
	if len(started) != 2 {
		t.Fatalf("OnPeerStart called %d times, want once per peer", len(started))
	}

	// A peer held by a running round is not dialed by a second one, which
	// still synchronizes with the peers that are free.
	if _, err := e.Round(context.Background(), []*peer.Entry{entryAt("aaaa", slow)}); !errors.Is(err, ErrRoundRunning) {
		t.Fatalf("second round with only the held peer: %v, want ErrRoundRunning", err)
	}
	st, err := e.Round(context.Background(), []*peer.Entry{entryAt("aaaa", slow), entryAt("bbbb", fast)})
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Busy) != 1 || st.Busy[0] != "aaaa" {
		t.Fatalf("second round's busy peers = %v, want [aaaa]", st.Busy)
	}

	release()
	waitFor(t, slowFailed, "the slow peer's failure to be reported")
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.st.Pulled != 1 || len(r.st.Errors) != 1 || !strings.HasPrefix(r.st.Errors[0], "aaaa:") {
			t.Fatalf("merged stats = %+v", r.st)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("round did not finish after the slow peer was released")
	}
	if e.Busy() || len(e.Progress()) != 0 {
		t.Fatalf("round finished but still busy: %+v", e.Progress())
	}
}

// LOC-66: while a file is being pulled, progress names the peer, root, file
// and the bytes planned and moved so far.
func TestProgressDuringPull(t *testing.T) {
	e := newEngineEnv(t)
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	content := []byte(strings.Repeat("0123456789abcdef", 8<<10)) // 128 KiB
	entry := ManifestEntry{
		RelPath:    "big.bin",
		Size:       int64(len(content)),
		Hash:       fswatch.HashBytes(content),
		Version:    vvector.New().Increment(writer).String(),
		LastWriter: writer,
	}
	half := make(chan struct{})
	gate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/manifest":
			_ = json.NewEncoder(w).Encode(Manifest{PeerID: writer, RootName: RootName(e.root), Entries: []ManifestEntry{entry}})
		case "/v1/file":
			_, _ = w.Write(content[:len(content)/2])
			w.(http.Flusher).Flush()
			close(half)
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write(content[len(content)/2:])
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})

	done := make(chan *Stats, 1)
	go func() {
		st, _ := e.Round(context.Background(), []*peer.Entry{entryAt("cccc", srv)})
		done <- st
	}()
	waitFor(t, half, "the first half of the file to be sent")

	var p PeerProgress
	deadline := time.Now().Add(10 * time.Second)
	for {
		ps := e.Progress()
		if len(ps) == 1 && ps[0].PulledBytes > 0 {
			p = ps[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no pulled bytes reported: %+v", ps)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if p.PeerID != "cccc" || p.Phase != "transfer" || p.CurrentOp != "pull" || p.CurrentFile != "big.bin" || p.Root != RootName(e.root) {
		t.Fatalf("progress mid-pull = %+v", p)
	}
	if p.PullBytesPlanned != int64(len(content)) || p.PulledBytes > int64(len(content)/2) {
		t.Fatalf("bytes planned %d moved %d; want %d planned and at most half moved", p.PullBytesPlanned, p.PulledBytes, len(content))
	}
	if r := p.Roots[0]; r.PullPlanned != 1 || r.Planned != 1 || r.Done != 0 || r.State != "transfer" {
		t.Fatalf("root progress mid-pull = %+v", r)
	}

	close(gate)
	select {
	case st := <-done:
		if st.Pulled != 1 {
			t.Fatalf("round stats = %+v", st)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("round did not finish")
	}
	if len(e.Progress()) != 0 {
		t.Fatal("progress outlived the round")
	}
}

// The rate is averaged over the last ten seconds, decays to zero when the
// transfer stalls, and the ETA follows from it and the bytes left.
func TestTrackerRateAndETA(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { return now }
	tr := newTracker("dddd", "host", []string{"Docs"}, clock)
	tr.beginRoot(0)
	tr.planned([]Action{
		{Kind: ActionPushCreate, RelPath: "a", Local: &ManifestEntry{Size: 10_000}},
		{Kind: ActionNoop, RelPath: "b"},
	})
	tr.start("push", "a")
	for i := 0; i < 20; i++ {
		now = now.Add(time.Second)
		tr.moved("push", 100)
	}
	p := tr.snapshot()
	if p.PushedBytes != 2000 || p.PushBytesPlanned != 10_000 {
		t.Fatalf("bytes = %d / %d", p.PushedBytes, p.PushBytesPlanned)
	}
	if p.BytesPerSec < 99 || p.BytesPerSec > 101 {
		t.Fatalf("rate = %.1f B/s, want 100", p.BytesPerSec)
	}
	if p.ETASeconds < 79 || p.ETASeconds > 81 {
		t.Fatalf("eta = %ds, want 80", p.ETASeconds)
	}
	if p.Roots[0].Planned != 1 || p.CurrentFile != "a" {
		t.Fatalf("snapshot = %+v", p)
	}
	now = now.Add(15 * time.Second)
	if p = tr.snapshot(); p.BytesPerSec != 0 || p.ETASeconds != 0 {
		t.Fatalf("stalled transfer: rate %.1f eta %d, want 0", p.BytesPerSec, p.ETASeconds)
	}
	// A failed transfer is taken back out of the totals.
	tr.finish(ActionPushCreate, false, true)
	if p = tr.snapshot(); p.PushedBytes != 0 || p.Roots[0].Failed != 1 || p.Roots[0].Done != 1 || p.CurrentFile != "" {
		t.Fatalf("after a failed push: %+v", p)
	}
}

func TestReason(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{&TransportError{Err: &os.SyscallError{Syscall: "connect", Err: errors.New("connection refused")}}, "connection refused"},
		{&StatusError{Status: "503 Service Unavailable", Code: 503}, "503 Service Unavailable"},
		{errors.New("local"), "local"},
		{nil, ""},
	} {
		if got := Reason(c.err); got != c.want {
			t.Errorf("Reason(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

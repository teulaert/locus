package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	gosync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/jobs"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/search"
	syncpkg "github.com/teulaert/locus/internal/sync"
	"github.com/teulaert/locus/internal/vvector"
)

func testApp(t *testing.T) *App {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "docs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = config.PathRoots(root)
	cfg.DataDir = filepath.Join(dir, "data")
	// Only what a test configures is probed, never the gateways of the
	// test machine's own WireGuard routes (LOC-76).
	noGateways := false
	cfg.Discovery.Gateways = &noGateways
	a, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// A conflict is named after the hostname of the peer whose version lost the
// normal path — including this peer itself, which presence never lists.
func TestHostnamesIncludesThisPeer(t *testing.T) {
	a := testApp(t)
	names := a.Hostnames()
	host, ok := names[a.Identity.PeerID]
	if !ok {
		t.Fatalf("hostnames missing this peer %s: %v", a.Identity.PeerID[:8], names)
	}
	if host != a.Hostname {
		t.Fatalf("this peer maps to %q, want %q", host, a.Hostname)
	}
	// Remote peers still win over nothing.
	a.Presence.Observe("10.0.0.9:8911", peer.Info{PeerID: "bbbb", Hostname: "laptop"}, 1, true)
	if got := a.Hostnames()["bbbb"]; got != "laptop" {
		t.Fatalf("remote peer hostname = %q", got)
	}
}

// Defaults must be filled in, not left zero: an unset upload limit used to
// truncate every upload to zero bytes.
func TestNewAppliesConfigDefaults(t *testing.T) {
	a := testApp(t)
	if a.Cfg.UploadMaxBytes <= 0 {
		t.Fatalf("UploadMaxBytes = %d, want a positive default", a.Cfg.UploadMaxBytes)
	}
	if a.Cfg.Workers <= 0 {
		t.Fatalf("Workers = %d", a.Cfg.Workers)
	}
	if a.Cfg.Sync.Interval.D() <= 0 || a.Cfg.ReconcileInterval.D() <= 0 {
		t.Fatalf("intervals not defaulted: %v / %v", a.Cfg.Sync.Interval.D(), a.Cfg.ReconcileInterval.D())
	}
}

// A Config built in code rather than loaded from disk must get the same
// defaults; testApp starts from config.Default() and cannot show this.
func TestNewAppliesDefaultsToZeroConfig(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "docs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Roots: config.PathRoots(root), DataDir: filepath.Join(dir, "data")}
	a, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	d := config.Default()
	if a.Cfg.UploadMaxBytes != d.UploadMaxBytes || a.Cfg.Workers != d.Workers {
		t.Fatalf("limits not defaulted: upload=%d workers=%d", a.Cfg.UploadMaxBytes, a.Cfg.Workers)
	}
	if a.Cfg.Sync.Interval != d.Sync.Interval || a.Cfg.ReconcileInterval != d.ReconcileInterval || a.Cfg.Discovery.Interval != d.Discovery.Interval {
		t.Fatalf("intervals not defaulted: %+v", a.Cfg)
	}
	if a.Cfg.API.Port != d.API.Port || a.Cfg.Embedding.Device != "auto" {
		t.Fatalf("api/embedding not defaulted: %+v", a.Cfg)
	}
}

func TestPeerRetryDelayGrowsAndStaysCapped(t *testing.T) {
	if d := peerRetryDelay(1); d != 15*time.Second {
		t.Fatalf("first retry after %s", d)
	}
	if d := peerRetryDelay(2); d != 30*time.Second {
		t.Fatalf("second retry after %s", d)
	}
	if d := peerRetryDelay(40); d != 5*time.Minute {
		t.Fatalf("retry delay escaped its cap: %s", d)
	}
}

// The outage worth reporting is a peer that answers discovery probes and still
// cannot be dialed; status must say so, and rounds must not pay a connect
// timeout every interval while it stays that way.
func TestUnreachablePeerIsReportedAndBackedOff(t *testing.T) {
	a := testApp(t)
	a.Presence.Observe("10.0.0.8:8911", peer.Info{PeerID: "cccc", Hostname: "walled", SyncAddr: "127.0.0.1:1"}, 1, true)
	a.syncOnce(context.Background())
	entry := presenceEntry(t, a, "cccc")
	if entry.Failures == 0 || entry.Reachable {
		t.Fatalf("unreachable peer looks fine: %+v", entry)
	}
	if entry.LastError == "" {
		t.Fatal("unreachable peer has no reason recorded")
	}
	deferred := a.peerBackoff["cccc"]
	if !deferred.After(time.Now()) {
		t.Fatalf("peer is not deferred: %v", deferred)
	}
	a.syncOnce(context.Background())
	if a.peerBackoff["cccc"] != deferred {
		t.Fatal("peer was retried inside its backoff window")
	}
	// Once transfers work again the peer is healthy and retried immediately.
	a.Sync.OnPeerSuccess("cccc")
	if entry = presenceEntry(t, a, "cccc"); !entry.Reachable || entry.Failures != 0 {
		t.Fatalf("after a good round: %+v", entry)
	}
	if _, ok := a.peerBackoff["cccc"]; ok {
		t.Fatal("backoff outlived the good round")
	}
}

// heldPeer answers nothing until release is called or the test ends (or
// the request is cancelled); entered receives a value when its first
// request arrives.
func heldPeer(t *testing.T) (srv *httptest.Server, entered <-chan struct{}, release func()) {
	t.Helper()
	in := make(chan struct{}, 1)
	gate := make(chan struct{})
	var once gosync.Once
	release = func() { once.Do(func() { close(gate) }) }
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
	t.Cleanup(func() { release(); srv.Close() })
	return srv, in, release
}

// emptyPeer answers manifest requests for root name with an empty manifest
// and counts them.
func emptyPeer(t *testing.T, name string, manifests *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/manifest" || r.URL.Query().Get("rootname") != name {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		manifests.Add(1)
		_ = json.NewEncoder(w).Encode(syncpkg.Manifest{PeerID: "ffffffff", RootName: name, Entries: []syncpkg.ManifestEntry{}})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// holdRound starts a round with the held peer at srv and waits until it has
// reached it. The round ends when the test does.
func holdRound(t *testing.T, a *App, id string, srv *httptest.Server, entered <-chan struct{}, release func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.Sync.Round(context.Background(), []*peer.Entry{{Info: peer.Info{PeerID: id, SyncAddr: srv.Listener.Addr().String()}}})
	}()
	t.Cleanup(func() { release(); <-done })
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the holding round never reached its peer")
	}
}

// LOC-61: a sync tick while a round is still transferring with one peer must
// synchronize the other peers instead of failing or waiting: the held peer
// stays synchronizing, the free one is exchanged with and shown online, and
// a peer recovering from a transport failure is retried and cleared.
func TestTickSyncsOtherPeersWhileOneIsHeld(t *testing.T) {
	a := testApp(t)
	slow, entered, release := heldPeer(t)
	a.Presence.Observe("10.0.0.7:8911", peer.Info{PeerID: "aaaa", Hostname: "slow", SyncAddr: slow.Listener.Addr().String()}, 1, true)
	holdRound(t, a, "aaaa", slow, entered, release)
	if e := presenceEntry(t, a, "aaaa"); e.State != peer.Synchronizing {
		t.Fatalf("held peer state %q, want synchronizing", e.State)
	}

	var manifests atomic.Int32
	fast := emptyPeer(t, a.Sync.NameOf(a.Roots()[0]), &manifests)
	a.Presence.Observe("10.0.0.8:8911", peer.Info{PeerID: "bbbb", Hostname: "fast", SyncAddr: fast.Listener.Addr().String()}, 1, true)
	// bbbb failed earlier (its daemon restarted) and its backoff has run out.
	a.Sync.OnPeerError("bbbb", &syncpkg.TransportError{Err: errors.New("connection refused")})
	a.mu.Lock()
	a.peerBackoff["bbbb"] = time.Now().Add(-time.Second)
	a.mu.Unlock()

	finished := make(chan struct{})
	go func() { defer close(finished); a.syncOnce(context.Background()) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("a tick waited for the round holding another peer")
	}
	if manifests.Load() != 1 {
		t.Fatalf("free peer's manifest fetched %d times, want 1", manifests.Load())
	}
	if e := presenceEntry(t, a, "bbbb"); e.State != peer.Online || e.Failures != 0 || !e.Reachable || e.LastTransportErrorAt != nil {
		t.Fatalf("free peer after the tick: %+v", e)
	}
	if e := presenceEntry(t, a, "aaaa"); e.State != peer.Synchronizing {
		t.Fatalf("held peer state %q after another tick, want synchronizing", e.State)
	}
	if !a.Sync.BusyWith("aaaa") {
		t.Fatal("the held round was disturbed")
	}
	if p := a.SyncStatus(); !p.Syncing || len(p.Progress) != 1 || p.Progress[0].PeerID != "aaaa" || p.Progress[0].Hostname != "slow" {
		t.Fatalf("sync status while held: %+v", p)
	}
}

// A round that cannot claim any of its peers (all held by another round)
// changes nothing: no dial, and no peer left reported as synchronizing.
func TestRoundOfHeldPeersChangesNothing(t *testing.T) {
	a := testApp(t)
	slow, entered, release := heldPeer(t)
	a.Presence.Observe("10.0.0.7:8911", peer.Info{PeerID: "eeee", Hostname: "slow", SyncAddr: slow.Listener.Addr().String()}, 1, true)
	holdRound(t, a, "eeee", slow, entered, release)

	if due := a.dueForSync(a.Presence.Online()); len(due) != 0 {
		t.Fatalf("held peer is due for another round: %+v", due)
	}
	a.syncOnce(context.Background()) // nothing due: a no-op
	if _, err := a.SyncNow(context.Background()); !errors.Is(err, syncpkg.ErrRoundRunning) {
		t.Fatalf("sync now with only a held peer: %v, want ErrRoundRunning", err)
	}
	if e := presenceEntry(t, a, "eeee"); e.State != peer.Synchronizing {
		t.Fatalf("held peer state %q, want synchronizing", e.State)
	}
}

// LOC-65: a peer with failed transfers says when it failed, why in short,
// and when it is tried next: after its backoff, on the first sync tick at or
// after it, or once the round still running with it ends.
func TestRetryScheduleIsReported(t *testing.T) {
	a := testApp(t)
	a.Presence.Observe("10.0.0.8:8911", peer.Info{PeerID: "cccc", Hostname: "walled", SyncAddr: "127.0.0.1:1"}, 1, true)
	a.syncOnce(context.Background())
	e := presenceEntry(t, a, "cccc")
	if e.LastTransportErrorAt == nil || e.TransportReason != "connection refused" {
		t.Fatalf("failure time or reason missing: at %v reason %q", e.LastTransportErrorAt, e.TransportReason)
	}

	entries := a.annotateRetries(a.Presence.Snapshot())
	until := a.peerBackoff["cccc"]
	if entries[0].RetryWait != "backoff" || entries[0].NextRetryAt == nil || !entries[0].NextRetryAt.Equal(until) {
		t.Fatalf("outside serve: wait %q at %v, want backoff at %v", entries[0].RetryWait, entries[0].NextRetryAt, until)
	}

	// Serving: attempts start on ticks, so the retry is the first tick at
	// or after the backoff runs out.
	tick := time.Now().Add(2 * time.Second)
	a.setNextSync(tick, 10*time.Second)
	entries = a.annotateRetries(a.Presence.Snapshot())
	got := entries[0].NextRetryAt
	if got == nil || got.Before(until) || got.Sub(until) >= 10*time.Second || got.Sub(tick)%(10*time.Second) != 0 {
		t.Fatalf("retry at %v; backoff until %v, ticks every 10s from %v", got, until, tick)
	}

	// Backoff over: only the next tick is awaited.
	a.mu.Lock()
	a.peerBackoff["cccc"] = time.Now().Add(-time.Second)
	a.mu.Unlock()
	entries = a.annotateRetries(a.Presence.Snapshot())
	if entries[0].RetryWait != "interval" || entries[0].NextRetryAt == nil || !entries[0].NextRetryAt.Equal(tick) {
		t.Fatalf("after backoff: wait %q at %v, want interval at %v", entries[0].RetryWait, entries[0].NextRetryAt, tick)
	}

	// A healthy peer has none of it.
	a.Sync.OnPeerSuccess("cccc")
	entries = a.annotateRetries(a.Presence.Snapshot())
	if entries[0].RetryWait != "" || entries[0].NextRetryAt != nil {
		t.Fatalf("healthy peer has a retry schedule: %+v", entries[0])
	}
}

// LOC-17: a peer that refuses transfer for a sync protocol mismatch is shown
// as incompatible with the reason, is not counted as a transport failure or
// backed off, and leaves the rounds until its announcement changes.
func TestIncompatiblePeerIsRecordedNotRetried(t *testing.T) {
	a := testApp(t)
	requests := 0
	future := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set(syncpkg.ProtocolHeader, strconv.Itoa(syncpkg.ProtocolVersion+1))
		http.Error(w, "incompatible sync protocol", http.StatusUpgradeRequired)
	}))
	t.Cleanup(future.Close)
	// A peer from before the sync protocol was advertised: discovery cannot
	// tell, so only the transfer finds out.
	info := peer.Info{PeerID: "ffff", Hostname: "future", Version: "9.0.0", SyncAddr: strings.TrimPrefix(future.URL, "http://")}
	a.Presence.Observe("10.0.0.9:8911", info, 1, true)

	a.syncOnce(context.Background())
	entry := presenceEntry(t, a, "ffff")
	if entry.State != peer.Incompatible || entry.SyncProtocol != syncpkg.ProtocolVersion+1 || !strings.Contains(entry.LastError, "protocol") {
		t.Fatalf("incompatible peer recorded as %+v", entry)
	}
	if entry.Failures != 0 || !entry.Reachable {
		t.Fatalf("incompatible peer counted as a transport failure: %+v", entry)
	}
	if _, ok := a.peerBackoff["ffff"]; ok {
		t.Fatal("incompatible peer was put on transport backoff")
	}

	// The next probe repeats the same announcement: still incompatible, and
	// no further round dials it.
	a.Presence.Observe("10.0.0.9:8911", info, 1, true)
	a.syncOnce(context.Background())
	if entry = presenceEntry(t, a, "ffff"); entry.State != peer.Incompatible {
		t.Fatalf("incompatibility forgotten after a probe: %+v", entry)
	}
	if requests != 1 {
		t.Fatalf("incompatible peer dialed %d times, want 1", requests)
	}

	// Upgraded to this version: a changed announcement makes it eligible again.
	info.Version = syncpkg.BuildVersion
	info.Capabilities = []string{"sync", peer.SyncProtocolCapability(syncpkg.ProtocolVersion)}
	a.Presence.Observe("10.0.0.9:8911", info, 1, true)
	if entry = presenceEntry(t, a, "ffff"); entry.State != peer.Online || entry.LastError != "" || entry.SyncProtocol != syncpkg.ProtocolVersion {
		t.Fatalf("upgraded peer still held incompatible: %+v", entry)
	}
}

func presenceEntry(t *testing.T, a *App, peerID string) *peer.Entry {
	t.Helper()
	for _, e := range a.Presence.Snapshot() {
		if e.PeerID == peerID {
			return e
		}
	}
	t.Fatalf("peer %s not in presence table", peerID)
	return nil
}

// The generated API token must be persisted: a token that exists only in this
// process leaves the user nothing to read, and every later call would mint a
// different one. A write failure is an error, never a silently unsaved token.
func TestAPITokenWriteFailureIsAnError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	a := testApp(t)
	dir := a.DataDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	tok, err := a.APIToken()
	if err == nil {
		t.Fatalf("APIToken on a read-only data dir returned %q with no error", tok)
	}
	if tok != "" {
		t.Fatalf("APIToken returned token %q alongside error %v", tok, err)
	}
}

// A generated token is written once and returned unchanged afterwards.
func TestAPITokenIsPersistedAndStable(t *testing.T) {
	a := testApp(t)
	first, err := a.APIToken()
	if err != nil || first == "" {
		t.Fatalf("APIToken = %q, %v", first, err)
	}
	b, err := os.ReadFile(filepath.Join(a.DataDir(), "api_token"))
	if err != nil || string(b) != first {
		t.Fatalf("api_token file = %q, %v; want %q", b, err, first)
	}
	second, err := a.APIToken()
	if err != nil || second != first {
		t.Fatalf("second APIToken = %q, %v; want %q", second, err, first)
	}
}

// If no entropy is available there is no token, and that is an error rather
// than an empty token that would match a request with no Authorization header.
func TestAPITokenEntropyFailureIsAnError(t *testing.T) {
	a := testApp(t)
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }
	t.Cleanup(func() { randRead = orig })
	if tok, err := a.APIToken(); err == nil || tok != "" {
		t.Fatalf("APIToken = %q, %v; want an error and no token", tok, err)
	}
	if _, err := os.Stat(filepath.Join(a.DataDir(), "api_token")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("api_token written despite entropy failure: %v", err)
	}
}

// Search must not wait for an index worker. index.db had a single
// connection, so a worker inside its IndexDocument transaction blocked every
// interactive query until it committed (LOC-42). Here a write transaction is
// held open on the writer connection, as a worker mid-index does, and a
// search must still answer promptly from the committed state.
func TestSearchIsNotBlockedByIndexTransaction(t *testing.T) {
	a := testApp(t)
	root := a.Roots()[0]
	if err := os.WriteFile(filepath.Join(root, "solar.txt"), []byte("the solar inverter invoice from March"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Reconciler.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Drain(context.Background(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	tx, err := a.Store.Index.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO documents (root, relpath, hash, size, indexed_at) VALUES (?, 'busy.txt', 'h', 1, 0)`, root); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		n   int
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := a.Search.Search(context.Background(), "solar invoice", search.Options{})
		done <- outcome{len(res), err}
	}()
	select {
	case o := <-done:
		if o.err != nil || o.n != 1 {
			t.Fatalf("search during index transaction: %d results, err %v; want 1", o.n, o.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("search blocked behind an open index write transaction")
	}
}

// Building an App must not requeue running jobs: every CLI command does so,
// possibly while a daemon is working them. Crash recovery belongs to
// Pipeline.Run alone (LOC-47).
func TestNewLeavesRunningJobsAlone(t *testing.T) {
	a := testApp(t)
	ctx := context.Background()
	if err := a.Queue.Enqueue(ctx, jobs.KindIndex, "root", "a.txt", "h", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Queue.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := New(a.Cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	list, err := b.Queue.List(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].State != "running" {
		t.Fatalf("jobs after a second New = %+v, want one running job", list)
	}
}

// LOC-85: a transport failure noted for a peer is cleared as soon as the
// peer answers in the next round, not when that round ends: a long round
// with a peer that answers every request must not keep saying "transfer
// failed ...; next attempt after the current round with it".
func TestTransportFailureClearedOnFirstAnswerMidRound(t *testing.T) {
	a := testApp(t)
	name := a.Sync.NameOf(a.Roots()[0])
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	body := []byte("content held back until the test releases it")
	entry := syncpkg.ManifestEntry{
		RelPath: "held.txt", Size: int64(len(body)), Hash: fswatch.HashBytes(body),
		Version: vvector.New().Increment(writer).String(), LastWriter: writer,
	}
	fileAsked := make(chan struct{}, 1)
	gate := make(chan struct{})
	var once gosync.Once
	release := func() { once.Do(func() { close(gate) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("rootname") != name {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		switch r.URL.Path {
		case "/v1/manifest":
			_ = json.NewEncoder(w).Encode(syncpkg.Manifest{PeerID: writer, RootName: name, Entries: []syncpkg.ManifestEntry{entry}})
		case "/v1/file":
			select {
			case fileAsked <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write(body)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(func() { release(); srv.Close() })

	a.Presence.Observe("10.0.0.9:8911", peer.Info{PeerID: "cccc", Hostname: "hermes", SyncAddr: srv.Listener.Addr().String()}, 1, true)
	a.Sync.OnPeerError("cccc", &syncpkg.TransportError{Err: errors.New("connection refused")})
	if e := presenceEntry(t, a, "cccc"); e.Failures == 0 || e.LastTransportErrorAt == nil {
		t.Fatalf("failure not recorded: %+v", e)
	}
	a.mu.Lock()
	a.peerBackoff["cccc"] = time.Now().Add(-time.Second)
	a.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.Sync.Round(context.Background(), a.Presence.Online())
	}()
	t.Cleanup(func() { release(); <-done })
	select {
	case <-fileAsked:
	case <-time.After(10 * time.Second):
		t.Fatal("the round never asked for the file")
	}
	if !a.Sync.BusyWith("cccc") {
		t.Fatal("round with the peer already over")
	}
	e := presenceEntry(t, a, "cccc")
	if e.Failures != 0 || !e.Reachable || e.LastTransportErrorAt != nil || e.TransportReason != "" {
		t.Fatalf("failure note outlived the peer's answer mid-round: %+v", e)
	}
	if entries := a.annotateRetries(a.Presence.Snapshot()); entries[0].RetryWait != "" {
		t.Fatalf("retry wait %q mid-round with a peer that answers", entries[0].RetryWait)
	}
	a.mu.Lock()
	_, backingOff := a.peerBackoff["cccc"]
	a.mu.Unlock()
	if backingOff {
		t.Fatal("backoff outlived the peer's answer")
	}
	release()
	<-done
	if got, err := os.ReadFile(filepath.Join(a.Roots()[0], "held.txt")); err != nil || string(got) != string(body) {
		t.Fatalf("file after the round: %q, %v", got, err)
	}
}

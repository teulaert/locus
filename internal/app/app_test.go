package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/peer"
	syncpkg "github.com/teulaert/locus/internal/sync"
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
	a.Presence.Observe("10.0.0.9:8911", peer.Info{PeerID: "bbbb", Hostname: "laptop"}, 1, true, "")
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
	a.Presence.Observe("10.0.0.8:8911", peer.Info{PeerID: "cccc", Hostname: "walled", SyncAddr: "127.0.0.1:1"}, 1, true, "")
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

// A tick that loses the race with an in-flight round (sync now, POST
// /api/v1/sync) gets an error from Round; it must not leave the peers it
// marked reported as synchronizing, because probes never clear that state.
func TestFailedRoundDoesNotLeavePeersSynchronizing(t *testing.T) {
	a := testApp(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		http.Error(w, "released", http.StatusServiceUnavailable)
	}))
	t.Cleanup(slow.Close)
	held := make(chan struct{})
	go func() {
		defer close(held)
		slowPeer := &peer.Entry{Info: peer.Info{PeerID: "eeee", SyncAddr: slow.Listener.Addr().String()}}
		_, _ = a.Sync.Round(context.Background(), []*peer.Entry{slowPeer})
	}()
	t.Cleanup(func() { close(release); <-held })
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the holding round never reached its peer")
	}
	if !a.Sync.Busy() {
		t.Fatal("no round in flight")
	}

	a.Presence.Observe("10.0.0.9:8911", peer.Info{PeerID: "dddd", Hostname: "busy", SyncAddr: "127.0.0.1:1"}, 1, true, "")
	a.syncOnce(context.Background())
	if entry := presenceEntry(t, a, "dddd"); entry.State != peer.Online {
		t.Fatalf("peer left in state %q after a refused round", entry.State)
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
	a.Presence.Observe("10.0.0.9:8911", info, 1, true, "")

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
	a.Presence.Observe("10.0.0.9:8911", info, 1, true, "")
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
	a.Presence.Observe("10.0.0.9:8911", info, 1, true, "")
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

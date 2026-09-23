package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"locus/internal/config"
	"locus/internal/peer"
)

func testApp(t *testing.T) *App {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "docs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = []string{root}
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

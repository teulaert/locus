package main

import (
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/peer"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// LOC-66: `locus status` summarises a running bulk transfer in one line.
func TestProgressSummary(t *testing.T) {
	const gb = 1 << 30
	p := syncpkg.PeerProgress{
		PeerID: "aaf711ba00", Hostname: "MacBook",
		PushBytesPlanned: 56 * gb / 10, PushedBytes: 12 * gb / 10,
		BytesPerSec: 0.9 * (1 << 20), ETASeconds: 80 * 60,
	}
	if got, want := progressSummary(p), "pushing 1.2 GB / 5.6 GB to MacBook (921.6 KB/s, ~1h20m)"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	p.Hostname, p.PushBytesPlanned, p.BytesPerSec, p.ETASeconds = "", 0, 0, 0
	p.Root = "Documents"
	if got := progressSummary(p); got != "exchanging manifests with aaf711ba (root Documents)" {
		t.Fatalf("summary before any plan = %q", got)
	}
	p.PullBytesPlanned, p.PulledBytes = 2048, 1024
	if got := progressSummary(p); got != "pulling 1.0 KB / 2.0 KB from aaf711ba (stalled)" {
		t.Fatalf("stalled pull summary = %q", got)
	}
}

// LOC-77: pulls left to another peer's round are reported per root and not
// counted in the bytes still to come.
func TestProgressDetailSkippedInFlight(t *testing.T) {
	const mb = 1 << 20
	p := syncpkg.PeerProgress{
		PeerID: "aaf711ba00", Hostname: "galaxy", StartedAt: time.Now().Add(-40 * time.Second),
		PullBytesPlanned: 600 * mb, PulledBytes: 100 * mb, SkippedBytes: 300 * mb, BytesPerSec: 12 * mb,
		Roots: []syncpkg.RootProgress{{
			Root: "Documents", State: "transfer", Planned: 1920, Done: 1357, PullPlanned: 1920, Pulled: 945,
			PulledBytes: 100 * mb, PullBytesPlanned: 600 * mb, SkippedInFlight: 412, SkippedBytes: 300 * mb,
		}},
	}
	lines := progressDetail(p, time.Now())
	if want := "pulling 100.0 MB / 300.0 MB from galaxy (12.0 MB/s)"; !strings.HasPrefix(lines[0], want) {
		t.Fatalf("summary = %q, want prefix %q", lines[0], want)
	}
	if want := "  Documents: transfer, 1357/1920 actions, pulled 945/1920 (100.0 MB / 600.0 MB), skipped 412 (in flight from other peers)"; lines[1] != want {
		t.Fatalf("root line = %q, want %q", lines[1], want)
	}
}

// LOC-65: the transfer-failure note says how long ago, why, and when next.
func TestRetryNote(t *testing.T) {
	now := time.Now()
	failed := now.Add(-32 * time.Minute)
	next := now.Add(12 * time.Second)
	p := &peer.Entry{Failures: 2, LastError: `Get "http://x/v1/manifest": dial tcp: connect: connection refused`, TransportReason: "connection refused", LastTransportErrorAt: &failed, NextRetryAt: &next, RetryWait: "backoff"}
	if got, want := retryNote(p, now), "transfer failed 32m ago (connection refused); next attempt in 12s"; got != want {
		t.Fatalf("note = %q, want %q", got, want)
	}
	p.NextRetryAt, p.RetryWait = nil, "round"
	if got := retryNote(p, now); !strings.HasSuffix(got, "next attempt after the current round with it") {
		t.Fatalf("note while a round runs = %q", got)
	}
	// A daemon from before LOC-65 sends neither time nor reason.
	old := &peer.Entry{Failures: 1, LastError: "boom"}
	if got := retryNote(old, now); got != "transfer failed (boom); retrying" {
		t.Fatalf("note without schedule = %q", got)
	}
}

func TestHumanDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		12 * time.Second:                "12s",
		32 * time.Minute:                "32m",
		80 * time.Minute:                "1h20m",
		50 * time.Hour:                  "2d",
		-time.Second:                    "0s",
		59*time.Minute + 40*time.Second: "1h00m",
	} {
		if got := humanDuration(d); got != want {
			t.Errorf("humanDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

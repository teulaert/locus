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
	if got := progressSummary(p); got != "pulling 1.0 KB / 2.0 KB from aaf711ba" {
		t.Fatalf("pull summary between rate samples = %q", got)
	}
	p.Stalled = true
	if got := progressSummary(p); got != "pulling 1.0 KB / 2.0 KB from aaf711ba (stalled)" {
		t.Fatalf("stalled pull summary = %q", got)
	}
}

// LOC-85: a round whose pushes the peer all answers "held" sends nothing
// and is not stalled: the summary says what is already there, and how many
// files are done, instead of "pushing 0 B / 4.8 GB to hermes (stalled)".
func TestProgressSummaryAllHeld(t *testing.T) {
	const mb = 1 << 20
	p := syncpkg.PeerProgress{
		PeerID: "95f91b2400", Hostname: "hermes", Phase: "transfer",
		PushBytesPlanned: 4915 * mb, SkippedPushBytes: 4900 * mb, SkippedHeldBytes: 4900 * mb, SkippedHeld: 1150,
		FilesPlanned: 1152, FilesDone: 1150, ActionsPlanned: 1152, ActionsDone: 1150,
	}
	if got, want := progressSummary(p), "pushing to hermes: 0 B sent, 4.8 GB already held there (1150/1152 files)"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}
	// Some sent, more held: the bytes to send leave the held ones out, and
	// the rate and ETA are about the bytes to send.
	p.PushedBytes, p.BytesPerSec, p.ETASeconds = 120*mb, 3.1*mb, 5*60
	p.PushBytesPlanned, p.SkippedPushBytes, p.SkippedHeldBytes = 3482*mb, 2355*mb, 2355*mb
	if got, want := progressSummary(p), "pushing 120.0 MB / 1.1 GB to hermes (3.1 MB/s, ~5m; 2.3 GB already held there)"; got != want {
		t.Fatalf("mixed summary = %q, want %q", got, want)
	}
	// Every way of handling content without sending it is named.
	p.SkippedReceiving, p.SkippedReceivingBytes, p.SkippedPushBytes = 2, 10*mb, 2365*mb
	p.PullBytesPlanned, p.SkippedBytes, p.SkippedInFlight = 30*mb, 20*mb, 4
	if got, want := progressSummary(p), "transferring 120.0 MB / 1.1 GB with hermes (3.1 MB/s, ~5m; 2.3 GB already held there, 10.0 MB on its way there from another peer, 20.0 MB coming in from another peer)"; got != want {
		t.Fatalf("summary with every skip = %q, want %q", got, want)
	}
	// A daemon from before LOC-85 reports only the skipped push total.
	old := syncpkg.PeerProgress{Hostname: "hermes", PushBytesPlanned: 4915 * mb, SkippedPushBytes: 4900 * mb}
	if got, want := progressSummary(old), "pushing to hermes: 0 B sent, 4.8 GB not needed there"; got != want {
		t.Fatalf("summary from an older daemon = %q, want %q", got, want)
	}
	// Nothing sent, nothing finished for the window: still stalled.
	q := syncpkg.PeerProgress{Hostname: "hermes", Phase: "transfer", PushBytesPlanned: 4915 * mb, SkippedPushBytes: 10 * mb, SkippedHeldBytes: 10 * mb, SkippedHeld: 3, FilesPlanned: 1152, FilesDone: 3, Stalled: true}
	if got, want := progressSummary(q), "pushing to hermes: 0 B sent, 10.0 MB already held there (3/1152 files, stalled)"; got != want {
		t.Fatalf("stalled summary = %q, want %q", got, want)
	}
	q.SkippedPushBytes, q.SkippedHeldBytes, q.SkippedHeld = 0, 0, 0
	if got, want := progressSummary(q), "pushing 0 B / 4.8 GB to hermes (stalled)"; got != want {
		t.Fatalf("stalled summary without skips = %q, want %q", got, want)
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
	if want := "pulling 100.0 MB / 300.0 MB from galaxy (12.0 MB/s; 300.0 MB coming in from another peer), started"; !strings.HasPrefix(lines[0], want) {
		t.Fatalf("summary = %q, want prefix %q", lines[0], want)
	}
	if want := "  Documents: transfer, 1357/1920 actions, pulled 945/1920 (100.0 MB / 600.0 MB), skipped 412 (in flight from other peers)"; lines[1] != want {
		t.Fatalf("root line = %q, want %q", lines[1], want)
	}
}

// LOC-78: pushes the peer did not need (it held the content, or another
// peer was sending it) are reported per root and in the round summary, and
// are not counted in the bytes still to come.
func TestProgressDetailSkippedPushes(t *testing.T) {
	const mb = 1 << 20
	p := syncpkg.PeerProgress{
		PeerID: "aaf711ba00", Hostname: "joiner", StartedAt: time.Now().Add(-40 * time.Second),
		PushBytesPlanned: 600 * mb, PushedBytes: 100 * mb, SkippedPushBytes: 300 * mb, BytesPerSec: 12 * mb,
		SkippedHeld: 90, SkippedHeldBytes: 290 * mb, SkippedReceiving: 1, SkippedReceivingBytes: 10 * mb,
		Roots: []syncpkg.RootProgress{{
			Root: "Documents", State: "transfer", Planned: 200, Done: 150, PushPlanned: 200, Pushed: 50,
			PushedBytes: 100 * mb, PushBytesPlanned: 600 * mb, SkippedHeld: 90, SkippedReceiving: 1, SkippedPushBytes: 300 * mb,
		}},
	}
	lines := progressDetail(p, time.Now())
	if want := "pushing 100.0 MB / 300.0 MB to joiner (12.0 MB/s; 290.0 MB already held there, 10.0 MB on its way there from another peer), started"; !strings.HasPrefix(lines[0], want) {
		t.Fatalf("summary = %q, want prefix %q", lines[0], want)
	}
	if want := "  Documents: transfer, 150/200 actions, pushed 50/200 (100.0 MB / 600.0 MB), skipped 90 pushes (peer already holds them), 1 push (peer receiving them from another peer)"; lines[1] != want {
		t.Fatalf("root line = %q, want %q", lines[1], want)
	}
	st := &syncpkg.Stats{SkippedInFlight: 2, SkippedHeld: 90}
	if got, want := skippedNote(st, " "), " skipped 2 (in flight from other peers), 90 pushes (peer already holds them)"; got != want {
		t.Fatalf("round note = %q, want %q", got, want)
	}
	if got := skippedNote(&syncpkg.Stats{}, " "); got != "" {
		t.Fatalf("note for a round that skipped nothing = %q", got)
	}
}

// LOC-82: files adopted from a local copy are reported per root and in the
// round notes, and are not counted in the bytes still to come, in either
// direction.
func TestProgressDetailAdopted(t *testing.T) {
	const mb = 1 << 20
	p := syncpkg.PeerProgress{
		PeerID: "aaf711ba00", Hostname: "orbit", StartedAt: time.Now().Add(-40 * time.Second),
		PullBytesPlanned: 500 * mb, PulledBytes: 10 * mb, PushBytesPlanned: 200 * mb, PushedBytes: 20 * mb,
		Adopted: 1196, AdoptedBytes: 450 * mb, AdoptedPushBytes: 150 * mb, BytesPerSec: 2 * mb,
		Roots: []syncpkg.RootProgress{{
			Root: "Documents", State: "transfer", Planned: 2400, Done: 2392, PullPlanned: 1200, Pulled: 4,
			PulledBytes: 10 * mb, PullBytesPlanned: 500 * mb, Adopted: 1196, AdoptedBytes: 450 * mb,
		}},
	}
	lines := progressDetail(p, time.Now())
	if want := "transferring 30.0 MB / 250.0 MB with orbit (2.0 MB/s"; !strings.HasPrefix(lines[0], want) {
		t.Fatalf("summary = %q, want prefix %q", lines[0], want)
	}
	if want := "  Documents: transfer, 2392/2400 actions, pulled 4/1200 (10.0 MB / 500.0 MB), adopted 1196 files (content already present locally)"; lines[1] != want {
		t.Fatalf("root line = %q, want %q", lines[1], want)
	}
	if got, want := adoptedNote(1), "adopted 1 file (content already present locally)"; got != want {
		t.Fatalf("note = %q, want %q", got, want)
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

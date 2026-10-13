package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// humanBytes renders a byte count with a binary-scaled unit (1.2 GB).
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTP"[exp])
}

// humanDuration renders a duration coarsely: 12s, 32m, 2h05m, 3d.
func humanDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d = d.Round(time.Second); d < time.Minute {
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
	if d = d.Round(time.Minute); d < time.Hour {
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
	if d < 48*time.Hour {
		return fmt.Sprintf("%dh%02dm", int(d/time.Hour), int(d%time.Hour/time.Minute))
	}
	return fmt.Sprintf("%dd", int(d.Round(24*time.Hour)/(24*time.Hour)))
}

// peerName is the hostname of a peer in progress, or its short ID.
func peerName(p syncpkg.PeerProgress) string {
	if p.Hostname != "" {
		return p.Hostname
	}
	return identity.Short(p.PeerID)
}

// handled is planned content a round dealt with without sending it over
// the link (LOC-85): label says how, as read after the bytes ("4.8 GB
// already held there"), files how many files that was.
type handled struct {
	label string
	bytes int64
	files int
}

// handledWithoutTransfer lists the ways a peer's round handled planned
// content without moving it, leaving out those that did not occur. Another
// way is one more entry here.
func handledWithoutTransfer(p syncpkg.PeerProgress) []handled {
	all := []handled{
		{"already held there", p.SkippedHeldBytes, p.SkippedHeld},
		{"on its way there from another peer", p.SkippedReceivingBytes, p.SkippedReceiving},
		// A daemon from before LOC-85 reports only the push total.
		{"not needed there", p.SkippedPushBytes - p.SkippedHeldBytes - p.SkippedReceivingBytes, 0},
		{"coming in from another peer", p.SkippedBytes, p.SkippedInFlight},
		// Content the receiving side produced from its own copy under
		// another path, as after a rename (LOC-82).
		{"already present locally", p.AdoptedBytes, p.Adopted},
	}
	var out []handled
	for _, h := range all {
		if h.bytes > 0 || h.files > 0 {
			out = append(out, h)
		}
	}
	return out
}

// progressSummary is one line about a peer's part of the running round, e.g.
// "pushing 1.2 GB / 5.6 GB to MacBook (0.9 MB/s, ~80m)". Content handled
// without being sent (LOC-77, LOC-78) is left out of the bytes to send and
// named after the rate: "pushing 120.0 MB / 1.1 GB to hermes (3.1 MB/s,
// ~5m; 2.3 GB already held there)". While nothing was sent but something
// was handled that way, the line counts files instead: "pushing to hermes:
// 0 B sent, 4.8 GB already held there (1150/1152 files)".
func progressSummary(p syncpkg.PeerProgress) string {
	name := peerName(p)
	push, pull := p.PushBytesPlanned, p.PullBytesPlanned
	var (
		verb, prep string
		sent       int64
	)
	switch {
	case push > 0 && pull > 0:
		verb, prep, sent = "transferring", "with", p.PushedBytes+p.PulledBytes
	case push > 0:
		verb, prep, sent = "pushing", "to", p.PushedBytes
	case pull > 0:
		verb, prep, sent = "pulling", "from", p.PulledBytes
	default:
		done, planned := 0, 0
		for _, r := range p.Roots {
			done += r.Done
			planned += r.Planned
		}
		if planned > 0 {
			s := fmt.Sprintf("applying %d / %d changes with %s", done, planned, name)
			if p.Stalled {
				s += " (stalled)"
			}
			return s
		}
		return fmt.Sprintf("exchanging manifests with %s (root %s)", name, p.Root)
	}
	var without []string
	for _, h := range handledWithoutTransfer(p) {
		without = append(without, humanBytes(h.bytes)+" "+h.label)
	}
	if sent == 0 && len(without) > 0 {
		s := fmt.Sprintf("%s %s %s: 0 B sent, %s", verb, prep, name, strings.Join(without, ", "))
		var extra []string
		if p.FilesPlanned > 0 {
			extra = append(extra, fmt.Sprintf("%d/%d files", p.FilesDone, p.FilesPlanned))
		}
		if p.Stalled {
			extra = append(extra, "stalled")
		}
		if len(extra) > 0 {
			s += " (" + strings.Join(extra, ", ") + ")"
		}
		return s
	}
	// The rate and ETA cover the bytes still to send only: skipped (LOC-77,
	// LOC-78) and adopted (LOC-82) content is not waited for.
	toSend := push - p.SkippedPushBytes - p.AdoptedPushBytes + pull - p.SkippedBytes - (p.AdoptedBytes - p.AdoptedPushBytes)
	var extra []string
	if p.BytesPerSec > 0 {
		extra = append(extra, humanBytes(int64(p.BytesPerSec))+"/s")
	}
	if p.ETASeconds > 0 {
		extra = append(extra, "~"+humanDuration(time.Duration(p.ETASeconds)*time.Second))
	}
	if p.Stalled {
		extra = append(extra, "stalled")
	}
	note := strings.Join(extra, ", ")
	if len(without) > 0 {
		if note != "" {
			note += "; "
		}
		note += strings.Join(without, ", ")
	}
	s := fmt.Sprintf("%s %s / %s %s %s", verb, humanBytes(sent), humanBytes(toSend), prep, name)
	if note != "" {
		s += " (" + note + ")"
	}
	return s
}

// progressDetail lists a peer's roots under its summary for `sync status`.
func progressDetail(p syncpkg.PeerProgress, now time.Time) []string {
	out := []string{fmt.Sprintf("%s, started %s ago", progressSummary(p), humanDuration(now.Sub(p.StartedAt)))}
	for _, r := range p.Roots {
		line := fmt.Sprintf("  %s: %s", r.Root, r.State)
		if r.State != "pending" && r.State != "manifest" {
			line += fmt.Sprintf(", %d/%d actions", r.Done, r.Planned)
			if r.PushPlanned > 0 {
				line += fmt.Sprintf(", pushed %d/%d (%s / %s)", r.Pushed, r.PushPlanned, humanBytes(r.PushedBytes), humanBytes(r.PushBytesPlanned))
			}
			if r.PullPlanned > 0 {
				line += fmt.Sprintf(", pulled %d/%d (%s / %s)", r.Pulled, r.PullPlanned, humanBytes(r.PulledBytes), humanBytes(r.PullBytesPlanned))
			}
			if parts := skippedParts(r.SkippedInFlight, r.SkippedHeld, r.SkippedReceiving); len(parts) > 0 {
				line += ", skipped " + strings.Join(parts, ", ")
			}
			if r.Adopted > 0 {
				line += ", " + adoptedNote(r.Adopted)
			}
			if r.DeletePlanned > 0 {
				line += fmt.Sprintf(", deleted %d/%d", r.Deleted, r.DeletePlanned)
			}
			if r.Failed > 0 {
				line += fmt.Sprintf(", %d failed", r.Failed)
			}
		}
		out = append(out, line)
	}
	if p.CurrentFile != "" {
		out = append(out, fmt.Sprintf("  now: %s %s", p.CurrentOp, p.CurrentFile))
	}
	return out
}

// retryNote says when a peer's transfers last failed and when the next
// attempt is due (LOC-65), e.g. "transfer failed 32m ago (connection
// refused); next attempt in 12s".
func retryNote(p *peer.Entry, now time.Time) string {
	reason := p.TransportReason
	if reason == "" {
		reason = p.LastError
	}
	failed := "transfer failed"
	if p.LastTransportErrorAt != nil {
		failed += " " + humanDuration(now.Sub(*p.LastTransportErrorAt)) + " ago"
	}
	if reason != "" {
		failed += " (" + reason + ")"
	}
	var next string
	switch {
	case p.RetryWait == "round":
		next = "next attempt after the current round with it"
	case p.NextRetryAt != nil && p.NextRetryAt.After(now):
		next = "next attempt in " + humanDuration(p.NextRetryAt.Sub(now))
	case p.NextRetryAt != nil:
		next = "next attempt due now"
	default:
		next = "retrying"
	}
	return failed + "; " + next
}

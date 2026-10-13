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

// progressSummary is one line about a peer's part of the running round, e.g.
// "pushing 1.2 GB / 5.6 GB to MacBook (0.9 MB/s, ~80m)".
func progressSummary(p syncpkg.PeerProgress) string {
	name := peerName(p)
	// Pulls left to another peer's round (LOC-77), pushes the peer did not
	// need (LOC-78), and content adopted from a local copy (LOC-82) are not
	// waited for.
	push := p.PushBytesPlanned - p.SkippedPushBytes - p.AdoptedPushBytes
	pull := p.PullBytesPlanned - p.SkippedBytes - (p.AdoptedBytes - p.AdoptedPushBytes)
	var what string
	switch {
	case push > 0 && pull > 0:
		what = fmt.Sprintf("transferring %s / %s with %s", humanBytes(p.PushedBytes+p.PulledBytes), humanBytes(push+pull), name)
	case push > 0:
		what = fmt.Sprintf("pushing %s / %s to %s", humanBytes(p.PushedBytes), humanBytes(push), name)
	case pull > 0:
		what = fmt.Sprintf("pulling %s / %s from %s", humanBytes(p.PulledBytes), humanBytes(pull), name)
	default:
		done, planned := 0, 0
		for _, r := range p.Roots {
			done += r.Done
			planned += r.Planned
		}
		if planned > 0 {
			return fmt.Sprintf("applying %d / %d changes with %s", done, planned, name)
		}
		return fmt.Sprintf("exchanging manifests with %s (root %s)", name, p.Root)
	}
	var extra []string
	if p.BytesPerSec > 0 {
		extra = append(extra, humanBytes(int64(p.BytesPerSec))+"/s")
	} else {
		extra = append(extra, "stalled")
	}
	if p.ETASeconds > 0 {
		extra = append(extra, "~"+humanDuration(time.Duration(p.ETASeconds)*time.Second))
	}
	return fmt.Sprintf("%s (%s)", what, strings.Join(extra, ", "))
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

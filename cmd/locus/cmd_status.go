package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
)

func cmdStatus(ctx context.Context, a *app.App, g *globalOptions) int {
	st, err := statusOf(ctx, a)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitError
	}
	if g.json {
		printJSON(st)
		return exitOK
	}
	fmt.Printf("peer    %s (%s)  %s\n", a.Hostname, identity.Short(a.Identity.PeerID), st.Version)
	fmt.Printf("data    %s\n", st.DataDir)
	for _, r := range st.Roots {
		fmt.Printf("root    %s  %s  %d files, %d deleted, %s\n", r.Name, r.Path, r.Files, r.Deleted, r.SyncState)
	}
	fmt.Printf("index   %d documents, %d chunks, %d embeddings (%d stale), model %s\n",
		st.Index.Documents, st.Index.Chunks, st.Index.Embeddings, st.Index.Stale, a.Embedder.ID())
	fmt.Printf("device  %s (requested %s, gpu found %v)\n", st.Embedding.Used, st.Embedding.Requested, st.Embedding.GPUFound)
	if st.Watch != nil {
		fmt.Printf("watch   %s\n", watchSummary(st.Watch, a.Cfg.ReconcileInterval.D()))
	}
	for _, p := range st.Peers {
		fmt.Printf("peer    %s %s %s %s%s\n", identity.Short(p.PeerID), p.Hostname, p.State, p.Addr, unreachableNote(p))
	}
	for _, p := range st.Progress {
		fmt.Printf("sync    %s\n", progressSummary(p))
	}
	fmt.Printf("conflicts %d\n", st.Conflicts)
	return exitOK
}

// statusOf is the running daemon's status when there is one (LOC-57), so a
// round in progress and the last round show; otherwise this process's own.
func statusOf(ctx context.Context, a *app.App) (*app.Status, error) {
	var st app.Status
	if live(ctx, a, "/api/v1/status", &st) {
		return &st, nil
	}
	return a.BuildStatus(ctx)
}

// watchSummary describes the daemon's filesystem watcher: its mode, how many
// directories it watches and, where watches hold file descriptors, the cap
// at which it falls back to polling (LOC-70).
func watchSummary(w *fswatch.Stats, reconcileEvery time.Duration) string {
	if w.Mode == fswatch.ModePoll {
		s := fmt.Sprintf("poll (changes found by reconciliation every %s)", reconcileEvery)
		if w.Reason != "" {
			s += ": " + w.Reason
		}
		return s
	}
	unit := "directories"
	if w.Watches == 1 {
		unit = "directory"
	}
	s := fmt.Sprintf("%s, %d %s", w.Mode, w.Watches, unit)
	if w.MaxWatches > 0 {
		s += fmt.Sprintf(" (falls back to polling at %d; descriptor limit %d)", w.MaxWatches, w.FDLimit)
	}
	if w.Mode == fswatch.ModeKqueueDirs {
		s += fmt.Sprintf("; in-place edits found by reconciliation every %s", reconcileEvery)
	}
	return s
}

// unreachableNote explains a peer that answers discovery probes but cannot be
// reached for transfers, which is what a one-directional firewall looks like,
// or one that speaks a sync protocol this build does not.
func unreachableNote(p *peer.Entry) string {
	if p != nil && p.State == peer.Incompatible {
		if p.LastError != "" {
			return fmt.Sprintf("  [not synchronizing; %s]", p.LastError)
		}
		return fmt.Sprintf("  [not synchronizing; incompatible protocol (discovery %d, sync %d)]", p.Protocol, p.SyncProtocol)
	}
	if p == nil || p.Failures == 0 {
		return ""
	}
	return fmt.Sprintf("  [%s]", retryNote(p, time.Now()))
}

func cmdPeers(ctx context.Context, a *app.App, g *globalOptions) int {
	var fromDaemon struct {
		Peers []*peer.Entry `json:"peers"`
	}
	var entries []*peer.Entry
	if live(ctx, a, "/api/v1/peers", &fromDaemon) && fromDaemon.Peers != nil {
		entries = fromDaemon.Peers
	} else {
		entries = a.PresenceSnapshot(ctx)
	}
	if g.json {
		printJSON(map[string]any{"peers": entries})
		return exitOK
	}
	if len(entries) == 0 {
		fmt.Println("no other peers have been observed")
		return exitNoResults
	}
	for _, p := range entries {
		fmt.Printf("%-10s %-24s %-14s %s%s\n", identity.Short(p.PeerID), p.Hostname, p.State, p.SyncAddr, unreachableNote(p))
	}
	return exitOK
}

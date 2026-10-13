package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/teulaert/locus/internal/app"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

func cmdSync(ctx context.Context, a *app.App, args []string, g *globalOptions) int {
	sub := "status"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "status":
		st, err := statusOf(ctx, a)
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		// The daemon marks every root synchronizing while a round runs.
		syncing := len(st.Progress) > 0
		for _, r := range st.Roots {
			syncing = syncing || r.SyncState == "synchronizing"
		}
		progress := st.Progress
		if progress == nil {
			progress = []syncpkg.PeerProgress{}
		}
		if g.json {
			printJSON(map[string]any{"roots": st.Roots, "last_sync": st.LastSync, "syncing": syncing, "progress": progress})
			return exitOK
		}
		if a.Cfg.Rendezvous {
			fmt.Println("rendezvous peer, no roots: nothing to synchronize")
		}
		if syncing {
			fmt.Println("synchronization in progress")
			now := time.Now()
			for _, p := range progress {
				for _, line := range progressDetail(p, now) {
					fmt.Println(line)
				}
			}
		}
		for _, r := range st.Roots {
			fmt.Printf("%s (%s): %d files, %s\n", r.Name, r.Path, r.Files, r.SyncState)
		}
		if st.LastSync != nil {
			skipped := skippedNote(st.LastSync, " ")
			fmt.Printf("last round: pushed %d pulled %d deleted %d conflicted %d%s (%s)\n",
				st.LastSync.Pushed, st.LastSync.Pulled, st.LastSync.Deleted, st.LastSync.Conflicted, skipped, st.LastSync.Duration)
		}
		return exitOK
	case "now":
		stats, err := syncNow(ctx, a)
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		if g.json {
			printJSON(stats)
		} else {
			fmt.Printf("peers %d · pushed %d · pulled %d · deleted %d · conflicted %d · noop %d · %s\n",
				stats.Peers, stats.Pushed, stats.Pulled, stats.Deleted, stats.Conflicted, stats.Noop, stats.Duration)
			if note := skippedNote(stats, ""); note != "" {
				fmt.Println(note)
			}
			for _, e := range stats.Errors {
				fmt.Println("warning:", e)
			}
			if len(stats.Busy) > 0 {
				fmt.Println("skipped, already in a running round:", strings.Join(stats.Busy, ", "))
			}
		}
		// The exit code is the same whatever the output format.
		if stats.Conflicted > 0 {
			return exitConflict
		}
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "locus: unknown sync subcommand %q\n", sub)
		return exitUsage
	}
}

// syncNow asks a running daemon for the round (POST /api/v1/sync), so the
// CLI does not race it with a second engine over the same databases; with no
// daemon, or one whose API cannot be reached, the round runs here.
func syncNow(ctx context.Context, a *app.App) (*syncpkg.Stats, error) {
	if d := daemonFor(a); d != nil {
		var stats syncpkg.Stats
		err := d.do(ctx, "POST", "/api/v1/sync", &stats)
		if err == nil {
			return &stats, nil
		}
		if !errors.Is(err, errDaemonAPI) {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "locus: %v; running the round in this process instead\n", err)
	}
	return a.SyncNow(ctx)
}

// skippedNote describes the transfers a round left out because their
// content was already here, on its way, or held by the peer (LOC-77,
// LOC-78), e.g. "skipped 3 (in flight from other peers), 12 pushes (peer
// already holds them)"; it is empty when there were none. prefix goes in
// front of a non-empty note.
func skippedNote(st *syncpkg.Stats, prefix string) string {
	parts := skippedParts(st.SkippedInFlight, st.SkippedHeld, st.SkippedReceiving)
	if len(parts) == 0 {
		return ""
	}
	return prefix + "skipped " + strings.Join(parts, ", ")
}

// skippedParts lists the non-zero skip counts in words.
func skippedParts(inFlight, held, receiving int) []string {
	var parts []string
	if inFlight > 0 {
		parts = append(parts, fmt.Sprintf("%d (in flight from other peers)", inFlight))
	}
	if held > 0 {
		parts = append(parts, fmt.Sprintf("%d %s (peer already holds them)", held, plural(held, "push", "pushes")))
	}
	if receiving > 0 {
		parts = append(parts, fmt.Sprintf("%d %s (peer receiving them from another peer)", receiving, plural(receiving, "push", "pushes")))
	}
	return parts
}

// plural picks the singular or plural form for n.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

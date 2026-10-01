package main

import (
	"context"
	"fmt"
	"os"

	"github.com/teulaert/locus/internal/app"
)

func cmdIndex(ctx context.Context, a *app.App, args []string, g *globalOptions) int {
	sub := "status"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "status":
		st, err := a.IndexStatus(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		if g.json {
			printJSON(st)
			return exitOK
		}
		stats, jobs := st.Index, st.Jobs
		fmt.Printf("documents %d · chunks %d · embeddings %d (%d stale) · no text %d\n", stats.Documents, stats.Chunks, stats.Embeddings, stats.Stale, stats.NoText)
		fmt.Printf("model %s (chunk version %d)\n", stats.Model, stats.ChunkVer)
		for kind, states := range jobs {
			for state, n := range states {
				fmt.Printf("queue %s %s: %d\n", kind, state, n)
			}
		}
		return exitOK
	case "rebuild":
		if err := a.RebuildIndex(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		stats, err := a.Store.GetIndexStats(a.Embedder.ID())
		if err != nil {
			return exitError
		}
		if g.json {
			printJSON(stats)
			return exitOK
		}
		fmt.Printf("rebuilt: %d documents, %d chunks, %d embeddings\n", stats.Documents, stats.Chunks, stats.Embeddings)
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "locus: unknown index subcommand %q\n", sub)
		return exitUsage
	}
}

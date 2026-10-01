package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/store"
)

func cmdConflicts(ctx context.Context, a *app.App, args []string, g *globalOptions) int {
	if len(args) == 0 {
		list, err := a.Store.ListConflicts()
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		if g.json {
			if list == nil {
				list = []*store.Conflict{}
			}
			printJSON(map[string]any{"conflicts": list})
			if len(list) > 0 {
				return exitConflict
			}
			return exitOK
		}
		if len(list) == 0 {
			fmt.Println("no conflicts")
			return exitOK
		}
		for _, c := range list {
			fmt.Printf("%s  %s\n    normal:   %s (%s, writer %s)\n    sibling:  %s (%s, writer %s)\n",
				c.ID, time.Unix(c.CreatedAt, 0).Format(time.RFC3339), c.LocalPath, short(c.LocalHash), identity.Short(c.LocalWriter),
				c.ConflictPath, short(c.ConflictHash), identity.Short(c.ConflictWriter))
		}
		return exitConflict
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "show":
		if len(rest) != 1 {
			fmt.Fprintln(os.Stderr, "locus: conflicts show <id>")
			return exitUsage
		}
		c, err := a.Store.GetConflict(rest[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		if g.json {
			printJSON(c)
			return exitOK
		}
		fmt.Printf("id:            %s\n", c.ID)
		fmt.Printf("path:          %s\n", c.RelPath)
		fmt.Printf("normal:        %s (hash %s, writer %s)\n", c.LocalPath, short(c.LocalHash), c.LocalWriter)
		fmt.Printf("conflict copy: %s (hash %s, writer %s)\n", c.ConflictPath, short(c.ConflictHash), c.ConflictWriter)
		fmt.Printf("created:       %s\n", time.Unix(c.CreatedAt, 0).Format(time.RFC3339))
		return exitOK
	case "resolve":
		if len(rest) != 1 {
			fmt.Fprintln(os.Stderr, "locus: conflicts resolve <id>")
			return exitUsage
		}
		if err := a.Store.ResolveConflict(rest[0]); err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		if g.json {
			printJSON(map[string]string{"resolved": rest[0]})
		} else {
			fmt.Printf("resolved %s (normal-path version kept)\n", rest[0])
		}
		return exitOK
	default:
		fmt.Fprintf(os.Stderr, "locus: unknown conflicts subcommand %q\n", sub)
		return exitUsage
	}
}

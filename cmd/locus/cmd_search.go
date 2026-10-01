package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/search"
)

func cmdSearch(ctx context.Context, a *app.App, args []string, g *globalOptions) int {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		asContext = fs.Bool("context", false, "compact passages for LLM context")
		pathPref  = fs.String("path", "", "restrict to a path prefix")
		typ       = fs.String("type", "", "restrict to a file extension (pdf, md, txt)")
		limit     = fs.Int("limit", 10, "maximum documents")
		budget    = fs.Int("budget", 8000, "character budget for --context")
	)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `usage: locus search <query> [flags]

  --json             stable JSON with provenance (paths, pages, sections)
  --context          compact high-quality passages for an LLM context window
  --path <prefix>    filter by path prefix
  --type <ext>       filter by file extension
  --limit <n>        maximum documents (default 10)
  --budget <chars>   --context character budget (default 8000)

Exit codes: 0 results, 3 no results, 1 error.
`)
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	query := strings.Join(fs.Args(), " ")
	if strings.TrimSpace(query) == "" {
		fmt.Fprintln(os.Stderr, "locus: search requires a query")
		return exitUsage
	}
	opts := search.Options{
		PathPrefix: *pathPref,
		Type:       *typ,
		Limit:      *limit,
	}
	if *asContext {
		resp, err := a.SearchWithContext(ctx, query, opts, *budget)
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitError
		}
		switch {
		case g.json:
			printJSON(resp)
		case len(resp.Results) == 0:
			fmt.Println("no results")
		default:
			for _, block := range resp.Context {
				fmt.Println(block)
				fmt.Println()
			}
		}
		return searchExit(&resp.SearchResponse)
	}
	resp, err := a.SearchDocuments(ctx, query, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitError
	}
	switch {
	case g.json:
		printJSON(resp)
	case len(resp.Results) == 0:
		fmt.Println("no results")
	default:
		for _, r := range resp.Results {
			fmt.Printf("%s  (%.3f)\n", r.Path, r.Score)
			for _, c := range r.Chunks {
				label := ""
				if c.Page > 0 {
					label = fmt.Sprintf("p.%d", c.Page)
				} else if c.Section != "" {
					label = "§ " + c.Section
				}
				fmt.Printf("    %s %s\n", label, strings.ReplaceAll(c.Snippet, "\n", " "))
			}
		}
	}
	return searchExit(resp)
}

// searchExit is exit 3 for a search that found nothing, else exit 0.
func searchExit(resp *app.SearchResponse) int {
	if len(resp.Results) == 0 {
		return exitNoResults
	}
	return exitOK
}

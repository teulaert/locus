// Command locus is the Locus peer daemon and its agent-friendly CLI.
//
// Locus is a local-first, peer-to-peer document system: files stay ordinary
// files on disk, synchronization replicates files, and all search state is
// local, derived and disposable.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/teulaert/locus/internal/agent"
	"github.com/teulaert/locus/internal/api"
	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/search"
	"github.com/teulaert/locus/internal/store"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// Exit codes, stable and documented (see SKILL.md).
const (
	exitOK        = 0
	exitError     = 1
	exitUsage     = 2
	exitNoResults = 3
	exitConfig    = 4
	exitConflict  = 5
)

func main() {
	os.Exit(run(os.Args[1:]))
}

type globalOptions struct {
	configPath string
	dataDir    string
	json       bool
	quiet      bool
	timeout    time.Duration
}

func run(argv []string) int {
	g := &globalOptions{}
	fs := flag.NewFlagSet("locus", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&g.configPath, "config", "", "path to config file (default: platform config dir/config.json)")
	fs.StringVar(&g.dataDir, "data-dir", "", "override the application data directory")
	fs.BoolVar(&g.json, "json", false, "machine-readable JSON output")
	fs.BoolVar(&g.quiet, "quiet", false, "suppress human progress output")
	fs.DurationVar(&g.timeout, "timeout", 2*time.Minute, "operation timeout")
	fs.Usage = usage
	if err := fs.Parse(argv); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			usage()
			return exitOK
		}
		return exitUsage
	}
	args := fs.Args()
	if len(args) == 0 {
		usage()
		return exitUsage
	}
	cmd, rest := args[0], args[1:]
	// Output-shaping flags work in either position, because agents naturally
	// write `locus search x --json` and `locus --json search x` alike.
	rest = extractOutputFlags(rest, g)
	// Flag parsing stops at the first positional argument, which would make
	// `locus search <query> --limit 5` silently treat --limit as part of the
	// query. Hoisting flags ahead of positionals keeps both orders working.
	rest = hoistFlags(rest)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if os.Getenv("LOCUS_LOG_LEVEL") == "info" {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	switch cmd {
	case "version", "--version":
		fmt.Printf("locus %s\n", syncpkg.BuildVersion)
		return exitOK
	case "help", "-h":
		usage()
		return exitOK
	case "skill":
		return cmdSkill(g, rest)
	case "init":
		return cmdInit(g, rest)
	case "serve":
		return cmdServe(ctx, g, rest, log)
	}

	// All remaining commands operate through the shared service layer.
	cfg, err := loadConfig(g)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	a, err := app.New(cfg, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	defer a.Close()

	cctx, ccancel := context.WithTimeout(ctx, g.timeout)
	defer ccancel()

	switch cmd {
	case "status":
		return cmdStatus(cctx, a, g)
	case "peers":
		return cmdPeers(cctx, a, g)
	case "sync":
		return cmdSync(cctx, a, rest, g)
	case "search":
		return cmdSearch(cctx, a, rest, g)
	case "index":
		return cmdIndex(cctx, a, rest, g)
	case "conflicts":
		return cmdConflicts(cctx, a, rest, g)
	default:
		fmt.Fprintf(os.Stderr, "locus: unknown command %q\n\n", cmd)
		usage()
		return exitUsage
	}
}

// extractOutputFlags removes the output-shaping boolean flags from args
// wherever they appear, so global and trailing positions agree.
func extractOutputFlags(args []string, g *globalOptions) []string {
	rest := make([]string, 0, len(args))
	for _, a := range args {
		switch a {
		case "--json", "-json":
			g.json = true
		case "--quiet", "-quiet":
			g.quiet = true
		default:
			rest = append(rest, a)
		}
	}
	return rest
}

// valueFlags are the flags that consume the following argument. Names are
// global because no two commands spell the same flag differently.
var valueFlags = map[string]bool{
	"--config": true, "--data-dir": true, "--timeout": true, "--root": true,
	"--peer-port": true, "--api-port": true, "--section": true, "--path": true,
	"--type": true, "--limit": true, "--budget": true,
}

// hoistFlags returns the flag arguments followed by the positional ones.
func hoistFlags(args []string) []string {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			positional = append(positional, args[i+1:]...)
			return append(flags, positional...)
		case strings.HasPrefix(a, "-") && a != "-" && len(a) > 1:
			name := a
			if j := strings.IndexByte(a, '='); j > 0 {
				name = a[:j]
			}
			flags = append(flags, a)
			if valueFlags[name] && !strings.Contains(a, "=") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		default:
			positional = append(positional, a)
		}
	}
	return append(flags, positional...)
}

func usage() {
	fmt.Fprint(os.Stderr, `locus — local-first peer-to-peer documents, search and sync

Usage:
  locus [global flags] <command> [args]

Global flags:
  --config <path>      configuration file (default: ~/.config/locus/config.json)
  --data-dir <dir>     override the application data directory
  --json               stable machine-readable JSON on stdout (no decoration; may
                       also be given after the command)
  --quiet              suppress human progress output
  --timeout <dur>      operation timeout (default 2m)

Commands:
  init                 create a configuration file with sync roots
    --root <dir>         synchronization root (repeatable; default ~/Documents)
                         or <name>=<dir> to pair with peers by that name
    --peer-port <n>      peer synchronization port
    --force              overwrite an existing configuration file
  serve                run the peer daemon (discovery, sync, indexing, API, PWA)
  status               peer, root, index and conflict status
  peers                reachable peers and their presence state
  sync status          local synchronization state
  sync now             reconcile locally and exchange changes with online peers
  search <query>       hybrid lexical + semantic search over indexed documents
    --json               JSON results with provenance
    --context            compact passages for LLM context insertion
    --path <prefix>      restrict to a path prefix
    --type <ext>         restrict to a file extension (pdf, md, txt, ...)
    --limit <n>          maximum documents (default 10)
    --budget <chars>     --context character budget (default 8000)
  index status         derived index state (documents, chunks, embeddings)
  index rebuild        delete and reconstruct the search index from local files
  conflicts            list unresolved conflicts
  conflicts show <id>  show one conflict
  conflicts resolve <id>  accept the current normal-path version
  skill                print the agent skill documentation
  version              print version

Exit codes:
  0 success · 1 error · 2 usage error · 3 no results · 4 configuration problem · 5 unresolved conflicts

Environment:
  LOCUS_CONFIG_DIR, LOCUS_DATA_DIR, LOCUS_LOG_LEVEL=info, LOCUS_WG_INTERFACE
`)
}

// cmdSkill prints the agent-facing documentation embedded in the binary, so
// that discovering the interface never requires an extra download.
func cmdSkill(g *globalOptions, args []string) int {
	fs := flag.NewFlagSet("skill", flag.ContinueOnError)
	section := fs.String("section", "", "print only one section, e.g. \"exit codes\"")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *section != "" {
		body := agent.Section(*section)
		if body == "" {
			fmt.Fprintf(os.Stderr, "locus: no such section %q\n", *section)
			return exitUsage
		}
		if g.json {
			printJSON(map[string]any{"section": *section, "body": body})
		} else {
			fmt.Println(body)
		}
		return exitOK
	}
	if g.json {
		printJSON(map[string]any{"skill": agent.Skill()})
	} else {
		fmt.Print(agent.Skill())
	}
	return exitOK
}

// ---------- init / serve ----------

func cmdInit(g *globalOptions, args []string) int {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	var roots rootList
	fs.Var(&roots, "root", "synchronization root as PATH or NAME=PATH (repeatable)")
	peerPort := fs.Int("peer-port", 0, "peer synchronization port")
	force := fs.Bool("force", false, "overwrite an existing configuration file")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	path := g.configPath
	if path == "" {
		p, err := config.Path()
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
		path = p
	}
	// A configuration carries state init cannot recreate (the API token,
	// configured peers, authorized peers), so never replace one silently.
	if _, err := os.Stat(path); err == nil && !*force {
		fmt.Fprintf(os.Stderr, "locus: configuration %s already exists; pass --force to overwrite it\n", path)
		return exitConfig
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	cfg := config.Default()
	if *peerPort > 0 {
		cfg.Peer.Port = *peerPort
	}
	// --data-dir is persisted so later commands without the flag use the same
	// data directory, and therefore the same identity, as this one.
	if g.dataDir != "" {
		abs, err := filepath.Abs(g.dataDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
		cfg.DataDir = abs
	}
	cfg.Roots = roots
	if len(cfg.Roots) == 0 {
		home, _ := os.UserHomeDir()
		cfg.Roots = config.PathRoots(filepath.Join(home, "Documents"))
	}
	for _, r := range cfg.Roots {
		if r.Path == "" {
			fmt.Fprintf(os.Stderr, "locus: --root %q has an empty path\n", r.String())
			return exitConfig
		}
		if err := os.MkdirAll(r.Path, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	if err := cfg.Save(path); err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	dataDir, err := cfg.ResolveDataDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	id, err := identity.Load(dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	out := map[string]any{"config": path, "data_dir": dataDir, "peer_id": id.PeerID, "roots": cfg.Roots}
	if g.json {
		printJSON(out)
	} else {
		fmt.Printf("initialized Locus peer\n  config: %s\n  data:   %s\n  peer:   %s\n  roots:  %s\n", path, dataDir, identity.Short(id.PeerID), rootList(cfg.Roots).String())
	}
	return exitOK
}

// rootList collects repeated --root flags, each PATH or NAME=PATH.
type rootList []config.Root

func (r *rootList) Set(v string) error { *r = append(*r, config.ParseRoot(v)); return nil }
func (r rootList) String() string {
	parts := make([]string, len(r))
	for i, root := range r {
		parts[i] = root.String()
	}
	return strings.Join(parts, ", ")
}

func cmdServe(ctx context.Context, g *globalOptions, args []string, log *slog.Logger) int {
	cfg, err := loadConfig(g)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	a, err := app.New(cfg, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	defer a.Close()
	h, err := api.Handler(a)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus: serve:", err)
		return exitError
	}
	if err := a.Serve(ctx, app.WithAPI(h)); err != nil {
		fmt.Fprintln(os.Stderr, "locus: serve:", err)
		return exitError
	}
	return exitOK
}

func loadConfig(g *globalOptions) (*config.Config, error) {
	path := g.configPath
	if path == "" {
		p, err := config.Path()
		if err != nil {
			return nil, err
		}
		path = p
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	if g.dataDir != "" {
		cfg.DataDir = g.dataDir
	}
	return cfg, nil
}

// ---------- status / peers ----------

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
	for _, p := range st.Peers {
		fmt.Printf("peer    %s %s %s %s%s\n", identity.Short(p.PeerID), p.Hostname, p.State, p.Addr, unreachableNote(p))
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
	return fmt.Sprintf("  [not reachable for transfer, retrying; %s]", p.LastError)
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

// ---------- sync ----------

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
		syncing := a.Sync.Busy()
		if st.Daemon {
			// The daemon marks every root synchronizing while a round runs.
			syncing = false
			for _, r := range st.Roots {
				syncing = syncing || r.SyncState == "synchronizing"
			}
		}
		if g.json {
			printJSON(map[string]any{"roots": st.Roots, "last_sync": st.LastSync, "syncing": syncing})
			return exitOK
		}
		if syncing {
			fmt.Println("synchronization in progress")
			return exitOK
		}
		for _, r := range st.Roots {
			fmt.Printf("%s (%s): %d files, %s\n", r.Name, r.Path, r.Files, r.SyncState)
		}
		if st.LastSync != nil {
			fmt.Printf("last round: pushed %d pulled %d deleted %d conflicted %d (%s)\n",
				st.LastSync.Pushed, st.LastSync.Pulled, st.LastSync.Deleted, st.LastSync.Conflicted, st.LastSync.Duration)
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
			for _, e := range stats.Errors {
				fmt.Println("warning:", e)
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

// ---------- search ----------

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

// ---------- index ----------

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

// ---------- conflicts ----------

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

func short(h string) string {
	if len(h) <= 8 {
		return h
	}
	return h[:8]
}

// printJSON writes v as indented JSON. Documented collections are kept
// non-nil by the typed responses in package app, not here.
func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
	}
}

var _ = strconv.Itoa

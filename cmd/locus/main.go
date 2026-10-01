// Command locus is the Locus peer daemon and its agent-friendly CLI.
//
// Locus is a local-first, peer-to-peer document system: files stay ordinary
// files on disk, synchronization replicates files, and all search state is
// local, derived and disposable.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
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

var _ = strconv.Itoa

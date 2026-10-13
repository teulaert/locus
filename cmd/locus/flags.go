package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type globalOptions struct {
	configPath string
	dataDir    string
	json       bool
	quiet      bool
	timeout    time.Duration
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
	"--peer-port": true, "--discovery-peer": true, "--section": true, "--path": true,
	"--type": true, "--limit": true, "--budget": true, "--user": true,
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
    --discovery-peer <host[:port]>
                         discovery target, typically the always-on peer
                         (repeatable; other peers are learned through it).
                         Not needed when a WireGuard route leads to the hub:
                         its gateway is probed automatically
    --force              overwrite an existing configuration file
    --system             system service: /etc/locus/config.json and /var/lib/locus,
                         owned by --user (default locus); run as root, needs
                         a root unless the peer is a rendezvous peer
    --rendezvous         discovery-only peer for the hub of a hub-and-spoke
                         WireGuard network: no roots, no sync
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

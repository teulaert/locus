package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/identity"
)

func cmdInit(g *globalOptions, args []string) int {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	var roots rootList
	fs.Var(&roots, "root", "synchronization root as PATH or NAME=PATH (repeatable)")
	peerPort := fs.Int("peer-port", 0, "peer synchronization port")
	var discPeers stringList
	fs.Var(&discPeers, "discovery-peer", "discovery target HOST or HOST:PORT, typically the always-on peer (repeatable)")
	force := fs.Bool("force", false, "overwrite an existing configuration file")
	system := fs.Bool("system", false, "configure a system service: "+systemConfigPath+" and "+systemDataDir+" owned by --user")
	userName := fs.String("user", defaultServiceUser, "service user for --system")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	var svc *serviceUser
	if *system {
		if runtime.GOOS == "windows" {
			fmt.Fprintln(os.Stderr, "locus: init --system is for Unix service managers; run init as the user who runs the service")
			return exitUsage
		}
		// The service user's home is not where documents live; a server
		// peer names its roots explicitly.
		if len(roots) == 0 {
			fmt.Fprintln(os.Stderr, "locus: init --system needs at least one --root")
			return exitConfig
		}
		u, err := lookupServiceUser(*userName)
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
		svc = u
	}
	path := g.configPath
	if path == "" && *system {
		path = systemConfigPath
	} else if path == "" {
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
	} else if *system {
		cfg.DataDir = systemDataDir
	}
	cfg.Discovery.Peers = discPeers
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
		_, statErr := os.Stat(r.Path)
		if err := os.MkdirAll(r.Path, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
		if svc == nil {
			continue
		}
		// A root init creates belongs to the service; an existing one is
		// the operator's to arrange, since chowning documents is not ours
		// to decide.
		if errors.Is(statErr, os.ErrNotExist) {
			if err := os.Chown(r.Path, svc.uid, svc.gid); err != nil {
				fmt.Fprintln(os.Stderr, "locus:", err)
				return exitConfig
			}
		} else if !svc.owns(r.Path) {
			fmt.Fprintf(os.Stderr, "locus: warning: root %s is not owned by %s; the service needs to write it\n", r.Path, svc.name)
		}
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	dataDir, err := cfg.ResolveDataDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	// Creating the identity in another user's data directory would leave it
	// owned by the wrong user (LOC-64). --system hands the directory to the
	// service user below instead.
	if svc == nil {
		if err := app.CheckDataDirOwner(dataDir); err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
	} else if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		// The service user must be able to reach its configuration, which
		// Save would otherwise put in a root-only 0700 directory.
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	if err := cfg.Save(path); err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	id, err := identity.Load(dataDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	if svc != nil {
		if err := os.Chown(path, svc.uid, svc.gid); err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
		if err := svc.chownTree(dataDir); err != nil {
			fmt.Fprintln(os.Stderr, "locus:", err)
			return exitConfig
		}
	}
	// What other peers will be told to dial (LOC-63). With the default
	// peer.addr 0.0.0.0 it is chosen from the route to the discovery peers,
	// so say when it is only the default route's address.
	adv := app.AdvertiseHost(cfg, cfg.Discovery.Peers)
	advAddr := net.JoinHostPort(adv.Host, strconv.Itoa(cfg.Peer.Port))
	out := map[string]any{"config": path, "data_dir": dataDir, "peer_id": id.PeerID, "roots": cfg.Roots,
		"advertised_addr": advAddr, "advertised_by": adv.Source}
	if svc != nil {
		out["user"] = svc.name
	}
	if g.json {
		printJSON(out)
		return exitOK
	}
	fallbackNote := func() {
		if adv.Fallback {
			fmt.Println("  note: this is the default route's address; if it is not this machine's WireGuard address, set peer.addr or peer.interface in the config")
		}
	}
	if svc == nil {
		fmt.Printf("initialized Locus peer\n  config:    %s\n  data:      %s\n  peer:      %s\n  roots:     %s\n  advertise: %s (%s)\n", path, dataDir, identity.Short(id.PeerID), rootList(cfg.Roots).String(), advAddr, adv.Source)
		fallbackNote()
		return exitOK
	}
	// Other peers need the full id for their authorized_peers.
	fmt.Printf("initialized Locus system peer\n  config:    %s\n  data:      %s\n  user:      %s\n  peer:      %s\n  roots:     %s\n  advertise: %s (%s)\n", path, dataDir, svc.name, id.PeerID, rootList(cfg.Roots).String(), advAddr, adv.Source)
	fallbackNote()
	fmt.Printf("next: set peer.addr, discovery.peers and authorized_peers in %s, then start the service\n", path)
	return exitOK
}

// stringList collects a repeated string flag.
type stringList []string

func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }
func (l stringList) String() string      { return strings.Join(l, ", ") }

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

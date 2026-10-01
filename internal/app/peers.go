package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/peer"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

func (a *App) discoveryTargets(roots []string) []string {
	_ = roots
	port := a.Cfg.Discovery.Port
	targets := append([]string{}, a.Cfg.Discovery.Peers...)
	if iface := os.Getenv("LOCUS_WG_INTERFACE"); iface != "" {
		if addrs, err := discovery.WireGuardPeers(iface, port); err == nil {
			targets = append(targets, addrs...)
		}
	} else if ifaces := discovery.LocalWireGuardInterfaces(); len(ifaces) > 0 {
		for _, iface := range ifaces {
			if addrs, err := discovery.WireGuardPeers(iface, port); err == nil {
				targets = append(targets, addrs...)
			}
		}
	}
	return discovery.Targets(targets, port)
}

// PeerInfo returns this peer's discovery announcement. Receivers take only
// the port of SyncAddr; the host is replaced by the address the probe or
// reply travelled over (discovery.BindSyncAddr), so the default-route guess
// below need not be the WireGuard address.
func (a *App) PeerInfo() peer.Info {
	return peer.Info{
		PeerID:       a.Identity.PeerID,
		Hostname:     a.Hostname,
		Version:      syncpkg.BuildVersion,
		SyncAddr:     net.JoinHostPort(a.syncHost(), fmt.Sprint(a.Cfg.Peer.Port)),
		Capabilities: []string{"sync", "search", "api", peer.SyncProtocolCapability(syncpkg.ProtocolVersion)},
	}
}

func (a *App) syncHost() string {
	if a.Cfg.Peer.Addr == "0.0.0.0" {
		return outboundIP()
	}
	return a.Cfg.Peer.Addr
}

func outboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if ta, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return ta.IP.String()
	}
	return "127.0.0.1"
}

func (a *App) hostnames() map[string]string {
	out := map[string]string{}
	// Include this peer: a conflict whose losing side is our own version is
	// named after our hostname, and presence only holds remote peers.
	if a.Hostname != "" {
		out[a.Identity.PeerID] = a.Hostname
	}
	for _, e := range a.Presence.Snapshot() {
		if e.Hostname != "" {
			out[e.PeerID] = e.Hostname
		}
	}
	return out
}

// peerRetryDelay grows exponentially while a peer stays unreachable, and is
// capped so a peer that comes back is noticed within a few minutes.
func peerRetryDelay(failures int) time.Duration {
	const (
		base  = 15 * time.Second
		limit = 5 * time.Minute
	)
	if failures < 1 {
		failures = 1
	}
	d := base << uint(failures-1)
	if d <= 0 || d > limit {
		return limit
	}
	return d
}

// dueForSync drops peers still inside their transport-backoff window.
func (a *App) dueForSync(peers []*peer.Entry) []*peer.Entry {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.peerBackoff) == 0 {
		return peers
	}
	now := time.Now()
	out := make([]*peer.Entry, 0, len(peers))
	for _, e := range peers {
		if until, ok := a.peerBackoff[e.PeerID]; ok && now.Before(until) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// probeOnce runs a single discovery round and returns peers considered online.
func (a *App) probeOnce(ctx context.Context) []*peer.Entry {
	prober := &discovery.Prober{
		Identity:   a.Identity,
		Info:       a.PeerInfo(),
		Table:      a.Presence,
		Targets:    a.Peers,
		Authorized: a.Cfg.AuthorizedPeers,
		Timeout:    2 * time.Second,

		SyncProtocol: syncpkg.ProtocolVersion,
	}
	prober.RoundOnce(ctx)
	return a.Presence.Online()
}

func (a *App) persistPresence() {
	b, err := json.Marshal(a.Presence.Snapshot())
	if err != nil {
		return
	}
	_ = a.Store.MetaSet(a.Store.State, metaPresence, string(b))
}

// metaPresence is the state-meta key holding the last presence snapshot.
const metaPresence = "presence.snapshot"

// PresenceSnapshot returns live presence when available, falling back to the
// snapshot persisted by a running daemon. The result is never nil.
func (a *App) PresenceSnapshot(ctx context.Context) []*peer.Entry {
	if len(a.Peers) > 0 {
		if online := a.probeOnce(ctx); len(online) > 0 || len(a.Presence.Snapshot()) > 0 {
			return a.Presence.Snapshot()
		}
	}
	saved, err := a.Store.MetaGet(a.Store.State, metaPresence)
	if err != nil || saved == "" {
		return []*peer.Entry{}
	}
	var entries []*peer.Entry
	if err := json.Unmarshal([]byte(saved), &entries); err != nil || entries == nil {
		return []*peer.Entry{}
	}
	return entries
}

// Hostnames maps known peer IDs to hostnames (used by conflict naming and UI).
func (a *App) Hostnames() map[string]string { return a.hostnames() }

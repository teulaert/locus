package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/store"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

func (a *App) discoveryTargets() []string {
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
// reply travelled over (discovery.BindSyncAddr). The discovery port is
// announced so a responder can probe this peer back, and tell others where
// to, although a probe's source port is ephemeral.
func (a *App) PeerInfo() peer.Info {
	return peer.Info{
		PeerID:       a.Identity.PeerID,
		Hostname:     a.Hostname,
		Version:      syncpkg.BuildVersion,
		SyncAddr:     net.JoinHostPort(a.Advertised().Host, fmt.Sprint(a.Cfg.Peer.Port)),
		Capabilities: a.capabilities(),
	}
}

func (a *App) capabilities() []string {
	if a.Cfg.Rendezvous {
		return a.rendezvousCapabilities()
	}
	return []string{
		peer.SyncCapability, "search", "api",
		peer.SyncProtocolCapability(syncpkg.ProtocolVersion),
		peer.PeerExchangeCapability,
		peer.DiscoveryPortCapability(a.Cfg.Discovery.Port),
	}
}

// Advertised returns the host this peer announces, chosen once per process
// (see AdvertiseHost) from the configured and learned discovery targets.
func (a *App) Advertised() Advertised {
	a.advertiseOnce.Do(func() {
		hints := append([]string{}, a.Cfg.Discovery.Peers...)
		hints = append(hints, a.Peers...)
		hints = append(hints, a.Book.Derived()...)
		for _, l := range a.Book.Snapshot() {
			hints = append(hints, l.DiscAddr)
		}
		a.advertised = AdvertiseHost(a.Cfg, hints)
	})
	return a.advertised
}

// discoveryBook returns the address book with the current configured
// targets (tests replace a.Peers after New).
func (a *App) discoveryBook() *discovery.Book {
	a.Book.SetConfigured(a.Peers)
	return a.Book
}

// observeProbe records an inbound probe: presence, and the prober's
// discovery address (the host it probed from, which BindSyncAddr already put
// in its SyncAddr, and the port it announces) in the address book.
func (a *App) observeProbe(addr string, p *discovery.Packet) {
	a.Presence.Observe(addr, p.Info, p.Protocol, discovery.Compatible(p, syncpkg.ProtocolVersion))
	disc := ""
	if port := p.Info.DiscoveryPort(); port > 0 {
		if host, _, err := net.SplitHostPort(p.Info.SyncAddr); err == nil {
			disc = net.JoinHostPort(host, strconv.Itoa(port))
		}
	}
	a.Book.ObserveDirect(p.Info.PeerID, disc, p.Info.SyncAddr, p.Info.Hostname)
}

// loadLearnedPeers fills the address book from state.db.
func (a *App) loadLearnedPeers() {
	rows, err := a.Store.ListLearnedPeers()
	if err != nil {
		a.Log.Warn("load learned peers", "err", err)
		return
	}
	entries := make([]discovery.Learned, len(rows))
	for i, r := range rows {
		entries[i] = discovery.Learned{
			KnownPeer: discovery.KnownPeer{PeerID: r.PeerID, DiscAddr: r.DiscoveryAddr, SyncAddr: r.SyncAddr},
			Hostname:  r.Hostname,
			Via:       r.Via,
			LastSeen:  r.LastSeen,
			LearnedAt: r.LearnedAt,
		}
	}
	a.Book.Load(entries)
}

// persistLearnedPeers writes the address book to state.db when it changed
// (or always, with force).
func (a *App) persistLearnedPeers(force bool) {
	a.Book.Prune()
	if !a.Book.TakeDirty() && !force {
		return
	}
	snap := a.Book.Snapshot()
	rows := make([]store.LearnedPeer, len(snap))
	for i, l := range snap {
		rows[i] = store.LearnedPeer{PeerID: l.PeerID, DiscoveryAddr: l.DiscAddr, SyncAddr: l.SyncAddr, Hostname: l.Hostname, Via: l.Via, LastSeen: l.LastSeen, LearnedAt: l.LearnedAt}
	}
	if err := a.Store.ReplaceLearnedPeers(rows); err != nil {
		a.Log.Warn("persist learned peers", "err", err)
	}
}

func (a *App) hostnames() map[string]string {
	out := map[string]string{}
	// Include this peer: presence only holds remote peers.
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

// dueForSync drops peers still inside their transport-backoff window and
// peers a round is already synchronizing with.
func (a *App) dueForSync(peers []*peer.Entry) []*peer.Entry {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	out := make([]*peer.Entry, 0, len(peers))
	for _, e := range peers {
		if until, ok := a.peerBackoff[e.PeerID]; ok && now.Before(until) {
			continue
		}
		if a.Sync.BusyWith(e.PeerID) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// annotateRetries fills in, for every peer with outstanding transport
// failures, when the next attempt is due and what it waits for (LOC-65).
// The entries must be copies (Presence.Snapshot returns copies).
func (a *App) annotateRetries(entries []*peer.Entry) []*peer.Entry {
	a.mu.Lock()
	next, every := a.nextSyncAt, a.syncEvery
	backoff := make(map[string]time.Time, len(a.peerBackoff))
	for id, t := range a.peerBackoff {
		backoff[id] = t
	}
	a.mu.Unlock()
	now := time.Now()
	for _, e := range entries {
		e.NextRetryAt, e.RetryWait = nil, ""
		if e.Failures == 0 || e.State == peer.Incompatible {
			continue
		}
		if a.Sync.BusyWith(e.PeerID) {
			e.RetryWait = "round"
			continue
		}
		at, wait := time.Time{}, ""
		if until, ok := backoff[e.PeerID]; ok && until.After(now) {
			at, wait = until, "backoff"
		}
		if !next.IsZero() {
			// Rounds start on sync ticks only, so the attempt is the
			// first tick at or after the backoff expires.
			tick := next
			if every > 0 && tick.Before(at) {
				n := (at.Sub(tick) + every - 1) / every
				tick = tick.Add(n * every)
			}
			if wait == "" {
				wait = "interval"
			}
			at = tick
		}
		if at.IsZero() {
			continue
		}
		e.NextRetryAt, e.RetryWait = &at, wait
	}
	return entries
}

// probeOnce runs a single discovery round and returns peers considered online.
func (a *App) probeOnce(ctx context.Context) []*peer.Entry {
	prober := &discovery.Prober{
		Identity:   a.Identity,
		Info:       a.PeerInfo(),
		Table:      a.Presence,
		Book:       a.discoveryBook(),
		Derive:     a.GatewayTargets,
		Authorized: a.Cfg.AuthorizedPeers,
		Timeout:    2 * time.Second,

		SyncProtocol: syncpkg.ProtocolVersion,
	}
	prober.RoundOnce(ctx)
	return a.Presence.Online()
}

func (a *App) persistPresence() {
	b, err := json.Marshal(a.annotateRetries(a.Presence.Snapshot()))
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
	if len(a.discoveryBook().Targets()) > 0 {
		if online := a.probeOnce(ctx); len(online) > 0 || len(a.Presence.Snapshot()) > 0 {
			return a.annotateRetries(a.Presence.Snapshot())
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

// Hostnames maps known peer IDs to hostnames, for display. Conflict sibling
// names never use it: they must not depend on what this peer has discovered
// (LOC-73).
func (a *App) Hostnames() map[string]string { return a.hostnames() }

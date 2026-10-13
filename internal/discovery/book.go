package discovery

import (
	"encoding/hex"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"sync"
	"time"
)

// MaxSharedPeers bounds the peer list in a discovery reply. It keeps the
// reply well inside one UDP datagram.
const MaxSharedPeers = 64

// MaxLearnedPeers bounds the address book; the least recently confirmed
// entries are forgotten first.
const MaxLearnedPeers = 256

// LearnedRetention is how long a learned peer is kept without being
// confirmed again, directly or through another peer.
const LearnedRetention = 30 * 24 * time.Hour

// freshContact is how long a direct handshake keeps a peer's address
// authoritative: within it, another peer's report of a different address is
// ignored, since we just reached the peer where we think it is.
const freshContact = 2 * time.Minute

// lastSeenGranularity limits how often a repeated handshake with an already
// known address marks the book dirty, so it is not rewritten every round.
const lastSeenGranularity = 5 * time.Minute

// KnownPeer is one entry of the peer list a responder shares in its reply:
// where it reached a peer it has verified itself.
type KnownPeer struct {
	PeerID   string `json:"peer_id"`
	DiscAddr string `json:"disc_addr"`
	SyncAddr string `json:"sync_addr,omitempty"`
}

// Learned is an address book entry: a peer this one did not have configured
// but heard from (an inbound probe or a reply) or heard about (another
// peer's reply).
type Learned struct {
	KnownPeer
	Hostname string
	// LastSeen is the last verified handshake with the peer itself, 0 when
	// it has only been heard about.
	LastSeen int64
	// LearnedAt is the last time any source confirmed this address.
	LearnedAt int64
	// Via is the peer that reported the address, "" when observed directly.
	Via string
}

func (l *Learned) touched() int64 { return max(l.LastSeen, l.LearnedAt) }

// Target is one address to probe. PeerID is the peer expected to answer,
// "" for a configured address.
type Target struct {
	Addr   string
	PeerID string
}

// Book is the set of addresses a peer probes: the configured discovery
// targets plus peers learned from authenticated discovery traffic. Every
// learned address comes from a signed packet of an authorized peer, either
// about itself or as part of its peer list; nothing is scanned or guessed.
type Book struct {
	mu         sync.Mutex
	self       string
	authorized []string
	configured []string
	// derived are the gateway-derived targets (GatewayTargets), refreshed
	// every probe round.
	derived []string
	peers   map[string]*Learned
	dirty   bool
	now     func() time.Time
}

// NewBook creates an empty book for peer self, admitting only authorized
// peer IDs (all verified peers when empty).
func NewBook(self string, authorized []string) *Book {
	return &Book{self: self, authorized: authorized, peers: map[string]*Learned{}, now: time.Now}
}

// SetConfigured replaces the configured targets (normalized host:port).
func (b *Book) SetConfigured(targets []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.configured = append([]string(nil), targets...)
}

// SetDerived replaces the targets derived from the routing table
// (normalized host:port). They are probed after the configured ones.
func (b *Book) SetDerived(targets []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.derived = append([]string(nil), targets...)
}

// Derived returns the current gateway-derived targets.
func (b *Book) Derived() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.derived...)
}

// Load adds persisted entries, dropping those that are no longer admissible
// (unauthorized, malformed or expired). It does not mark the book dirty.
func (b *Book) Load(entries []Learned) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := b.now().Add(-LearnedRetention).Unix()
	for _, e := range entries {
		if !b.admissible(e.PeerID) || !validPeerAddr(e.DiscAddr, true) || e.touched() < cutoff {
			continue
		}
		c := e
		b.peers[e.PeerID] = &c
	}
	b.evictLocked()
}

// Snapshot returns the learned entries sorted by peer ID.
func (b *Book) Snapshot() []Learned {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Learned, 0, len(b.peers))
	for _, e := range b.peers {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerID < out[j].PeerID })
	return out
}

// TakeDirty reports whether the learned entries changed since the last call
// (and clears the flag), so the caller knows when to persist them.
func (b *Book) TakeDirty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	d := b.dirty
	b.dirty = false
	return d
}

// Targets returns the configured targets, then the derived ones, then every
// learned address not already among them.
func (b *Book) Targets() []Target {
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := map[string]bool{}
	out := make([]Target, 0, len(b.configured)+len(b.derived)+len(b.peers))
	for _, a := range append(append([]string(nil), b.configured...), b.derived...) {
		if !seen[a] {
			seen[a] = true
			out = append(out, Target{Addr: a})
		}
	}
	learned := make([]*Learned, 0, len(b.peers))
	for _, e := range b.peers {
		learned = append(learned, e)
	}
	sort.Slice(learned, func(i, j int) bool { return learned[i].PeerID < learned[j].PeerID })
	for _, e := range learned {
		if e.DiscAddr == "" || seen[e.DiscAddr] || !b.admissible(e.PeerID) {
			continue
		}
		seen[e.DiscAddr] = true
		out = append(out, Target{Addr: e.DiscAddr, PeerID: e.PeerID})
	}
	return out
}

// ObserveDirect records a verified handshake with peerID, reachable for
// discovery at discAddr. An empty discAddr (a probe from a peer that does not
// announce its discovery port) only refreshes an existing entry.
func (b *Book) ObserveDirect(peerID, discAddr, syncAddr, hostname string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if peerID == b.self || !b.admissible(peerID) {
		return
	}
	now := b.now().Unix()
	e, ok := b.peers[peerID]
	if !ok {
		if discAddr == "" || !validPeerAddr(discAddr, true) {
			return
		}
		e = &Learned{}
		b.peers[peerID] = e
		b.dirty = true
	}
	if discAddr != "" && validPeerAddr(discAddr, true) && e.DiscAddr != discAddr {
		e.DiscAddr = discAddr
		b.dirty = true
		// The address now belongs to this peer; an entry that still names
		// it for someone else is stale.
		for id, other := range b.peers {
			if id != peerID && other.DiscAddr == discAddr {
				delete(b.peers, id)
			}
		}
	}
	e.PeerID = peerID
	if syncAddr != "" && e.SyncAddr != syncAddr {
		e.SyncAddr = syncAddr
		b.dirty = true
	}
	if hostname != "" && e.Hostname != hostname {
		e.Hostname = hostname
		b.dirty = true
	}
	if e.Via != "" {
		e.Via = ""
		b.dirty = true
	}
	if now-e.LastSeen >= int64(lastSeenGranularity/time.Second) {
		b.dirty = true
	}
	e.LastSeen = now
	e.LearnedAt = now
	b.evictLocked()
}

// ObserveExchange records the peer list from a verified reply by peer via,
// which arrived from address from. Entries for this peer, for unauthorized or
// malformed peer IDs, and with unusable addresses are dropped; a loopback
// address is only taken from a responder on loopback itself. A peer heard
// from directly within the last few minutes keeps its address. It returns
// the number of entries that added or changed an address.
func (b *Book) ObserveExchange(via string, from net.Addr, entries []KnownPeer) int {
	loopback := false
	if ua, ok := from.(*net.UDPAddr); ok && ua.IP.IsLoopback() {
		loopback = true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	changed := 0
	for i, k := range entries {
		if i >= MaxSharedPeers {
			break
		}
		if k.PeerID == b.self || k.PeerID == via || !validPeerID(k.PeerID) || !b.admissible(k.PeerID) {
			continue
		}
		if !validPeerAddr(k.DiscAddr, loopback) {
			continue
		}
		if k.SyncAddr != "" && !validPeerAddr(k.SyncAddr, loopback) {
			k.SyncAddr = ""
		}
		e, ok := b.peers[k.PeerID]
		if ok && e.DiscAddr != k.DiscAddr && now.Sub(time.Unix(e.LastSeen, 0)) < freshContact {
			continue
		}
		if !ok {
			e = &Learned{KnownPeer: KnownPeer{PeerID: k.PeerID}}
			b.peers[k.PeerID] = e
		}
		if e.DiscAddr != k.DiscAddr {
			// Hearsay: not verified at this address until it answers.
			e.DiscAddr = k.DiscAddr
			e.Via = via
			e.LastSeen = 0
			changed++
			b.dirty = true
		}
		if k.SyncAddr != "" && e.SyncAddr != k.SyncAddr {
			e.SyncAddr = k.SyncAddr
			b.dirty = true
		}
		if now.Unix()-e.LearnedAt >= int64(lastSeenGranularity/time.Second) {
			b.dirty = true
		}
		e.LearnedAt = now.Unix()
	}
	b.evictLocked()
	return changed
}

// Share returns the peers to list in a reply to requester: those this peer
// has verified itself, most recently seen first, at most MaxSharedPeers.
// Peers only heard about are not passed on, so a stale address cannot
// circulate between peers that never reach it.
func (b *Book) Share(requester string) []KnownPeer {
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := b.now().Add(-LearnedRetention).Unix()
	var list []*Learned
	for _, e := range b.peers {
		if e.PeerID == requester || e.LastSeen == 0 || e.LastSeen < cutoff || e.DiscAddr == "" || !b.admissible(e.PeerID) {
			continue
		}
		list = append(list, e)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].LastSeen != list[j].LastSeen {
			return list[i].LastSeen > list[j].LastSeen
		}
		return list[i].PeerID < list[j].PeerID
	})
	if len(list) > MaxSharedPeers {
		list = list[:MaxSharedPeers]
	}
	out := make([]KnownPeer, len(list))
	for i, e := range list {
		out[i] = e.KnownPeer
	}
	return out
}

// Prune forgets entries not confirmed within LearnedRetention.
func (b *Book) Prune() {
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := b.now().Add(-LearnedRetention).Unix()
	for id, e := range b.peers {
		if e.touched() < cutoff {
			delete(b.peers, id)
			b.dirty = true
		}
	}
}

func (b *Book) admissible(peerID string) bool {
	return peerID != b.self && allowed(b.authorized, peerID)
}

// evictLocked keeps the book within MaxLearnedPeers, dropping the least
// recently confirmed entries.
func (b *Book) evictLocked() {
	if len(b.peers) <= MaxLearnedPeers {
		return
	}
	all := make([]*Learned, 0, len(b.peers))
	for _, e := range b.peers {
		all = append(all, e)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].touched() < all[j].touched() })
	for _, e := range all[:len(all)-MaxLearnedPeers] {
		delete(b.peers, e.PeerID)
	}
	b.dirty = true
}

// validPeerID accepts the hex encoding of an ed25519 public key.
func validPeerID(id string) bool {
	raw, err := hex.DecodeString(id)
	return err == nil && len(raw) == 32
}

// validPeerAddr accepts an IP literal with a usable port: never a host name
// (nothing is resolved on another peer's say-so), and never an unspecified,
// multicast or broadcast address. Loopback is accepted only when allowed.
func validPeerAddr(addr string, allowLoopback bool) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" {
		return false
	}
	ip = ip.Unmap()
	if ip.IsUnspecified() || ip.IsMulticast() || ip == netip.AddrFrom4([4]byte{255, 255, 255, 255}) {
		return false
	}
	if ip.IsLoopback() && !allowLoopback {
		return false
	}
	return true
}

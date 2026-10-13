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

// LearnedFailureLimit is how long a learned peer that did answer at some
// point may fail every probe before it is forgotten (LOC-80). A learned peer
// that never answered is forgotten on its first failed probe; configured
// targets are never forgotten.
const LearnedFailureLimit = time.Hour

// ownAddrRefresh is how long the list of this machine's own addresses is
// cached between lookups.
const ownAddrRefresh = 30 * time.Second

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

	// loopback is true when a configured target is on loopback, or this
	// peer itself only listens on loopback (SetLoopbackOnly), as in the
	// test harness: only then are loopback addresses learned (LOC-80).
	loopback           bool
	loopbackConfigured bool
	loopbackOnly       bool
	// ownAddrs lists this machine's interface addresses; an address on
	// one of them is never learned, whatever peer ID claims it (LOC-80).
	ownAddrs  func() []netip.Addr
	ownCache  []netip.Addr
	ownCached time.Time
	// failing holds, per learned peer, when its current run of failed
	// probes began.
	failing map[string]time.Time
}

// systemOwnAddrs lists the addresses of this machine's interfaces. It is a
// seam for tests, whose fixed addresses may be the test machine's own.
var systemOwnAddrs = func() []netip.Addr {
	var out []netip.Addr
	for _, ifc := range systemInterfaces() {
		out = append(out, ifc.Addrs...)
	}
	return out
}

// NewBook creates an empty book for peer self, admitting only authorized
// peer IDs (all verified peers when empty).
func NewBook(self string, authorized []string) *Book {
	return &Book{self: self, authorized: authorized, peers: map[string]*Learned{}, now: time.Now, ownAddrs: systemOwnAddrs, failing: map[string]time.Time{}}
}

// SetOwnAddrs replaces the source of this machine's own addresses (tests).
func (b *Book) SetOwnAddrs(f func() []netip.Addr) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ownAddrs = f
	b.ownCache, b.ownCached = nil, time.Time{}
}

// SetConfigured replaces the configured targets (normalized host:port). A
// configured target on loopback allows learning loopback addresses.
func (b *Book) SetConfigured(targets []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.configured = append([]string(nil), targets...)
	b.loopbackConfigured = false
	for _, t := range targets {
		if host, _, err := net.SplitHostPort(t); err == nil {
			if ip, err := netip.ParseAddr(host); err == nil && ip.Unmap().IsLoopback() {
				b.loopbackConfigured = true
			}
		}
	}
	b.loopback = b.loopbackConfigured || b.loopbackOnly
}

// SetLoopbackOnly says that this peer listens for sync on loopback only, so
// every peer it can work with is on loopback: loopback addresses are then
// learned even without a configured loopback target.
func (b *Book) SetLoopbackOnly(on bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.loopbackOnly = on
	b.loopback = b.loopbackConfigured || b.loopbackOnly
}

// LoopbackAllowed reports whether loopback addresses may be learned: some
// configured target is on loopback, or this peer listens on loopback only.
func (b *Book) LoopbackAllowed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.loopback
}

// AdmitAddr reports whether addr may be learned or shown as a peer's
// address: a usable IP literal that is neither one of this machine's own
// addresses nor on loopback unless loopback targets are configured.
func (b *Book) AdmitAddr(addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.admitAddrLocked(addr, b.loopback)
}

func (b *Book) admitAddrLocked(addr string, allowLoopback bool) bool {
	return validPeerAddr(addr, allowLoopback) && !b.ownLocked(addr)
}

// ownLocked reports whether addr's host is one of this machine's own
// non-loopback addresses (loopback is governed by the loopback rule).
func (b *Book) ownLocked(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	if ip.IsLoopback() || b.ownAddrs == nil {
		return false
	}
	if now := b.now(); b.ownCached.IsZero() || now.Sub(b.ownCached) > ownAddrRefresh || now.Before(b.ownCached) {
		b.ownCache, b.ownCached = b.ownAddrs(), now
	}
	for _, a := range b.ownCache {
		if a.Unmap() == ip {
			return true
		}
	}
	return false
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

// Load adds persisted entries, dropping those that are no longer admissible:
// unauthorized, malformed or expired, on loopback without a configured
// loopback target, or at one of this machine's own addresses (LOC-80). It
// returns the number of entries dropped, so the caller can remove them from
// the store. It does not mark the book dirty. Call SetConfigured first.
func (b *Book) Load(entries []Learned) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := b.now().Add(-LearnedRetention).Unix()
	dropped := 0
	for _, e := range entries {
		if !b.admissible(e.PeerID) || !b.admitAddrLocked(e.DiscAddr, b.loopback) || e.touched() < cutoff ||
			(e.SyncAddr != "" && !b.admitAddrLocked(e.SyncAddr, b.loopback)) {
			dropped++
			continue
		}
		c := e
		b.peers[e.PeerID] = &c
	}
	before := len(b.peers)
	b.evictLocked()
	return dropped + before - len(b.peers)
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
	// Never learn this machine's own addresses, nor loopback ones on a
	// network that is not configured on loopback (LOC-80).
	if discAddr != "" && !b.admitAddrLocked(discAddr, b.loopback) {
		return
	}
	if syncAddr != "" && !b.admitAddrLocked(syncAddr, b.loopback) {
		syncAddr = ""
	}
	now := b.now().Unix()
	e, ok := b.peers[peerID]
	if !ok {
		if discAddr == "" {
			return
		}
		e = &Learned{}
		b.peers[peerID] = e
		b.dirty = true
	}
	delete(b.failing, peerID)
	if discAddr != "" {
		// The address now belongs to this peer; an entry that still names
		// it for someone else is stale.
		for id, other := range b.peers {
			if id != peerID && other.DiscAddr == discAddr {
				b.deleteLocked(id)
			}
		}
	}
	if discAddr != "" && e.DiscAddr != discAddr {
		e.DiscAddr = discAddr
		b.dirty = true
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
// address is only taken from a responder on loopback itself, and only when
// a configured target is on loopback; an address of this machine is never
// taken. A peer heard from directly within the last few minutes keeps its
// address, and an address another peer answered at directly is not given to
// a peer only heard about (LOC-80). It returns the number of entries that
// added or changed an address.
func (b *Book) ObserveExchange(via string, from net.Addr, entries []KnownPeer) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	loopback := false
	if ua, ok := from.(*net.UDPAddr); ok && ua.IP.IsLoopback() {
		loopback = b.loopback
	}
	now := b.now()
	changed := 0
	for i, k := range entries {
		if i >= MaxSharedPeers {
			break
		}
		if k.PeerID == b.self || k.PeerID == via || !validPeerID(k.PeerID) || !b.admissible(k.PeerID) {
			continue
		}
		if !b.admitAddrLocked(k.DiscAddr, loopback) || b.verifiedAtLocked(k.DiscAddr, k.PeerID) {
			continue
		}
		if k.SyncAddr != "" && !b.admitAddrLocked(k.SyncAddr, loopback) {
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
			b.deleteLocked(id)
		}
	}
	for id := range b.failing {
		if _, ok := b.peers[id]; !ok {
			delete(b.failing, id)
		}
	}
}

// ObserveFailure records a failed probe of peerID's learned address addr. A
// learned peer that never answered at this address is forgotten at once;
// one that did is forgotten once its probes have failed for longer than
// LearnedFailureLimit (LOC-80). It reports whether the entry was forgotten,
// so the caller can drop the peer from presence too. Configured targets are
// not learned entries and never come here.
func (b *Book) ObserveFailure(peerID, addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.peers[peerID]
	if !ok || e.DiscAddr != addr {
		return false
	}
	now := b.now()
	if e.LastSeen == 0 {
		b.deleteLocked(peerID)
		return true
	}
	since, ok := b.failing[peerID]
	if !ok {
		b.failing[peerID] = now
		return false
	}
	if now.Sub(since) > LearnedFailureLimit {
		b.deleteLocked(peerID)
		return true
	}
	return false
}

// Forget drops the learned entry of peerID when it names addr ("" for any
// address): a probe of addr was answered by another peer, so the entry is
// stale (LOC-80). It reports whether an entry was dropped.
func (b *Book) Forget(peerID, addr string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.peers[peerID]
	if !ok || (addr != "" && e.DiscAddr != addr) {
		return false
	}
	b.deleteLocked(peerID)
	return true
}

// ForgetAddr drops every learned entry at addr, e.g. once this peer answered
// a probe of it itself: whatever peer ID the entries claim, the address is
// ours (LOC-80). It returns the peer IDs dropped.
func (b *Book) ForgetAddr(addr string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for id, e := range b.peers {
		if e.DiscAddr == addr {
			b.deleteLocked(id)
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func (b *Book) deleteLocked(peerID string) {
	if _, ok := b.peers[peerID]; ok {
		delete(b.peers, peerID)
		b.dirty = true
	}
	delete(b.failing, peerID)
}

// verifiedAtLocked reports whether a peer other than peerID answered at addr
// directly: that observation wins over hearsay naming someone else there.
func (b *Book) verifiedAtLocked(addr, peerID string) bool {
	for id, e := range b.peers {
		if id != peerID && e.DiscAddr == addr && e.LastSeen > 0 {
			return true
		}
	}
	return false
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
		delete(b.failing, e.PeerID)
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

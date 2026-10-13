package discovery

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
)

// exNode is one in-process discovery peer for the peer-exchange tests: a
// responder and a prober sharing an address book, as App wires them.
type exNode struct {
	id     *identity.Identity
	info   peer.Info
	port   int
	book   *Book
	table  *peer.Table
	prober *Prober
	cancel context.CancelFunc
}

func (n *exNode) addr() string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(n.port)) }

func newExNode(t *testing.T, configured []string, authorized []string) *exNode {
	t.Helper()
	id, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	port := freeUDPPort(t)
	info := peer.Info{
		PeerID:       id.PeerID,
		Hostname:     "h-" + identity.Short(id.PeerID),
		Version:      "test",
		SyncAddr:     "127.0.0.1:1",
		Capabilities: []string{"sync", peer.PeerExchangeCapability, peer.DiscoveryPortCapability(port)},
	}
	n := &exNode{id: id, info: info, port: port, book: NewBook(id.PeerID, authorized), table: peer.NewTable(30 * time.Second)}
	// Like the test harness, these peers live on loopback only (LOC-80).
	n.book.SetLoopbackOnly(true)
	n.book.SetConfigured(Targets(configured, port))
	n.prober = &Prober{Identity: id, Info: info, Table: n.table, Book: n.book, Authorized: authorized, Timeout: time.Second}
	n.start(t)
	return n
}

// start runs the responder; OnProbe mirrors App.observeProbe.
func (n *exNode) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	t.Cleanup(cancel)
	srv := &Server{Port: n.port, Identity: n.id, Info: n.info, Authorized: n.prober.Authorized, Share: n.book.Share}
	srv.OnProbe = func(addr string, p *Packet) {
		if p.Info.PeerID == n.id.PeerID || !n.book.AdmitAddr(addr) {
			return
		}
		n.table.Observe(addr, p.Info, p.Protocol, true)
		disc := ""
		if port := p.Info.DiscoveryPort(); port > 0 {
			host, _, _ := net.SplitHostPort(p.Info.SyncAddr)
			disc = net.JoinHostPort(host, strconv.Itoa(port))
		}
		n.book.ObserveDirect(p.Info.PeerID, disc, p.Info.SyncAddr, p.Info.Hostname)
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		t.Fatal(err)
	}
}

func (n *exNode) stop() {
	n.cancel()
}

func (n *exNode) online(peerID string) bool {
	for _, e := range n.table.Online() {
		if e.PeerID == peerID {
			return true
		}
	}
	return false
}

func rounds(ctx context.Context, nodes ...*exNode) {
	for _, n := range nodes {
		n.prober.RoundOnce(ctx)
	}
}

// LOC-62: B and C configure only A, and A configures nothing. A learns both
// from their probes and lists each to the other; B and C then reach each
// other directly, keep doing so after A stops, and still find each other
// after a restart from their persisted address books.
func TestPeerExchangeThroughAlwaysOnPeer(t *testing.T) {
	ctx := context.Background()
	a := newExNode(t, nil, nil)
	b := newExNode(t, []string{a.addr()}, nil)
	c := newExNode(t, []string{a.addr()}, nil)

	// Round 1: A learns B and C from their probes; C's probe comes after
	// B's, so C already hears of B. Round 2: B hears of C.
	rounds(ctx, b, c)
	rounds(ctx, b, c)
	if !b.online(c.id.PeerID) || !c.online(b.id.PeerID) {
		t.Fatalf("B and C do not see each other: b=%+v c=%+v", b.table.Snapshot(), c.table.Snapshot())
	}
	if !a.online(b.id.PeerID) || !a.online(c.id.PeerID) {
		t.Fatalf("A did not learn its clients from their probes: %+v", a.table.Snapshot())
	}
	if len(a.book.Targets()) != 2 {
		t.Fatalf("A's targets = %+v, want B and C", a.book.Targets())
	}

	// A goes down: B and C still probe each other directly.
	a.stop()
	time.Sleep(600 * time.Millisecond) // the responder notices cancellation within its read deadline
	rounds(ctx, b, c)
	if !b.online(c.id.PeerID) || !c.online(b.id.PeerID) {
		t.Fatal("B and C lost each other when A stopped")
	}
	for _, e := range b.table.Snapshot() {
		if e.PeerID == a.id.PeerID && e.State != peer.Offline {
			t.Fatalf("A still %s on B after stopping", e.State)
		}
	}

	// Restart B and C with only their persisted books (and A still down).
	bSaved, cSaved := b.book.Snapshot(), c.book.Snapshot()
	b.stop()
	c.stop()
	time.Sleep(600 * time.Millisecond)
	b2 := restartExNode(t, b, bSaved)
	c2 := restartExNode(t, c, cSaved)
	rounds(ctx, b2, c2)
	if !b2.online(c.id.PeerID) || !c2.online(b.id.PeerID) {
		t.Fatalf("after restart B and C do not find each other: b=%+v c=%+v", b2.table.Snapshot(), c2.table.Snapshot())
	}
}

// restartExNode brings n back on the same identity and port with a fresh
// presence table and a book loaded from saved, as App.New does from state.db.
func restartExNode(t *testing.T, n *exNode, saved []Learned) *exNode {
	t.Helper()
	r := &exNode{id: n.id, info: n.info, port: n.port, book: NewBook(n.id.PeerID, nil), table: peer.NewTable(30 * time.Second)}
	r.book.SetLoopbackOnly(true)
	r.book.SetConfigured(n.book.configured)
	r.book.Load(saved)
	r.prober = &Prober{Identity: n.id, Info: n.info, Table: r.table, Book: r.book, Timeout: time.Second}
	r.start(t)
	return r
}

// A peer list naming peers outside authorized_peers is ignored: B accepts A
// and C but A also knows D, which B never probes.
func TestPeerExchangeIgnoresUnauthorizedPeers(t *testing.T) {
	ctx := context.Background()
	a := newExNode(t, nil, nil)
	c := newExNode(t, []string{a.addr()}, nil)
	d := newExNode(t, []string{a.addr()}, nil)
	rounds(ctx, c, d)

	b := newExNode(t, []string{a.addr()}, nil)
	// Rebuild B's book and prober with an allowlist of A and C only.
	allow := []string{a.id.PeerID, c.id.PeerID}
	b.book = NewBook(b.id.PeerID, allow)
	b.book.SetConfigured([]string{a.addr()})
	b.prober.Book = b.book
	b.prober.Authorized = allow
	rounds(ctx, b)
	rounds(ctx, b)

	if !b.online(c.id.PeerID) {
		t.Fatalf("B did not learn the authorized peer C: %+v", b.table.Snapshot())
	}
	if b.table.Known(d.id.PeerID) {
		t.Fatal("B recorded unauthorized peer D")
	}
	for _, tg := range b.book.Targets() {
		if tg.Addr == d.addr() || tg.PeerID == d.id.PeerID {
			t.Fatalf("B probes unauthorized peer D at %s", tg.Addr)
		}
	}
}

// A reply carries the peer list only for a prober that says it understands
// it (older builds would fail the signature over their re-encoding), and the
// list is signed with the rest of the packet.
func TestReplyListsPeersOnlyForPeerExchangeProbers(t *testing.T) {
	ctx := context.Background()
	a := newExNode(t, nil, nil)
	b := newExNode(t, []string{a.addr()}, nil)
	rounds(ctx, b) // A learns B

	oldID, oldInfo := testInfo(t, t.TempDir())
	reply, err := ProbeOnce(ctx, a.addr(), oldID, oldInfo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Peers) != 0 {
		t.Fatalf("reply to a prober without peer exchange lists %d peers", len(reply.Peers))
	}

	newInfo := oldInfo
	newInfo.Capabilities = append(newInfo.Capabilities, peer.PeerExchangeCapability)
	reply, err = ProbeOnce(ctx, a.addr(), oldID, newInfo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Peers) != 1 || reply.Peers[0].PeerID != b.id.PeerID || reply.Peers[0].DiscAddr != b.addr() {
		t.Fatalf("reply peers = %+v, want B at %s", reply.Peers, b.addr())
	}

	// Tampering with the list breaks the signature.
	p := Packet{Magic: Magic, Protocol: Protocol, TS: 1, Info: a.info, Peers: []KnownPeer{{PeerID: b.id.PeerID, DiscAddr: b.addr()}}}
	SignPacket(&p, a.id)
	if !p.Verify() {
		t.Fatal("signed packet with peers does not verify")
	}
	p.Peers[0].DiscAddr = "127.0.0.1:9"
	if p.Verify() {
		t.Fatal("packet verified after its peer list was changed")
	}
}

func hexID(b byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = "0123456789abcdef"[int(b)%16]
	}
	return string(out)
}

func TestObserveExchangeValidatesEntries(t *testing.T) {
	self, via, good := hexID(1), hexID(2), hexID(3)
	book := NewBook(self, nil)
	remote := &net.UDPAddr{IP: net.ParseIP("10.42.0.1"), Port: 8911}
	n := book.ObserveExchange(via, remote, []KnownPeer{
		{PeerID: self, DiscAddr: "10.42.0.9:8911"},            // ourselves
		{PeerID: via, DiscAddr: "10.42.0.9:8911"},             // the responder itself
		{PeerID: "not-a-peer-id", DiscAddr: "10.42.0.9:8911"}, // malformed
		{PeerID: hexID(4), DiscAddr: "galaxy.lan:8911"},       // host name
		{PeerID: hexID(5), DiscAddr: "127.0.0.1:8911"},        // loopback from a remote responder
		{PeerID: hexID(6), DiscAddr: "0.0.0.0:8911"},          // unspecified
		{PeerID: hexID(7), DiscAddr: "224.0.0.1:8911"},        // multicast
		{PeerID: hexID(8), DiscAddr: "10.42.0.9:0"},           // no port
		{PeerID: good, DiscAddr: "10.42.0.3:8911", SyncAddr: "10.42.0.3:8909"},
	})
	if n != 1 {
		t.Fatalf("accepted %d entries, want 1: %+v", n, book.Snapshot())
	}
	ts := book.Targets()
	if len(ts) != 1 || ts[0].Addr != "10.42.0.3:8911" || ts[0].PeerID != good {
		t.Fatalf("targets = %+v", ts)
	}
	// Heard about only: not passed on to others.
	if got := book.Share(""); len(got) != 0 {
		t.Fatalf("hearsay shared: %+v", got)
	}
	book.ObserveDirect(good, "10.42.0.3:8911", "10.42.0.3:8909", "galaxy")
	if got := book.Share(""); len(got) != 1 || got[0].PeerID != good {
		t.Fatalf("verified peer not shared: %+v", got)
	}
	if got := book.Share(good); len(got) != 0 {
		t.Fatalf("peer listed to itself: %+v", got)
	}
}

// A peer reached directly a moment ago keeps its address whatever another
// peer reports; once that contact is stale, the report wins.
func TestFreshDirectContactWinsOverHearsay(t *testing.T) {
	self, via, p := hexID(1), hexID(2), hexID(3)
	now := time.Unix(1_800_000_000, 0)
	book := NewBook(self, nil)
	book.now = func() time.Time { return now }
	remote := &net.UDPAddr{IP: net.ParseIP("10.42.0.1"), Port: 8911}
	book.ObserveDirect(p, "10.42.0.3:8911", "", "")
	book.ObserveExchange(via, remote, []KnownPeer{{PeerID: p, DiscAddr: "10.42.0.77:8911"}})
	if ts := book.Targets(); len(ts) != 1 || ts[0].Addr != "10.42.0.3:8911" {
		t.Fatalf("fresh address replaced: %+v", ts)
	}
	now = now.Add(10 * time.Minute)
	book.ObserveExchange(via, remote, []KnownPeer{{PeerID: p, DiscAddr: "10.42.0.77:8911"}})
	if ts := book.Targets(); len(ts) != 1 || ts[0].Addr != "10.42.0.77:8911" {
		t.Fatalf("stale address kept: %+v", ts)
	}
}

func TestBookLoadDropsExpiredAndUnauthorized(t *testing.T) {
	self, ok, old, other := hexID(1), hexID(2), hexID(3), hexID(4)
	now := time.Now()
	book := NewBook(self, []string{ok, old})
	book.Load([]Learned{
		{KnownPeer: KnownPeer{PeerID: ok, DiscAddr: "10.42.0.2:8911"}, LastSeen: now.Unix()},
		{KnownPeer: KnownPeer{PeerID: old, DiscAddr: "10.42.0.3:8911"}, LastSeen: now.Add(-LearnedRetention - time.Hour).Unix()},
		{KnownPeer: KnownPeer{PeerID: other, DiscAddr: "10.42.0.4:8911"}, LastSeen: now.Unix()},
	})
	snap := book.Snapshot()
	if len(snap) != 1 || snap[0].PeerID != ok {
		t.Fatalf("loaded %+v, want only the fresh authorized peer", snap)
	}
	if book.TakeDirty() {
		t.Fatal("loading marked the book dirty")
	}
}

// LOC-76: gateway-derived targets come after the configured ones, are not
// repeated, and are replaced (not accumulated) every round, so a route that
// disappears stops being probed.
func TestDerivedTargets(t *testing.T) {
	b := NewBook("self", nil)
	b.SetConfigured([]string{"10.0.0.1:8911"})
	b.SetDerived([]string{"10.42.0.1:8911", "10.0.0.1:8911"})
	got := b.Targets()
	if len(got) != 2 || got[0].Addr != "10.0.0.1:8911" || got[1].Addr != "10.42.0.1:8911" {
		t.Fatalf("Targets = %+v", got)
	}
	b.SetDerived(nil)
	if got := b.Targets(); len(got) != 1 {
		t.Fatalf("Targets after the route went = %+v", got)
	}
}

// LOC-76: two clients that configure nothing find each other through the
// one address their routing table yields (Prober.Derive), the rendezvous
// peer, which lists each to the other.
func TestDeriveFindsPeersThroughGateway(t *testing.T) {
	ctx := context.Background()
	hub := newExNode(t, nil, nil)
	b := newExNode(t, nil, nil)
	c := newExNode(t, nil, nil)
	calls := 0
	for _, n := range []*exNode{b, c} {
		n.prober.Derive = func() []string { calls++; return []string{hub.addr()} }
	}
	rounds(ctx, b, c)
	rounds(ctx, b, c)
	if !b.online(c.id.PeerID) || !c.online(b.id.PeerID) {
		t.Fatalf("B and C do not see each other: b=%+v c=%+v", b.table.Snapshot(), c.table.Snapshot())
	}
	if calls != 4 {
		t.Fatalf("Derive called %d times in 4 rounds, want once per round", calls)
	}
}

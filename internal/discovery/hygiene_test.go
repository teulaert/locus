package discovery

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/netguard"
	"github.com/teulaert/locus/internal/peer"
)

// LOC-80 tests: learned-peer hygiene and the test network guard.

func ownAddrs(addrs ...string) func() []netip.Addr {
	return func() []netip.Addr {
		out := make([]netip.Addr, len(addrs))
		for i, a := range addrs {
			out[i] = netip.MustParseAddr(a)
		}
		return out
	}
}

func learnedIDs(b *Book) map[string]Learned {
	out := map[string]Learned{}
	for _, l := range b.Snapshot() {
		out[l.PeerID] = l
	}
	return out
}

// A probe answered by this peer itself (a learned address that is ours) is
// never a presence entry, and the learned address is forgotten.
func TestProberForgetsItsOwnAddress(t *testing.T) {
	n := newExNode(t, nil, nil)
	stray := hexID(0x40)
	n.book.Load([]Learned{{KnownPeer: KnownPeer{PeerID: stray, DiscAddr: n.addr()}, LastSeen: time.Now().Unix(), Via: hexID(0x41)}})
	if len(n.book.Targets()) != 1 {
		t.Fatalf("targets = %+v, want the stray entry", n.book.Targets())
	}
	n.prober.RoundOnce(context.Background())
	if snap := n.table.Snapshot(); len(snap) != 0 {
		t.Fatalf("presence after probing ourselves = %+v, want empty", snap)
	}
	if snap := n.book.Snapshot(); len(snap) != 0 {
		t.Fatalf("book still holds %+v after the address answered as ourselves", snap)
	}
	if !n.book.TakeDirty() {
		t.Fatal("forgetting the address did not mark the book for persisting")
	}
}

// An exchange never admits this peer, nor any peer at one of this machine's
// own addresses; a direct observation at an own address is refused too.
func TestBookRefusesSelfAndOwnAddresses(t *testing.T) {
	self := hexID(1)
	b := NewBook(self, nil)
	b.SetOwnAddrs(ownAddrs("10.42.0.3", "192.168.1.20"))
	from := &net.UDPAddr{IP: net.ParseIP("10.42.0.1"), Port: 8911}
	b.ObserveExchange(hexID(9), from, []KnownPeer{
		{PeerID: hexID(2), DiscAddr: "10.42.0.3:8911", SyncAddr: "10.42.0.3:8909"}, // another id at our address
		{PeerID: self, DiscAddr: "10.42.0.5:8911"},                                 // ourselves elsewhere
		{PeerID: hexID(3), DiscAddr: "192.168.1.20:8911"},                          // our LAN address
		{PeerID: hexID(4), DiscAddr: "10.42.0.6:8911", SyncAddr: "10.42.0.3:8909"}, // sync address is ours
	})
	got := learnedIDs(b)
	if len(got) != 1 {
		t.Fatalf("learned %+v, want only peer 4", got)
	}
	if e, ok := got[hexID(4)]; !ok || e.SyncAddr != "" {
		t.Fatalf("peer 4 = %+v, want its own disc address and no sync address", e)
	}
	b.ObserveDirect(hexID(5), "10.42.0.3:8911", "10.42.0.3:8909", "stray")
	b.ObserveDirect(self, "10.42.0.7:8911", "10.42.0.7:8909", "me")
	if got := learnedIDs(b); len(got) != 1 {
		t.Fatalf("direct observations at our address or of ourselves were learned: %+v", got)
	}
}

// Loopback addresses are learned only when a configured target is on
// loopback or the peer listens on loopback only (the test harness).
func TestBookLearnsLoopbackOnlyWhenConfigured(t *testing.T) {
	b := NewBook(hexID(1), nil)
	lo := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 28558}
	b.ObserveDirect(hexID(2), "127.0.0.1:28558", "127.0.0.1:21036", "test")
	b.ObserveExchange(hexID(3), lo, []KnownPeer{{PeerID: hexID(4), DiscAddr: "127.0.0.1:24600"}})
	if !b.AdmitAddr("10.42.0.9:8911") || b.AdmitAddr("127.0.0.1:28558") {
		t.Fatal("AdmitAddr ignores the loopback rule")
	}
	if got := learnedIDs(b); len(got) != 0 {
		t.Fatalf("a daemon without loopback targets learned %+v", got)
	}

	b.SetConfigured([]string{"127.0.0.1:9000"})
	b.ObserveDirect(hexID(2), "127.0.0.1:28558", "127.0.0.1:21036", "test")
	b.ObserveExchange(hexID(3), lo, []KnownPeer{{PeerID: hexID(4), DiscAddr: "127.0.0.1:24600"}})
	if got := learnedIDs(b); len(got) != 2 {
		t.Fatalf("with a loopback target configured learned %+v, want peers 2 and 4", got)
	}

	c := NewBook(hexID(1), nil)
	c.SetLoopbackOnly(true)
	c.ObserveDirect(hexID(2), "127.0.0.1:28558", "127.0.0.1:21036", "test")
	if got := learnedIDs(c); len(got) != 1 {
		t.Fatalf("a loopback-only peer did not learn a loopback prober: %+v", got)
	}
}

// A learned peer that never answered is forgotten on its first failed probe;
// one that did answer, once it has failed for longer than
// LearnedFailureLimit. Both leave presence. A configured target is retried.
func TestProberForgetsFailingLearnedPeers(t *testing.T) {
	id, info := testInfo(t, t.TempDir())
	clock := time.Unix(1_800_000_000, 0)
	book := NewBook(id.PeerID, nil)
	book.now = func() time.Time { return clock }
	book.SetLoopbackOnly(true)
	dead := func() string { return net.JoinHostPort("127.0.0.1", strconv.Itoa(freeUDPPort(t))) }
	answered, hearsay, configured := dead(), dead(), dead()
	book.SetConfigured([]string{configured})
	book.Load([]Learned{
		{KnownPeer: KnownPeer{PeerID: hexID(2), DiscAddr: answered}, LastSeen: clock.Unix(), LearnedAt: clock.Unix()},
		{KnownPeer: KnownPeer{PeerID: hexID(3), DiscAddr: hearsay}, LearnedAt: clock.Unix(), Via: hexID(9)},
	})
	table := peer.NewTable(30 * time.Second)
	table.Observe(answered, peer.Info{PeerID: hexID(2), SyncAddr: "127.0.0.1:1"}, Protocol, true)
	table.Observe(configured, peer.Info{PeerID: hexID(4), SyncAddr: "127.0.0.1:1"}, Protocol, true)
	p := &Prober{Identity: id, Info: info, Table: table, Book: book, Timeout: 300 * time.Millisecond}
	ctx := context.Background()

	p.RoundOnce(ctx)
	got := learnedIDs(book)
	if _, ok := got[hexID(3)]; ok {
		t.Fatal("a learned peer that never answered survived a failed probe")
	}
	if _, ok := got[hexID(2)]; !ok {
		t.Fatal("a peer that answered before was forgotten on its first failure")
	}

	clock = clock.Add(30 * time.Minute)
	p.RoundOnce(ctx)
	if _, ok := learnedIDs(book)[hexID(2)]; !ok {
		t.Fatal("forgotten within LearnedFailureLimit")
	}
	clock = clock.Add(31 * time.Minute)
	p.RoundOnce(ctx)
	if _, ok := learnedIDs(book)[hexID(2)]; ok {
		t.Fatal("a peer failing for over an hour is still learned")
	}
	if table.Known(hexID(2)) {
		t.Fatal("the forgotten peer is still in presence")
	}
	if !book.TakeDirty() {
		t.Fatal("forgetting did not mark the book for persisting")
	}
	// The configured target is still probed and its peer still listed.
	ts := book.Targets()
	if len(ts) != 1 || ts[0].Addr != configured || !table.Known(hexID(4)) {
		t.Fatalf("configured target: targets %+v, known %v", ts, table.Known(hexID(4)))
	}
}

// A success within the failure window starts the count over.
func TestBookFailureWindowResetsOnContact(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0)
	b := NewBook(hexID(1), nil)
	b.now = func() time.Time { return clock }
	b.ObserveDirect(hexID(2), "10.42.0.9:8911", "10.42.0.9:8909", "x")
	b.ObserveFailure(hexID(2), "10.42.0.9:8911")
	clock = clock.Add(50 * time.Minute)
	b.ObserveDirect(hexID(2), "10.42.0.9:8911", "10.42.0.9:8909", "x")
	clock = clock.Add(50 * time.Minute)
	if b.ObserveFailure(hexID(2), "10.42.0.9:8911") {
		t.Fatal("forgotten although it answered 50 minutes ago")
	}
	clock = clock.Add(61 * time.Minute)
	if !b.ObserveFailure(hexID(2), "10.42.0.9:8911") {
		t.Fatal("not forgotten after failing for 61 minutes")
	}
}

// A learned entry whose address answers as another peer is dropped and only
// the peer that answered is recorded; hearsay re-offering the stale entry is
// ignored while the address is known directly.
func TestProberDropsEntryAnsweredByAnotherPeer(t *testing.T) {
	y := newExNode(t, nil, nil)
	id, info := testInfo(t, t.TempDir())
	book := NewBook(id.PeerID, nil)
	book.SetLoopbackOnly(true)
	x := hexID(0x24)
	book.Load([]Learned{{KnownPeer: KnownPeer{PeerID: x, DiscAddr: y.addr()}, LastSeen: time.Now().Unix(), Via: hexID(9)}})
	table := peer.NewTable(30 * time.Second)
	table.Observe(y.addr(), peer.Info{PeerID: x, SyncAddr: "127.0.0.1:1"}, Protocol, true)
	p := &Prober{Identity: id, Info: info, Table: table, Book: book, Timeout: time.Second}
	p.RoundOnce(context.Background())

	got := learnedIDs(book)
	if _, ok := got[x]; ok {
		t.Fatalf("stale entry kept: %+v", got)
	}
	if e, ok := got[y.id.PeerID]; !ok || e.DiscAddr != y.addr() || e.LastSeen == 0 {
		t.Fatalf("the peer that answered was not recorded: %+v", got)
	}
	if table.Known(x) || !table.Known(y.id.PeerID) {
		t.Fatalf("presence = %+v, want only the peer that answered", table.Snapshot())
	}

	lo := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}
	if n := book.ObserveExchange(hexID(9), lo, []KnownPeer{{PeerID: x, DiscAddr: y.addr()}}); n != 0 {
		t.Fatalf("exchange re-admitted the stale entry (%d changes)", n)
	}
	if _, ok := learnedIDs(book)[x]; ok {
		t.Fatal("hearsay replaced a directly verified address")
	}
}

// Load drops loopback entries (without loopback targets) and entries at
// this machine's own addresses, and reports how many, so the caller can
// delete them from the store.
func TestBookLoadDropsPollutedEntries(t *testing.T) {
	b := NewBook(hexID(1), nil)
	b.SetOwnAddrs(ownAddrs("10.42.0.3"))
	now := time.Now().Unix()
	dropped := b.Load([]Learned{
		{KnownPeer: KnownPeer{PeerID: hexID(2), DiscAddr: "127.0.0.1:28558", SyncAddr: "127.0.0.1:21036"}, LastSeen: now},
		{KnownPeer: KnownPeer{PeerID: hexID(3), DiscAddr: "10.42.0.3:8911", SyncAddr: "10.42.0.3:8909"}, LearnedAt: now, Via: hexID(9)},
		{KnownPeer: KnownPeer{PeerID: hexID(4), DiscAddr: "10.42.0.4:8911", SyncAddr: "10.42.0.3:8909"}, LastSeen: now},
		{KnownPeer: KnownPeer{PeerID: hexID(5), DiscAddr: "10.42.0.5:8911", SyncAddr: "10.42.0.5:8909"}, LastSeen: now},
	})
	if got := learnedIDs(b); dropped != 3 || len(got) != 1 {
		t.Fatalf("Load dropped %d and kept %+v, want 3 dropped and peer 5 kept", dropped, got)
	}
}

// Under the guard nothing is derived from the routing table or `wg show`,
// and only loopback addresses on non-default ports are probed or served.
func TestNetworkGuard(t *testing.T) {
	oldR, oldI := systemRoutes, systemInterfaces
	t.Cleanup(func() { systemRoutes, systemInterfaces = oldR, oldI })
	systemRoutes = func() ([]Route, error) { return parseProcNetRoute(procNetRoute), nil }
	systemInterfaces = func() []Interface { return linuxIfaces }

	t.Setenv(netguard.EnvVar, "")
	if len(GatewayTargets(8911)) == 0 {
		t.Fatal("the fake routing table yields no targets without the guard")
	}
	t.Setenv(netguard.EnvVar, "1")
	if got := GatewayTargets(8911); len(got) != 0 {
		t.Fatalf("GatewayTargets under the guard = %v", got)
	}
	if got := LocalWireGuardInterfaces(); len(got) != 0 {
		t.Fatalf("LocalWireGuardInterfaces under the guard = %v", got)
	}
	if _, err := WireGuardPeers("wg0", 8911); err == nil {
		t.Fatal("WireGuardPeers ran under the guard")
	}

	id, info := testInfo(t, t.TempDir())
	for _, addr := range []string{"10.42.0.1:8911", "192.0.2.1:9911", "127.0.0.1:8911", "127.0.0.1:8909", "localhost:9911"} {
		if _, err := ProbeOnce(context.Background(), addr, id, info, 100*time.Millisecond); !errors.Is(err, netguard.ErrBlocked) {
			t.Errorf("ProbeOnce(%s) under the guard: %v, want ErrBlocked", addr, err)
		}
	}
	table := peer.NewTable(30 * time.Second)
	book := NewBook(id.PeerID, nil)
	book.SetConfigured([]string{"10.42.0.1:9911"})
	p := &Prober{Identity: id, Info: info, Table: table, Book: book, Derive: func() []string { return []string{"10.42.0.1:8911"} }, Timeout: 100 * time.Millisecond}
	p.RoundOnce(context.Background())
	if len(table.Snapshot()) != 0 || len(book.Snapshot()) != 0 {
		t.Fatalf("a skipped probe left traces: %+v %+v", table.Snapshot(), book.Snapshot())
	}

	srv := &Server{Port: 8911, Identity: id, Info: info}
	if err := srv.ListenAndServe(context.Background()); !errors.Is(err, netguard.ErrBlocked) {
		t.Fatalf("Server on the default discovery port under the guard: %v", err)
	}
}

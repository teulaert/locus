package app

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/store"
)

// LOC-80: learned-peer hygiene at the daemon.

func probeFrom(t *testing.T, src string, discPort int) (string, *discovery.Packet) {
	t.Helper()
	id, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	host, _, _ := net.SplitHostPort(src)
	return id.PeerID, &discovery.Packet{Protocol: discovery.Protocol, Info: peer.Info{
		PeerID:       id.PeerID,
		SyncAddr:     net.JoinHostPort(host, "21036"),
		Capabilities: []string{peer.SyncCapability, peer.PeerExchangeCapability, peer.DiscoveryPortCapability(discPort)},
	}}
}

// An inbound probe carrying our own peer ID is recorded nowhere.
func TestInboundProbeFromOurselvesIsIgnored(t *testing.T) {
	a := testApp(t)
	a.observeProbe("10.42.0.3:40000", &discovery.Packet{Protocol: discovery.Protocol, Info: peer.Info{
		PeerID: a.Identity.PeerID, SyncAddr: "10.42.0.3:8909",
		Capabilities: []string{peer.SyncCapability, peer.DiscoveryPortCapability(8911)},
	}})
	if len(a.Presence.Snapshot()) != 0 || len(a.Book.Snapshot()) != 0 {
		t.Fatalf("our own probe was recorded: presence %+v, book %+v", a.Presence.Snapshot(), a.Book.Snapshot())
	}
}

// A daemon without loopback targets neither shows nor learns a prober on
// loopback (a test peer on the same machine); with a loopback target, as
// in the test harness, it does both.
func TestInboundLoopbackProberNeedsLoopbackTargets(t *testing.T) {
	a := testApp(t)
	a.Peers = nil
	a.discoveryBook()
	id, p := probeFrom(t, "127.0.0.1:51234", 28558)
	a.observeProbe("127.0.0.1:51234", p)
	if a.Presence.Known(id) || len(a.Book.Snapshot()) != 0 {
		t.Fatalf("a loopback prober was recorded: presence %+v, book %+v", a.Presence.Snapshot(), a.Book.Snapshot())
	}

	a.Peers = []string{"127.0.0.1:9911"}
	a.discoveryBook()
	a.observeProbe("127.0.0.1:51234", p)
	if !a.Presence.Known(id) {
		t.Fatal("with a loopback target the loopback prober is not in presence")
	}
	if ts := a.Book.Snapshot(); len(ts) != 1 || ts[0].DiscAddr != "127.0.0.1:28558" {
		t.Fatalf("with a loopback target the loopback prober was not learned: %+v", ts)
	}
}

// ownIPv4 returns a non-loopback address of this machine, if it has one.
func ownIPv4(t *testing.T) string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok {
				if ip, ok := netip.AddrFromSlice(ipn.IP); ok && ip.Unmap().Is4() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
					return ip.Unmap().String()
				}
			}
		}
	}
	t.Skip("no non-loopback IPv4 address on this machine")
	return ""
}

// On startup, learned peers on loopback (without loopback targets) or at
// one of this machine's own addresses are dropped from the store, which
// heals a state.db polluted by earlier builds.
func TestStartupDropsPollutedLearnedPeers(t *testing.T) {
	own := ownIPv4(t)
	a := testApp(t)
	now := time.Now().Unix()
	hex := func(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 32) }
	good := hex(0x55)
	rows := []store.LearnedPeer{
		{PeerID: hex(0x24), DiscoveryAddr: "127.0.0.1:28558", SyncAddr: "127.0.0.1:21036", LastSeen: now, LearnedAt: now},
		{PeerID: hex(0x40), DiscoveryAddr: net.JoinHostPort(own, "8911"), SyncAddr: net.JoinHostPort(own, "8909"), Via: hex(0xc4), LearnedAt: now},
		{PeerID: good, DiscoveryAddr: "192.0.2.77:8911", SyncAddr: "192.0.2.77:8909", LastSeen: now, LearnedAt: now},
	}
	if own == "192.0.2.77" {
		t.Skip("the test's peer address is this machine's")
	}
	if err := a.Store.ReplaceLearnedPeers(rows); err != nil {
		t.Fatal(err)
	}
	cfg := *a.Cfg
	a.Close()
	b, err := New(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	got, err := b.Store.ListLearnedPeers()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PeerID != good {
		t.Fatalf("learned peers in the store after restart = %+v, want only %s", got, good[:8])
	}
	if snap := b.Book.Snapshot(); len(snap) != 1 || snap[0].PeerID != good {
		t.Fatalf("book after restart = %+v", snap)
	}
}

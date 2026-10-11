package app

import (
	"net"
	"net/netip"
	"testing"

	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
)

// fakeNetwork replaces the interface and route seams with a laptop on Wi-Fi
// (default route) and WireGuard, and restores them after the test.
func fakeNetwork(t *testing.T, locals []localAddr, routes map[string]string) {
	t.Helper()
	oldIf, oldRoute, oldDefault := interfaceAddrs, routeSource, defaultRouteIP
	t.Cleanup(func() { interfaceAddrs, routeSource, defaultRouteIP = oldIf, oldRoute, oldDefault })
	interfaceAddrs = func() []localAddr { return locals }
	routeSource = func(dst netip.Addr) (netip.Addr, bool) {
		if src, ok := routes[dst.String()]; ok {
			return netip.MustParseAddr(src), true
		}
		return netip.Addr{}, false
	}
	defaultRouteIP = func() string { return "10.0.0.181" }
}

func laptop() []localAddr {
	return []localAddr{
		{Iface: "lo", Prefix: netip.MustParsePrefix("127.0.0.1/8")},
		{Iface: "wlan0", Prefix: netip.MustParsePrefix("10.0.0.181/24")},
		{Iface: "worker1-desktop", Prefix: netip.MustParsePrefix("10.42.0.3/16")},
		{Iface: "worker1-desktop", Prefix: netip.MustParsePrefix("fe80::1/64")},
	}
}

func TestAdvertiseHost(t *testing.T) {
	cfg := func(addr, iface string) *config.Config {
		c := config.Default()
		c.Peer.Addr, c.Peer.Interface = addr, iface
		return c
	}
	cases := []struct {
		name     string
		locals   []localAddr
		routes   map[string]string
		cfg      *config.Config
		hints    []string
		host     string
		fallback bool
	}{
		{"explicit peer.addr", laptop(), nil, cfg("10.42.0.3", ""), nil, "10.42.0.3", false},
		{"interface name", laptop(), nil, cfg("0.0.0.0", "worker1-desktop"), nil, "10.42.0.3", false},
		{"interface CIDR", laptop(), nil, cfg("0.0.0.0", "10.42.0.0/16"), nil, "10.42.0.3", false},
		{"prefix shared with discovery peer", laptop(), nil, cfg("0.0.0.0", ""), []string{"10.42.0.1:8911"}, "10.42.0.3", false},
		// A /32 tunnel address shares no prefix: the route towards the
		// peer still leaves through the tunnel.
		{"route to discovery peer", []localAddr{{Iface: "wg0", Prefix: netip.MustParsePrefix("10.42.0.3/32")}, {Iface: "wlan0", Prefix: netip.MustParsePrefix("10.0.0.181/24")}},
			map[string]string{"10.42.0.1": "10.42.0.3"}, cfg("0.0.0.0", ""), []string{"10.42.0.1"}, "10.42.0.3", false},
		{"host names are not resolved", laptop(), nil, cfg("0.0.0.0", ""), []string{"hub.example:8911"}, "10.0.0.181", true},
		{"no peers falls back to default route", laptop(), nil, cfg("0.0.0.0", ""), nil, "10.0.0.181", true},
		{"missing interface falls back", laptop(), nil, cfg("0.0.0.0", "utun9"), nil, "10.0.0.181", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fakeNetwork(t, c.locals, c.routes)
			got := AdvertiseHost(c.cfg, c.hints)
			if got.Host != c.host || got.Fallback != c.fallback {
				t.Fatalf("AdvertiseHost = %+v, want host %s fallback %v", got, c.host, c.fallback)
			}
		})
	}
	fakeNetwork(t, laptop(), nil)
	if got := AdvertiseHost(cfg("0.0.0.0", "utun9"), nil); got.Problem == "" {
		t.Fatalf("missing peer.interface not reported: %+v", got)
	}
}

// An inbound probe's source port is ephemeral: the prober is recorded at the
// host it probed from and the discovery port it announces, and is persisted
// so a restarted daemon probes it again.
func TestInboundProbeIsLearnedAndPersisted(t *testing.T) {
	a := testApp(t)
	id, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := &discovery.Packet{Protocol: discovery.Protocol, Info: peer.Info{
		PeerID:       id.PeerID,
		Hostname:     "macbook",
		SyncAddr:     "10.42.0.7:8909", // already bound to the probe's source
		Capabilities: []string{peer.PeerExchangeCapability, peer.DiscoveryPortCapability(9911)},
	}}
	a.observeProbe(net.JoinHostPort("10.42.0.7", "53122"), p)
	want := "10.42.0.7:9911"
	ts := a.discoveryBook().Targets()
	if len(ts) != 1 || ts[0].Addr != want || ts[0].PeerID != id.PeerID {
		t.Fatalf("targets after inbound probe = %+v, want %s", ts, want)
	}
	a.persistLearnedPeers(false)

	// A probe from a peer that does not announce its port teaches nothing.
	old, _ := identity.Load(t.TempDir())
	a.observeProbe("10.42.0.8:40000", &discovery.Packet{Protocol: discovery.Protocol, Info: peer.Info{PeerID: old.PeerID, SyncAddr: "10.42.0.8:8909"}})
	if n := len(a.discoveryBook().Targets()); n != 1 {
		t.Fatalf("peer without a discovery port became a target (%d targets)", n)
	}

	cfg := *a.Cfg
	a.Close()
	b, err := New(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.Peers = nil
	ts = b.discoveryBook().Targets()
	if len(ts) != 1 || ts[0].Addr != want {
		t.Fatalf("targets after restart = %+v, want %s", ts, want)
	}
}

// PeerInfo announces the discovery port and peer exchange.
func TestPeerInfoAnnouncesDiscoveryPort(t *testing.T) {
	a := testApp(t)
	info := a.PeerInfo()
	if info.DiscoveryPort() != a.Cfg.Discovery.Port || !info.Has(peer.PeerExchangeCapability) {
		t.Fatalf("PeerInfo capabilities = %v", info.Capabilities)
	}
}

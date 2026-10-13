package discovery

import (
	"net/netip"
	"reflect"
	"testing"
)

// Captured on a Linux desktop (galaxy) whose WireGuard interface is not
// named wg*: a /24 on-link route for its own address and a static /16
// route for the rest of the tunnel, plus Wi-Fi, a container bridge, a
// host route, a link-local route and a route through a gateway.
const procNetRoute = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT                                                       
wlp9s0	00000000	0100000A	0003	0	0	600	00000000	0	0	0                                                                           
wlp9s0	0000000A	00000000	0001	0	0	600	00FFFFFF	0	0	0                                                                           
worker1-desktop	00002A0A	00000000	0001	0	0	50	00FFFFFF	0	0	0                                                                   
worker1-desktop	00002A0A	00000000	0001	0	0	50	0000FFFF	0	0	0                                                                   
docker0	000011AC	00000000	0001	0	0	0	0000FFFF	0	0	0
wg1	0700A8C0	00000000	0005	0	0	0	FFFFFFFF	0	0	0
wg1	0000FEA9	00000000	0001	0	0	0	0000FFFF	0	0	0
wg1	00002B0A	01002A0A	0003	0	0	0	0000FFFF	0	0	0
wg1	0000000B	00000000	0001	0	0	0	000000FF	0	0	0
wg1	0000000C	00000000	0000	0	0	0	000000FF	0	0	0
`

var linuxIfaces = []Interface{
	{Name: "lo", Up: true, Loopback: true, Addrs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}},
	{Name: "wlp9s0", Up: true, HasHardwareAddr: true, Addrs: []netip.Addr{netip.MustParseAddr("10.0.0.181")}},
	{Name: "worker1-desktop", Up: true, PointToPoint: true, Addrs: []netip.Addr{netip.MustParseAddr("10.42.0.3")}},
	{Name: "docker0", Up: true, HasHardwareAddr: true, Addrs: []netip.Addr{netip.MustParseAddr("172.17.0.1")}},
	{Name: "wg1", Up: true, PointToPoint: true, Addrs: []netip.Addr{netip.MustParseAddr("192.168.0.2")}},
}

func TestParseProcNetRoute(t *testing.T) {
	routes := parseProcNetRoute(procNetRoute)
	// The route that is not up (flags 0000) is dropped.
	if len(routes) != 9 {
		t.Fatalf("parsed %d routes, want 9: %+v", len(routes), routes)
	}
	want := Route{Iface: "worker1-desktop", Dst: netip.MustParsePrefix("10.42.0.0/16")}
	if routes[3] != want {
		t.Fatalf("route 3 = %+v, want %+v", routes[3], want)
	}
	if g := routes[0].Gateway; g != netip.MustParseAddr("10.0.0.1") || routes[0].Dst.Bits() != 0 {
		t.Fatalf("default route = %+v", routes[0])
	}
	if g := routes[7].Gateway; g != netip.MustParseAddr("10.42.0.1") {
		t.Fatalf("gateway route = %+v", routes[7])
	}
}

func TestGatewayTargetsLinux(t *testing.T) {
	got := gatewayTargets(parseProcNetRoute(procNetRoute), linuxIfaces, nil, 8911)
	// worker1-desktop: 10.42.0.0/24 and /16 both give 10.42.0.1, once.
	// wg1: the /32 host route, link-local and the route that is down give
	// nothing; 10.43/16 via 10.42.0.1 is the same gateway again (deduped);
	// 11.0.0.0/24 is public. Wi-Fi and the docker bridge have link-layer
	// addresses, and their routes are not WireGuard routes; only Wi-Fi's
	// private default gateway 10.0.0.1 is added (LOC-79).
	want := []string{"10.42.0.1:8911", "10.0.0.1:8911"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gatewayTargets = %v, want %v", got, want)
	}
}

// The machine's own address is never a target: a hub with 10.42.0.1/16
// on wg0 derives nothing from its own route.
// A container on the hub has only an Ethernet interface whose default
// gateway is the hub's bridge address (LOC-79): that one address is probed.
// A public default gateway, a gateway that is the machine's own address, and
// a default route on an interface that is down give nothing.
func TestGatewayTargetsDefaultGateway(t *testing.T) {
	def := netip.MustParsePrefix("0.0.0.0/0")
	eth := []Interface{{Name: "eth0", Up: true, HasHardwareAddr: true, Addrs: []netip.Addr{netip.MustParseAddr("10.42.1.187")}}}
	routes := []Route{
		{Iface: "eth0", Dst: def, Gateway: netip.MustParseAddr("10.42.1.1")},
		{Iface: "eth0", Dst: netip.MustParsePrefix("10.42.1.0/24")},
	}
	if got, want := gatewayTargets(routes, eth, nil, 8911), []string{"10.42.1.1:8911"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("container on the hub: %v, want %v", got, want)
	}
	public := []Route{{Iface: "eth0", Dst: def, Gateway: netip.MustParseAddr("203.0.113.1")}}
	if got := gatewayTargets(public, eth, nil, 8911); len(got) != 0 {
		t.Fatalf("public default gateway probed: %v", got)
	}
	self := []Route{{Iface: "eth0", Dst: def, Gateway: netip.MustParseAddr("10.42.1.187")}}
	if got := gatewayTargets(self, eth, nil, 8911); len(got) != 0 {
		t.Fatalf("own address probed: %v", got)
	}
	down := []Interface{{Name: "eth0", HasHardwareAddr: true}}
	if got := gatewayTargets(routes, down, nil, 8911); len(got) != 0 {
		t.Fatalf("default route on a down interface probed: %v", got)
	}
	// The same gateway reached as a WireGuard route and as the default
	// gateway is listed once.
	both := []Interface{eth[0], {Name: "wg0", Up: true, PointToPoint: true, Addrs: []netip.Addr{netip.MustParseAddr("10.42.0.9")}}}
	dup := []Route{{Iface: "wg0", Dst: netip.MustParsePrefix("10.42.0.0/16")}, {Iface: "eth0", Dst: def, Gateway: netip.MustParseAddr("10.42.0.1")}}
	if got, want := gatewayTargets(dup, both, nil, 8911), []string{"10.42.0.1:8911"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dedupe: %v, want %v", got, want)
	}
}

func TestGatewayTargetsSkipsOwnAddress(t *testing.T) {
	routes := []Route{{Iface: "wg0", Dst: netip.MustParsePrefix("10.42.0.0/16")}}
	ifaces := []Interface{{Name: "wg0", Up: true, PointToPoint: true, Addrs: []netip.Addr{netip.MustParseAddr("10.42.0.1")}}}
	if got := gatewayTargets(routes, ifaces, nil, 8911); len(got) != 0 {
		t.Fatalf("hub derived %v from its own route", got)
	}
}

// peer.interface or LOCUS_WG_INTERFACE make an interface count whatever its
// flags say, by name or by a CIDR its address lies in.
func TestGatewayTargetsPreferred(t *testing.T) {
	routes := []Route{{Iface: "tun-office", Dst: netip.MustParsePrefix("192.168.50.0/24")}}
	ifaces := []Interface{{Name: "tun-office", Up: true, HasHardwareAddr: true, Addrs: []netip.Addr{netip.MustParseAddr("192.168.50.7")}}}
	if got := gatewayTargets(routes, ifaces, nil, 8911); len(got) != 0 {
		t.Fatalf("an interface with a link-layer address was taken for WireGuard: %v", got)
	}
	want := []string{"192.168.50.1:8911"}
	for _, pref := range []string{"tun-office", "192.168.50.0/24"} {
		if got := gatewayTargets(routes, ifaces, []string{"", pref}, 8911); !reflect.DeepEqual(got, want) {
			t.Fatalf("preferred %q: %v, want %v", pref, got, want)
		}
	}
}

func TestRouteTarget(t *testing.T) {
	for _, tc := range []struct {
		dst, gw string
		want    string
	}{
		{"10.42.0.0/16", "", "10.42.0.1"},
		{"10.42.0.0/24", "", "10.42.0.1"},
		{"10.42.7.9/24", "", "10.42.7.1"}, // unmasked input
		{"10.42.0.0/30", "", "10.42.0.1"},
		{"10.42.0.4/31", "", "10.42.0.4"},
		{"172.16.0.0/12", "", "172.16.0.1"},
		{"100.64.0.0/10", "", "100.64.0.1"},
		{"10.43.0.0/16", "10.42.0.1", "10.42.0.1"},
		{"10.42.0.9/32", "", ""},
		{"0.0.0.0/0", "10.0.0.1", ""},
		{"169.254.0.0/16", "", ""},
		{"127.0.0.0/8", "", ""},
		{"224.0.0.0/4", "", ""},
		{"8.8.8.0/24", "", ""},
		{"10.43.0.0/16", "8.8.8.8", ""},
	} {
		r := Route{Iface: "wg0", Dst: netip.MustParsePrefix(tc.dst)}
		if tc.gw != "" {
			r.Gateway = netip.MustParseAddr(tc.gw)
		}
		got, ok := RouteTarget(r)
		if tc.want == "" {
			if ok {
				t.Errorf("RouteTarget(%s via %q) = %s, want none", tc.dst, tc.gw, got)
			}
			continue
		}
		if !ok || got.String() != tc.want {
			t.Errorf("RouteTarget(%s via %q) = %s %v, want %s", tc.dst, tc.gw, got, ok, tc.want)
		}
		if p := r.Dst.Masked(); tc.gw == "" && p.Bits() < 31 {
			last := p.Addr()
			for p.Contains(last.Next()) {
				last = last.Next()
			}
			if got == p.Addr() || got == last {
				t.Errorf("RouteTarget(%s) = %s, the network or broadcast address", tc.dst, got)
			}
		}
	}
}

// macOS (Ventura) `netstat -rn -f inet` with the WireGuard app connected:
// utun4 carries 10.42/16; utun0-3 are system tunnels with no IPv4 routes.
const netstatVentura = `Routing tables

Internet:
Destination        Gateway            Flags               Netif Expire
default            192.168.1.1        UGScg                 en0
10.42/16           utun4              USc                 utun4
10.42.0.2          10.42.0.2          UH                  utun4
127                127.0.0.1          UCS                   lo0
127.0.0.1          127.0.0.1          UH                    lo0
169.254            link#6             UCS                   en0      !
192.168.1          link#6             UCS                   en0      !
192.168.1.1/32     link#6             UCS                   en0      !
192.168.1.1        a4:91:b1:aa:bb:cc  UHLWIir               en0   1191
192.168.1.20/32    link#6             UCS                   en0      !
224.0.0/4          link#6             UmCS                  en0      !
255.255.255.255/32 link#6             UCS                   en0      !
`

// An older layout with Refs and Use columns, a tunnel that routes through
// its own address, and an IPv6 section that must be ignored.
const netstatCatalina = `Routing tables

Internet:
Destination        Gateway            Flags        Refs      Use   Netif Expire
default            10.0.0.1           UGSc           62        0     en0
10.0.0/24          link#4             UCS             2        0     en0      !
10.42              10.42.0.2          UGSc            0        0   utun2
10.42.0.2          10.42.0.2          UH              1        0   utun2
172.20.1/24        utun3              USc             0        0   utun3

Internet6:
Destination                             Gateway                         Flags         Netif Expire
fd00:42::/64                            utun2                           USc           utun2
`

var macIfaces = []Interface{
	{Name: "lo0", Up: true, Loopback: true, Addrs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}},
	{Name: "en0", Up: true, HasHardwareAddr: true, Addrs: []netip.Addr{netip.MustParseAddr("192.168.1.20"), netip.MustParseAddr("10.0.0.5")}},
	{Name: "utun0", Up: true, PointToPoint: true},
	{Name: "utun2", Up: true, PointToPoint: true, Addrs: []netip.Addr{netip.MustParseAddr("10.42.0.2")}},
	{Name: "utun3", Up: true, PointToPoint: true, Addrs: []netip.Addr{netip.MustParseAddr("172.20.1.9")}},
	{Name: "utun4", Up: true, PointToPoint: true, Addrs: []netip.Addr{netip.MustParseAddr("10.42.0.2")}},
}

func TestParseNetstat(t *testing.T) {
	routes := parseNetstat(netstatVentura)
	if len(routes) != 12 {
		t.Fatalf("parsed %d routes, want 12: %+v", len(routes), routes)
	}
	want := Route{Iface: "utun4", Dst: netip.MustParsePrefix("10.42.0.0/16")}
	if routes[1] != want {
		t.Fatalf("route 1 = %+v, want %+v", routes[1], want)
	}
	if r := routes[6]; r.Dst != netip.MustParsePrefix("192.168.1.0/24") || r.Iface != "en0" || r.Gateway.IsValid() {
		t.Fatalf("classful route = %+v", r)
	}
	if r := routes[2]; r.Dst != netip.MustParsePrefix("10.42.0.2/32") {
		t.Fatalf("host route = %+v", r)
	}
	old := parseNetstat(netstatCatalina)
	if len(old) != 5 {
		t.Fatalf("parsed %d routes from the older layout, want 5 (no IPv6): %+v", len(old), old)
	}
	if r := old[2]; r.Iface != "utun2" || r.Dst != netip.MustParsePrefix("10.42.0.0/16") || r.Gateway != netip.MustParseAddr("10.42.0.2") {
		t.Fatalf("classful /16 via own address = %+v", r)
	}
}

func TestGatewayTargetsDarwin(t *testing.T) {
	// en0's private default gateway is probed too (LOC-79).
	if got, want := gatewayTargets(parseNetstat(netstatVentura), macIfaces, nil, 8911), []string{"10.42.0.1:8911", "192.168.1.1:8911"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ventura: %v, want %v", got, want)
	}
	// The route through utun2's own address is on-link: its prefix's
	// first address is probed, not the machine itself.
	if got, want := gatewayTargets(parseNetstat(netstatCatalina), macIfaces, nil, 9411), []string{"10.42.0.1:9411", "172.20.1.1:9411", "10.0.0.1:9411"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Catalina: %v, want %v", got, want)
	}
}

func TestWireGuardLike(t *testing.T) {
	for _, tc := range []struct {
		ifc  Interface
		want bool
	}{
		{Interface{Name: "wg0", Up: true}, true},
		{Interface{Name: "utun7", Up: true}, true},
		{Interface{Name: "worker1-desktop", Up: true, PointToPoint: true}, true},
		{Interface{Name: "worker1-desktop", Up: false, PointToPoint: true}, false},
		{Interface{Name: "eth0", Up: true, HasHardwareAddr: true}, false},
		{Interface{Name: "ppp0", Up: true, PointToPoint: true, HasHardwareAddr: true}, false},
		{Interface{Name: "lo", Up: true, Loopback: true}, false},
	} {
		if got := WireGuardLike(tc.ifc, nil); got != tc.want {
			t.Errorf("WireGuardLike(%+v) = %v, want %v", tc.ifc, got, tc.want)
		}
	}
}

func TestGatewayTargetsSystemSeams(t *testing.T) {
	oldR, oldI := systemRoutes, systemInterfaces
	t.Cleanup(func() { systemRoutes, systemInterfaces = oldR, oldI })
	systemRoutes = func() ([]Route, error) { return parseProcNetRoute(procNetRoute), nil }
	systemInterfaces = func() []Interface { return linuxIfaces }
	if got, want := GatewayTargets(8911), []string{"10.42.0.1:8911", "10.0.0.1:8911"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("GatewayTargets = %v, want %v", got, want)
	}
	if got, want := LocalWireGuardInterfaces(), []string{"wg1", "worker1-desktop"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LocalWireGuardInterfaces = %v, want %v", got, want)
	}
}

package discovery

import (
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
)

// Gateway-derived probe targets (LOC-76).
//
// On a hub-and-spoke WireGuard network a spoke's only WireGuard peer is the
// hub, whose allowed-ips is the whole tunnel prefix, so `wg show` names no
// peer to probe (and needs CAP_NET_ADMIN besides). The routing table, which
// any user can read, still says where the tunnel leads: every route through
// a WireGuard interface into a private range yields exactly one address to
// probe, the route's gateway when it has one and otherwise the first usable
// address of its prefix (10.42.0.0/16 -> 10.42.0.1), which by convention is
// the hub. A node without any WireGuard interface (a container on the hub)
// probes its private default gateway instead. One address per route, never
// more: this is not a scan, and the rest of the network is learned from the
// hub's signed peer list.

// Route is one IPv4 route of the system routing table.
type Route struct {
	Iface string
	Dst   netip.Prefix
	// Gateway is the next hop, invalid for an on-link route.
	Gateway netip.Addr
}

// Interface is what target derivation needs to know about a network
// interface.
type Interface struct {
	Name         string
	Up           bool
	Loopback     bool
	PointToPoint bool
	// HasHardwareAddr is false for layer-3 tunnels (WireGuard, utun),
	// which have no link-layer address.
	HasHardwareAddr bool
	Addrs           []netip.Addr
}

// Seams for tests: the system routing table and interfaces.
var (
	systemRoutes     = readSystemRoutes
	systemInterfaces = readSystemInterfaces
)

// GatewayTargets returns the probe targets derived from the routing table
// (see the package comment above): one host:port on the given discovery port
// for every distinct gateway address of a route through a WireGuard
// interface. preferred names interfaces to treat as WireGuard whatever they
// look like: interface names, or CIDRs that one of the interface's
// addresses lies in (peer.interface, LOCUS_WG_INTERFACE). It needs no
// privileges and is cheap enough to call every probe round. Errors reading
// the table yield no targets.
func GatewayTargets(port int, preferred ...string) []string {
	routes, err := systemRoutes()
	if err != nil {
		return nil
	}
	return gatewayTargets(routes, systemInterfaces(), preferred, port)
}

func gatewayTargets(routes []Route, ifaces []Interface, preferred []string, port int) []string {
	byName := make(map[string]Interface, len(ifaces))
	for _, ifc := range ifaces {
		byName[ifc.Name] = ifc
	}
	seen := map[netip.Addr]bool{}
	var out []string
	for _, r := range routes {
		ifc, ok := byName[r.Iface]
		if !ok || !WireGuardLike(ifc, preferred) {
			continue
		}
		if ownAddr(ifaces, r.Gateway) {
			// Some tunnels (macOS packet tunnels) route through their
			// own address: the route is on-link.
			r.Gateway = netip.Addr{}
		}
		target, ok := RouteTarget(r)
		if !ok || seen[target] || ownAddr(ifaces, target) {
			continue
		}
		seen[target] = true
		out = append(out, net.JoinHostPort(target.String(), strconv.Itoa(port)))
	}
	// A node with no WireGuard interface of its own, such as a container on
	// the hub, still has one address that leads towards the network: its
	// default gateway (LOC-79). When that is a private address it is probed
	// too. On a network whose hub is not the gateway this is one signed
	// probe per interval to the LAN router, which does not answer, and
	// nothing more. Public gateways are never probed.
	for _, r := range routes {
		if r.Dst.Bits() != 0 || !r.Dst.Addr().Is4() {
			continue
		}
		ifc, ok := byName[r.Iface]
		if !ok || !ifc.Up || ifc.Loopback {
			continue
		}
		g := r.Gateway.Unmap()
		if !g.IsValid() || g.IsUnspecified() || !tunnelRange(g) || seen[g] || ownAddr(ifaces, g) {
			continue
		}
		seen[g] = true
		out = append(out, net.JoinHostPort(g.String(), strconv.Itoa(port)))
	}
	return out
}

// WireGuardLike reports whether an interface is treated as a WireGuard
// tunnel: one named in preferred (by name, or by a CIDR containing one of
// its addresses), or an up, non-loopback interface that is named wg* or
// utun* or is point-to-point without a link-layer address, as WireGuard
// interfaces are on Linux (POINTOPOINT,NOARP, link/none) and utun devices
// on macOS. Other layer-3 tunnels match too; they only ever contribute one
// probe address per private route.
func WireGuardLike(ifc Interface, preferred []string) bool {
	for _, p := range preferred {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p == ifc.Name {
			return true
		}
		if pfx, err := netip.ParsePrefix(p); err == nil {
			for _, a := range ifc.Addrs {
				if pfx.Contains(a) {
					return true
				}
			}
		}
	}
	if !ifc.Up || ifc.Loopback {
		return false
	}
	if strings.HasPrefix(ifc.Name, "wg") || strings.HasPrefix(ifc.Name, "utun") {
		return true
	}
	return ifc.PointToPoint && !ifc.HasHardwareAddr
}

// cgnat is 100.64.0.0/10, the shared address space some WireGuard overlays
// number their tunnels from.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// tunnelRange reports whether a is in a range a WireGuard tunnel is
// numbered from: RFC 1918 or the shared address space.
func tunnelRange(a netip.Addr) bool {
	return a.Is4() && (a.IsPrivate() || cgnat.Contains(a))
}

// RouteTarget is the one address probed for a route: its gateway when it
// has one, else the first usable address of the destination prefix (the
// network address + 1; for a /31 the network address itself). Default
// routes, host routes (/32: full-mesh peers come from `wg show`), and
// destinations outside the private and shared ranges (link-local, loopback,
// multicast, public) yield none. The result is never the network or the
// broadcast address of the prefix.
func RouteTarget(r Route) (netip.Addr, bool) {
	dst := r.Dst.Masked()
	bits := dst.Bits()
	if !dst.Addr().Is4() || bits <= 0 || bits >= 32 || !tunnelRange(dst.Addr()) {
		return netip.Addr{}, false
	}
	if g := r.Gateway.Unmap(); g.IsValid() && !g.IsUnspecified() {
		if !tunnelRange(g) {
			return netip.Addr{}, false
		}
		return g, true
	}
	if bits == 31 {
		return dst.Addr(), true
	}
	return dst.Addr().Next(), true
}

func ownAddr(ifaces []Interface, a netip.Addr) bool {
	for _, ifc := range ifaces {
		for _, x := range ifc.Addrs {
			if x == a {
				return true
			}
		}
	}
	return false
}

func readSystemInterfaces() []Interface {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	out := make([]Interface, 0, len(ifaces))
	for _, ifc := range ifaces {
		i := Interface{
			Name:            ifc.Name,
			Up:              ifc.Flags&net.FlagUp != 0,
			Loopback:        ifc.Flags&net.FlagLoopback != 0,
			PointToPoint:    ifc.Flags&net.FlagPointToPoint != 0,
			HasHardwareAddr: len(ifc.HardwareAddr) > 0,
		}
		if addrs, err := ifc.Addrs(); err == nil {
			for _, a := range addrs {
				if ipn, ok := a.(*net.IPNet); ok {
					if ip, ok := netip.AddrFromSlice(ipn.IP); ok {
						i.Addrs = append(i.Addrs, ip.Unmap())
					}
				}
			}
		}
		out = append(out, i)
	}
	return out
}

// LocalWireGuardInterfaces lists the interfaces that look like WireGuard
// tunnels (see WireGuardLike), sorted by name, for `wg show`.
func LocalWireGuardInterfaces() []string {
	var out []string
	for _, ifc := range systemInterfaces() {
		if WireGuardLike(ifc, nil) {
			out = append(out, ifc.Name)
		}
	}
	sort.Strings(out)
	return out
}

// parseProcNetRoute reads Linux's /proc/net/route: a header line, then one
// route per line with tab-separated Iface, Destination, Gateway, Flags,
// RefCnt, Use, Metric, Mask, ... where addresses and the mask are 32-bit hex
// in host (little-endian) byte order. Routes that are not up (RTF_UP) or
// whose mask is not contiguous are skipped.
func parseProcNetRoute(text string) []Route {
	const rtfUp, rtfGateway = 0x1, 0x2
	var out []Route
	for i, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if i == 0 || len(f) < 8 {
			continue
		}
		dst, ok1 := procAddr(f[1])
		gw, ok2 := procAddr(f[2])
		mask, ok3 := procAddr(f[7])
		flags, err := strconv.ParseUint(f[3], 16, 32)
		if !ok1 || !ok2 || !ok3 || err != nil || flags&rtfUp == 0 {
			continue
		}
		m := mask.As4()
		ones, bits := net.IPMask(m[:]).Size()
		if bits == 0 {
			continue // not contiguous
		}
		r := Route{Iface: f[0], Dst: netip.PrefixFrom(dst, ones).Masked()}
		if flags&rtfGateway != 0 && !gw.IsUnspecified() {
			r.Gateway = gw
		}
		out = append(out, r)
	}
	return out
}

func procAddr(h string) (netip.Addr, bool) {
	v, err := strconv.ParseUint(h, 16, 32)
	if err != nil || len(h) != 8 {
		return netip.Addr{}, false
	}
	return netip.AddrFrom4([4]byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}), true
}

// parseNetstat reads the IPv4 table of macOS (BSD) `netstat -rn -f inet`.
// The column of the interface is found from the header ("Netif"); the
// columns before it (Destination, Gateway, Flags and, on older releases,
// Refs and Use) are always present, so its index holds for every row.
// Destinations are abbreviated: "10.42/16" has an explicit prefix length,
// "192.168.1" without one is a classful network (one octet per 8 bits) and
// four octets without one are a host. The gateway is an IP literal, or a
// link ("link#6"), interface or MAC address for on-link routes.
func parseNetstat(text string) []Route {
	var out []Route
	netif := -1
	inet := true
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		switch {
		case strings.HasPrefix(f[0], "Internet6"):
			inet = false
			continue
		case strings.HasPrefix(f[0], "Internet"):
			inet = true
			continue
		case f[0] == "Destination":
			netif = -1
			for i, c := range f {
				if c == "Netif" {
					netif = i
				}
			}
			continue
		}
		if !inet || netif < 0 || len(f) <= netif {
			continue
		}
		dst, ok := netstatPrefix(f[0])
		if !ok {
			continue
		}
		r := Route{Iface: f[netif], Dst: dst}
		if gw, err := netip.ParseAddr(f[1]); err == nil && gw.Is4() && strings.Contains(f[2], "G") {
			r.Gateway = gw
		}
		out = append(out, r)
	}
	return out
}

func netstatPrefix(s string) (netip.Prefix, bool) {
	if s == "default" {
		return netip.PrefixFrom(netip.IPv4Unspecified(), 0), true
	}
	if i := strings.IndexByte(s, '%'); i >= 0 {
		s = s[:i]
	}
	host, bitsStr, explicit := strings.Cut(s, "/")
	parts := strings.Split(host, ".")
	if len(parts) < 1 || len(parts) > 4 {
		return netip.Prefix{}, false
	}
	var b [4]byte
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 255 {
			return netip.Prefix{}, false
		}
		b[i] = byte(n)
	}
	bits := 8 * len(parts)
	if explicit {
		n, err := strconv.Atoi(bitsStr)
		if err != nil || n < 0 || n > 32 {
			return netip.Prefix{}, false
		}
		bits = n
	}
	return netip.PrefixFrom(netip.AddrFrom4(b), bits).Masked(), true
}

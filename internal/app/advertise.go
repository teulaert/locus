package app

import (
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/teulaert/locus/internal/config"
)

// Advertised is the host this peer announces in its sync address, and how
// it was chosen.
type Advertised struct {
	Host string `json:"host"`
	// Source says why this host: "peer.addr", "peer.interface wg0",
	// "interface wg0 shares a prefix with 10.42.0.1", "route to 10.42.0.1"
	// or "default route".
	Source string `json:"source"`
	// Fallback is true when nothing pointed at the trusted interface and the
	// default-route address was taken; it may be the LAN or Wi-Fi address.
	Fallback bool `json:"fallback"`
	// Problem explains why a configured peer.interface was not used.
	Problem string `json:"problem,omitempty"`
}

// localAddr is one address of a local interface, with its prefix length.
type localAddr struct {
	Iface  string
	Prefix netip.Prefix
}

// Seams for tests: the machine's interface addresses, the source address
// the kernel would use towards dst (no packet is sent), and the default
// route's source address.
var (
	interfaceAddrs = systemInterfaceAddrs
	routeSource    = systemRouteSource
	defaultRouteIP = outboundIP
)

func systemInterfaceAddrs() []localAddr {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []localAddr
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(ipn.IP)
			if !ok {
				continue
			}
			ones, _ := ipn.Mask.Size()
			out = append(out, localAddr{Iface: ifc.Name, Prefix: netip.PrefixFrom(ip.Unmap(), ones)})
		}
	}
	return out
}

func systemRouteSource(dst netip.Addr) (netip.Addr, bool) {
	conn, err := net.Dial("udp", netip.AddrPortFrom(dst, 9).String())
	if err != nil {
		return netip.Addr{}, false
	}
	defer conn.Close()
	ua, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return netip.Addr{}, false
	}
	ip, ok := netip.AddrFromSlice(ua.IP)
	return ip.Unmap(), ok
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

// wildcard reports whether a listen address binds every interface.
func wildcard(addr string) bool { return addr == "" || addr == "0.0.0.0" || addr == "::" }

// AdvertiseHost chooses the host this peer announces as its sync address.
// A specific peer.addr is announced as is. With 0.0.0.0 it is, in order:
// an address of peer.interface (a name or a CIDR); an address on an
// interface whose prefix contains one of the peers (hints: discovery targets
// as host or host:port; only IP literals count); the source address of the
// route towards one of them; and finally the default route's address, which
// may well be the Wi-Fi address rather than the WireGuard one (Fallback).
//
// Receivers do not trust the host (they bind it to the address the packet
// came from), so this mostly decides what this peer shows and logs.
func AdvertiseHost(cfg *config.Config, hints []string) Advertised {
	if !wildcard(cfg.Peer.Addr) {
		return Advertised{Host: cfg.Peer.Addr, Source: "peer.addr"}
	}
	locals := interfaceAddrs()
	var problem string
	if want := strings.TrimSpace(cfg.Peer.Interface); want != "" {
		if ip, ok := addrOnInterface(locals, want); ok {
			return Advertised{Host: ip.String(), Source: "peer.interface " + want}
		}
		problem = fmt.Sprintf("peer.interface %q has no usable address", want)
	}
	peers := hintAddrs(hints)
	for _, p := range peers {
		for _, l := range locals {
			if l.Prefix.Contains(p) && l.Prefix.Addr() != p && usable(l.Prefix.Addr(), p) {
				return Advertised{Host: l.Prefix.Addr().String(), Source: fmt.Sprintf("interface %s shares a prefix with %s", l.Iface, p), Problem: problem}
			}
		}
	}
	for _, p := range peers {
		if src, ok := routeSource(p); ok && src != p && usable(src, p) {
			return Advertised{Host: src.String(), Source: "route to " + p.String(), Problem: problem}
		}
	}
	return Advertised{Host: defaultRouteIP(), Source: "default route", Fallback: true, Problem: problem}
}

// usable rejects unspecified and link-local sources, and loopback unless the
// peer is on loopback too.
func usable(src, peer netip.Addr) bool {
	if src.IsUnspecified() || src.IsLinkLocalUnicast() {
		return false
	}
	return !src.IsLoopback() || peer.IsLoopback()
}

// addrOnInterface finds the address peer.interface names: the first address
// inside a CIDR, or the first IPv4 (else global IPv6) address of a named
// interface.
func addrOnInterface(locals []localAddr, want string) (netip.Addr, bool) {
	if pfx, err := netip.ParsePrefix(want); err == nil {
		for _, l := range locals {
			if pfx.Contains(l.Prefix.Addr()) {
				return l.Prefix.Addr(), true
			}
		}
		return netip.Addr{}, false
	}
	var v6 netip.Addr
	for _, l := range locals {
		if l.Iface != want {
			continue
		}
		a := l.Prefix.Addr()
		if a.Is4() {
			return a, true
		}
		if !v6.IsValid() && !a.IsLinkLocalUnicast() {
			v6 = a
		}
	}
	return v6, v6.IsValid()
}

// hintAddrs extracts the IP literals from host or host:port hints, in order
// and without duplicates. Host names are skipped: choosing an interface must
// not depend on DNS.
func hintAddrs(hints []string) []netip.Addr {
	seen := map[netip.Addr]bool{}
	var out []netip.Addr
	for _, h := range hints {
		host := h
		if hp, _, err := net.SplitHostPort(h); err == nil {
			host = hp
		}
		ip, err := netip.ParseAddr(host)
		if err != nil || ip.IsUnspecified() {
			continue
		}
		ip = ip.Unmap()
		if !seen[ip] {
			seen[ip] = true
			out = append(out, ip)
		}
	}
	return out
}

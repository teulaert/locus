// Package discovery implements Locus peer discovery over an existing
// WireGuard network: probes are sent only to known WireGuard peer addresses,
// never to a scanned subnet, and replies carry an authenticated handshake.
package discovery

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
)

// Magic marks Locus discovery packets so unrelated services are ignored.
const Magic = "locus-probe-v1"

// Protocol is the discovery protocol version this build speaks.
const Protocol = 1

// Compatible reports whether a verified handshake comes from a peer this
// build can synchronize with: the same discovery protocol and, when the peer
// advertises one, the same sync protocol. Peers from before the sync protocol
// was advertised say nothing and are assumed compatible; their transfers are
// still checked by the sync server. syncProtocol 0 skips the sync check.
func Compatible(p *Packet, syncProtocol int) bool {
	if p.Protocol != Protocol {
		return false
	}
	sp := p.Info.SyncProtocol()
	return syncProtocol == 0 || sp == 0 || sp == syncProtocol
}

// Packet is a signed discovery handshake (request or reply).
type Packet struct {
	Magic    string    `json:"magic"`
	Protocol int       `json:"protocol"`
	TS       int64     `json:"ts"`
	Info     peer.Info `json:"info"`
	Sig      string    `json:"sig"`
}

// SignPacket signs the packet body in place.
func SignPacket(p *Packet, id *identity.Identity) {
	p.Sig = ""
	b, _ := json.Marshal(p)
	p.Sig = hex.EncodeToString(id.Sign(b))
}

// Verify checks the packet signature against the embedded peer ID.
func (p *Packet) Verify() bool {
	if p.Magic != Magic {
		return false
	}
	sig, err := hex.DecodeString(p.Sig)
	if err != nil {
		return false
	}
	s := p.Sig
	p.Sig = ""
	b, _ := json.Marshal(p)
	p.Sig = s
	return identity.Verify(p.Info.PeerID, b, sig)
}

// Server answers discovery probes on a fixed UDP port.
type Server struct {
	Port     int
	Identity *identity.Identity
	Info     peer.Info

	// Authorized mirrors Prober.Authorized: when non-empty, probes from any
	// other peer ID are dropped without a reply and never reach OnProbe, so
	// a single UDP packet cannot make an unauthorized peer a sync target.
	Authorized []string

	// OnProbe is called for every well-formed probe from a verified,
	// authorized peer. The packet's Info.SyncAddr has already been bound to
	// the probe's source IP (see BindSyncAddr).
	OnProbe func(addr string, p *Packet)

	conn *net.UDPConn
	done chan struct{}
}

// ListenAndServe starts the discovery responder. Bind failures are returned;
// the caller decides whether discovery is optional.
func (s *Server) ListenAndServe(ctx context.Context) error {
	addr := net.UDPAddr{Port: s.Port}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return fmt.Errorf("discovery: listen udp %d: %w", s.Port, err)
	}
	s.conn = conn
	s.done = make(chan struct{})
	go s.serve(ctx, conn)
	return nil
}

// Done is closed once the responder started by ListenAndServe has stopped
// after its context was cancelled. It is closed already when the responder
// never started.
func (s *Server) Done() <-chan struct{} {
	if s.done == nil {
		c := make(chan struct{})
		close(c)
		return c
	}
	return s.done
}

func (s *Server) serve(ctx context.Context, conn *net.UDPConn) {
	defer close(s.done)
	buf := make([]byte, 4096)
	for {
		if ctx.Err() != nil {
			conn.Close()
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		var p Packet
		if err := json.Unmarshal(buf[:n], &p); err != nil {
			continue
		}
		if p.Magic != Magic || !p.Verify() {
			continue
		}
		if !allowed(s.Authorized, p.Info.PeerID) {
			continue
		}
		bound, ok := BindSyncAddr(p.Info.SyncAddr, from)
		if !ok {
			continue
		}
		p.Info.SyncAddr = bound
		if s.OnProbe != nil {
			s.OnProbe(from.String(), &p)
		}
		reply := Packet{Magic: Magic, Protocol: Protocol, TS: identity.Timestamp(), Info: s.Info}
		SignPacket(&reply, s.Identity)
		out, _ := json.Marshal(reply)
		_, _ = conn.WriteToUDP(out, from)
	}
}

// BindSyncAddr pins the host of a peer's self-declared sync address to the
// source IP its probe arrived from, keeping only the advertised port. The
// signature proves who sent a probe, not that the sender owns the address it
// names; without this, any peer could direct our pushes at a third host.
// It reports false when the advertised address has no usable port.
func BindSyncAddr(advertised string, from *net.UDPAddr) (string, bool) {
	if from == nil {
		return "", false
	}
	_, port, err := net.SplitHostPort(advertised)
	if err != nil {
		return "", false
	}
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
		return "", false
	}
	ip, ok := netip.AddrFromSlice(from.IP)
	if !ok {
		return "", false
	}
	return net.JoinHostPort(ip.Unmap().String(), port), true
}

// ProbeOnce sends a single probe to addr and returns the reply. The reply's
// Info.SyncAddr is bound to the address that answered (see BindSyncAddr):
// the responder's advertised host is often its default-route interface
// rather than the WireGuard address we reached it on, and is never trusted.
// As on the responder side, the rewritten packet no longer verifies.
func ProbeOnce(ctx context.Context, addr string, id *identity.Identity, info peer.Info, timeout time.Duration) (*Packet, error) {
	p := Packet{Magic: Magic, Protocol: Protocol, TS: identity.Timestamp(), Info: info}
	SignPacket(&p, id)
	out, _ := json.Marshal(p)

	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "udp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.Write(out); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	var reply Packet
	if err := json.Unmarshal(buf[:n], &reply); err != nil {
		return nil, fmt.Errorf("discovery: malformed reply from %s: %w", addr, err)
	}
	if reply.Magic != Magic || !reply.Verify() {
		return nil, fmt.Errorf("discovery: unauthenticated reply from %s", addr)
	}
	// The socket is connected, so the reply came from the probed address.
	from, _ := conn.RemoteAddr().(*net.UDPAddr)
	bound, ok := BindSyncAddr(reply.Info.SyncAddr, from)
	if !ok {
		return nil, fmt.Errorf("discovery: reply from %s has no usable sync port", addr)
	}
	reply.Info.SyncAddr = bound
	return &reply, nil
}

// Prober periodically probes known addresses and maintains presence.
type Prober struct {
	Identity   *identity.Identity
	Info       peer.Info
	Table      *peer.Table
	Targets    []string
	Authorized []string
	Interval   time.Duration
	Timeout    time.Duration
	// SyncProtocol is this build's sync protocol; peers advertising another
	// are recorded as incompatible. 0 skips the check.
	SyncProtocol int
}

// Run probes all targets immediately, then on every interval, until ctx ends.
func (p *Prober) Run(ctx context.Context) {
	interval := p.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	timeout := p.probeTimeout()
	p.round(ctx, timeout)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.round(ctx, timeout)
		}
	}
}

// RoundOnce performs a single probe round over all targets.
func (p *Prober) RoundOnce(ctx context.Context) {
	p.round(ctx, p.probeTimeout())
}

func (p *Prober) probeTimeout() time.Duration {
	if p.Timeout > 0 {
		return p.Timeout
	}
	return 2 * time.Second
}

func (p *Prober) round(ctx context.Context, timeout time.Duration) {
	for _, addr := range p.Targets {
		reply, err := ProbeOnce(ctx, addr, p.Identity, p.Info, timeout)
		if err != nil {
			p.Table.ObserveFailure(addr, "")
			continue
		}
		if !p.authorized(reply.Info.PeerID) {
			p.Table.ObserveFailure(addr, reply.Info.PeerID)
			continue
		}
		compatible := Compatible(reply, p.SyncProtocol)
		p.Table.Observe(addr, reply.Info, reply.Protocol, compatible)
	}
	p.Table.Expire()
}

func (p *Prober) authorized(peerID string) bool {
	return allowed(p.Authorized, peerID)
}

// allowed applies an authorized_peers list: empty admits every verified peer.
func allowed(list []string, peerID string) bool {
	if len(list) == 0 {
		return true
	}
	for _, a := range list {
		if a == peerID {
			return true
		}
	}
	return false
}

// WireGuardPeers attempts to read the tunnel addresses of the peers of the
// given WireGuard interface using the `wg` tool. It is best effort:
// environments without wg (or without WireGuard tooling permissions) fall
// back to the explicit discovery.peers configuration.
func WireGuardPeers(iface string) ([]string, error) {
	if iface == "" {
		return nil, errors.New("discovery: no wireguard interface configured")
	}
	if _, err := exec.LookPath("wg"); err != nil {
		return nil, errors.New("discovery: wg tool not available")
	}
	out, err := exec.Command("wg", "show", iface, "dump").Output()
	if err != nil {
		return nil, fmt.Errorf("wg show %s: %w", iface, err)
	}
	return parseWireGuardDump(string(out)), nil
}

// parseWireGuardDump extracts probe targets from `wg show <iface> dump`
// output. Per wg(8) the first line describes the interface itself
// (private-key public-key listen-port fwmark) and every following line is a
// peer: public-key preshared-key endpoint allowed-ips latest-handshake
// transfer-rx transfer-tx persistent-keepalive.
//
// The targets are the peers' tunnel addresses, taken from allowed-ips (a
// comma-separated list of prefixes). Only single-address prefixes (/32 for
// IPv4, /128 for IPv6) identify a peer; wider ranges such as a routed /24 are
// skipped because Locus never scans subnets. The endpoint column is
// deliberately ignored: it is the peer's public Internet address, where the
// discovery port is not reachable.
func parseWireGuardDump(dump string) []string {
	var addrs []string
	seen := map[string]bool{}
	first := true
	for _, line := range strings.Split(dump, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if first {
			// Interface line.
			first = false
			continue
		}
		if len(fields) < 4 {
			continue
		}
		for _, s := range strings.Split(fields[3], ",") {
			pfx, err := netip.ParsePrefix(strings.TrimSpace(s))
			if err != nil || !pfx.IsSingleIP() {
				continue
			}
			target := net.JoinHostPort(pfx.Addr().Unmap().String(), strconv.Itoa(DiscoveryPortDefault))
			if seen[target] {
				continue
			}
			seen[target] = true
			addrs = append(addrs, target)
		}
	}
	return addrs
}

// DiscoveryPortDefault is the UDP port probed on known peers.
const DiscoveryPortDefault = 8911

// LocalWireGuardInterfaces lists interfaces whose name starts with wg.
func LocalWireGuardInterfaces() []string {
	entries, err := os.ReadDir("/sys/class/net")
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wg") {
			out = append(out, e.Name())
		}
	}
	return out
}

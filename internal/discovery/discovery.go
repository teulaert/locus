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

	"locus/internal/identity"
	"locus/internal/peer"
)

// Magic marks Locus discovery packets so unrelated services are ignored.
const Magic = "locus-probe-v1"

// Protocol is the discovery protocol version this build speaks.
const Protocol = 1

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

	// OnProbe is called for every well-formed probe with a verified peer.
	OnProbe func(addr string, p *Packet)

	conn *net.UDPConn
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
	go s.serve(ctx, conn)
	return nil
}

func (s *Server) serve(ctx context.Context, conn *net.UDPConn) {
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
		if s.OnProbe != nil {
			s.OnProbe(from.String(), &p)
		}
		reply := Packet{Magic: Magic, Protocol: Protocol, TS: identity.Timestamp(), Info: s.Info}
		SignPacket(&reply, s.Identity)
		out, _ := json.Marshal(reply)
		_, _ = conn.WriteToUDP(out, from)
	}
}

// ProbeOnce sends a single probe to addr and returns the reply.
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

	// SyncAddrFor is consulted to fill in the sync service address of a
	// reply when the packet omits it.
	SyncAddrFor func(addr string) string
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
		compatible := reply.Protocol == Protocol
		p.Table.Observe(addr, reply.Info, reply.Protocol, compatible, "")
	}
	p.Table.Expire()
}

func (p *Prober) authorized(peerID string) bool {
	if len(p.Authorized) == 0 {
		return true
	}
	for _, a := range p.Authorized {
		if a == peerID {
			return true
		}
	}
	return false
}

// WireGuardPeers attempts to read peer endpoint addresses of the given
// WireGuard interface using the `wg` tool. It is best effort: environments
// without wg (or without WireGuard tooling permissions) fall back to the
// explicit discovery.peers configuration.
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
	var addrs []string
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		endpoint := fields[3]
		if endpoint == "(none)" || endpoint == "" {
			continue
		}
		if ap, err := netip.ParseAddrPort(endpoint); err == nil {
			addrs = append(addrs, net.JoinHostPort(ap.Addr().String(), strconv.Itoa(DiscoveryPortDefault)))
			continue
		}
		if a, err := netip.ParseAddr(endpoint); err == nil {
			addrs = append(addrs, net.JoinHostPort(a.String(), strconv.Itoa(DiscoveryPortDefault)))
		}
	}
	return addrs, nil
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

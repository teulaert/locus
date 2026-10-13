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
	"log/slog"
	"net"
	"net/netip"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/netguard"
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
//
// Peers is the responder's peer list (peer exchange): only present in a
// reply to a probe that announces peer.PeerExchangeCapability, because older
// builds verify the signature over their own re-encoding of the packet and
// would drop the unknown field. It is covered by the signature like the rest.
type Packet struct {
	Magic    string      `json:"magic"`
	Protocol int         `json:"protocol"`
	TS       int64       `json:"ts"`
	Info     peer.Info   `json:"info"`
	Peers    []KnownPeer `json:"peers,omitempty"`
	Sig      string      `json:"sig"`
}

// maxPacket is the read buffer for discovery packets: a reply with a full
// peer list is larger than one Ethernet frame, never larger than this.
const maxPacket = 64 << 10

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

	// Share, when set, returns the peer list for a reply to the given peer
	// (see Book.Share). It is only consulted for probes that announce peer
	// exchange.
	Share func(requester string) []KnownPeer

	conn *net.UDPConn
	done chan struct{}
}

// ListenAndServe starts the discovery responder. Bind failures are returned;
// the caller decides whether discovery is optional.
//
// Under the test guard (netguard) it listens on loopback only and refuses
// the default discovery port, where this machine's own daemon answers.
func (s *Server) ListenAndServe(ctx context.Context) error {
	addr := net.UDPAddr{Port: s.Port}
	if netguard.Active() {
		if err := netguard.CheckListen(s.Port); err != nil {
			return fmt.Errorf("discovery: listen udp %d: %w", s.Port, err)
		}
		addr.IP = net.IPv4(127, 0, 0, 1)
	}
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
	buf := make([]byte, maxPacket)
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
		if s.Share != nil && p.Info.Has(peer.PeerExchangeCapability) {
			reply.Peers = s.Share(p.Info.PeerID)
		}
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
//
// Under the test guard (netguard) only loopback addresses on non-default
// ports are probed; anything else fails with netguard.ErrBlocked.
func ProbeOnce(ctx context.Context, addr string, id *identity.Identity, info peer.Info, timeout time.Duration) (*Packet, error) {
	if err := netguard.CheckDial(addr); err != nil {
		return nil, fmt.Errorf("discovery: probe %w", err)
	}
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
	// A shutdown does not wait out the reply timeout (LOC-81).
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Now()) })
	defer stop()
	buf := make([]byte, maxPacket)
	n, err := conn.Read(buf)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
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
	Identity *identity.Identity
	Info     peer.Info
	Table    *peer.Table
	// Targets are fixed probe addresses, used when Book is nil.
	Targets []string
	// Book, when set, supplies the targets every round (configured plus
	// learned addresses) and learns from every verified reply: the
	// responder itself and the peers it lists.
	Book *Book
	// Derive, when set, is called at the start of every round for the
	// targets derived from the routing table (GatewayTargets): interfaces
	// and routes come and go. With a Book they replace its derived
	// targets; without one they are probed after Targets.
	Derive     func() []string
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
	if p.Book != nil && p.Derive != nil {
		p.Book.SetDerived(p.Derive())
	}
	probed := map[string]bool{}
	for _, t := range p.targets() {
		if ctx.Err() != nil {
			return
		}
		probed[t.Addr] = true
		p.probe(ctx, t, timeout)
	}
	// Peers first heard of in this round's replies are probed right away
	// rather than an interval later.
	if p.Book != nil {
		for _, t := range p.targets() {
			if !probed[t.Addr] && ctx.Err() == nil {
				p.probe(ctx, t, timeout)
			}
		}
	}
	p.Table.Expire()
}

func (p *Prober) targets() []Target {
	if p.Book != nil {
		return p.Book.Targets()
	}
	addrs := p.Targets
	if p.Derive != nil {
		addrs = Targets(append(append([]string(nil), p.Targets...), p.Derive()...), 0)
	}
	out := make([]Target, len(addrs))
	for i, a := range addrs {
		out[i] = Target{Addr: a}
	}
	return out
}

// guardOnce logs the first probe the test guard refuses.
var guardOnce sync.Once

func (p *Prober) probe(ctx context.Context, t Target, timeout time.Duration) {
	addr := t.Addr
	reply, err := ProbeOnce(ctx, addr, p.Identity, p.Info, timeout)
	if errors.Is(err, netguard.ErrBlocked) {
		guardOnce.Do(func() {
			slog.Warn("discovery: not probing a non-loopback or default-port address under "+netguard.EnvVar, "addr", addr)
		})
		return
	}
	if err != nil && ctx.Err() != nil {
		// Cut short by a shutdown: the address did not fail, so it is
		// neither marked down nor counted towards forgetting it (LOC-81).
		return
	}
	if err != nil {
		p.Table.ObserveFailure(addr, "")
		// A learned address that stops answering is forgotten, from the
		// book and from presence (LOC-80); configured ones are retried.
		if t.PeerID != "" && p.Book != nil && p.Book.ObserveFailure(t.PeerID, addr) {
			p.Table.Forget(t.PeerID, addr)
		}
		return
	}
	id := reply.Info.PeerID
	if p.Identity != nil && id == p.Identity.PeerID {
		// We probed ourselves: the address is ours, whatever peer ID a
		// learned entry claimed for it. Never a presence entry (LOC-80).
		if p.Book != nil {
			for _, gone := range p.Book.ForgetAddr(addr) {
				p.Table.Forget(gone, addr)
			}
		}
		return
	}
	if t.PeerID != "" && t.PeerID != id && p.Book != nil {
		// A learned entry named another peer at this address: it is
		// stale. Only the peer that answered is recorded (LOC-80).
		if p.Book.Forget(t.PeerID, addr) {
			p.Table.Forget(t.PeerID, addr)
		}
	}
	if !p.authorized(id) {
		p.Table.ObserveFailure(addr, id)
		return
	}
	compatible := Compatible(reply, p.SyncProtocol)
	p.Table.Observe(addr, reply.Info, reply.Protocol, compatible)
	if p.Book != nil {
		p.Book.ObserveDirect(id, addr, reply.Info.SyncAddr, reply.Info.Hostname)
		if from, err := net.ResolveUDPAddr("udp", addr); err == nil {
			p.Book.ObserveExchange(id, from, reply.Peers)
		}
	}
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
// given WireGuard interface using the `wg` tool and returns them as probe
// targets on the given discovery port. It is best effort: environments
// without wg (or without WireGuard tooling permissions) fall back to the
// explicit discovery.peers configuration.
func WireGuardPeers(iface string, port int) ([]string, error) {
	if netguard.Active() {
		return nil, errors.New("discovery: wg show skipped under " + netguard.EnvVar)
	}
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
	return parseWireGuardDump(string(out), port), nil
}

// parseWireGuardDump extracts probe targets from `wg show <iface> dump`
// output, probing each peer on the given discovery port. Per wg(8) the first line describes the interface itself
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
func parseWireGuardDump(dump string, port int) []string {
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
			target := net.JoinHostPort(pfx.Addr().Unmap().String(), strconv.Itoa(port))
			if seen[target] {
				continue
			}
			seen[target] = true
			addrs = append(addrs, target)
		}
	}
	return addrs
}

// Targets normalizes probe targets: empty entries are dropped, a bare host
// is probed on the given discovery port, an explicit host:port keeps its own
// port, and duplicates are removed (first occurrence wins).
func Targets(entries []string, port int) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range entries {
		if t == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(t); err != nil {
			t = net.JoinHostPort(t, strconv.Itoa(port))
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

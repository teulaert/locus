package discovery

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
)

func testInfo(t *testing.T, dir string) (*identity.Identity, peer.Info) {
	t.Helper()
	id, err := identity.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	return id, peer.Info{PeerID: id.PeerID, Hostname: "testhost", Version: "0.1.0", SyncAddr: "127.0.0.1:1", Capabilities: []string{"sync"}}
}

func TestSignAndVerifyPacket(t *testing.T) {
	id, info := testInfo(t, t.TempDir())
	p := Packet{Magic: Magic, Protocol: Protocol, TS: identity.Timestamp(), Info: info}
	SignPacket(&p, id)
	if !p.Verify() {
		t.Fatal("freshly signed packet must verify")
	}

	// Tampering with the body invalidates it.
	original := p.Info.Hostname
	p.Info.Hostname = "evil"
	if p.Verify() {
		t.Fatal("packet verified after its body was modified")
	}
	p.Info.Hostname = original

	// A signature from someone else's key does not verify, even if the
	// attacker claims the victim's peer ID.
	other, _ := testInfo(t, t.TempDir())
	p.Info.PeerID = other.PeerID
	if p.Verify() {
		t.Fatal("packet verified against the wrong identity")
	}
}

func TestVerifyRejectsForeignMagic(t *testing.T) {
	id, info := testInfo(t, t.TempDir())
	p := Packet{Magic: "not-locus", Protocol: Protocol, TS: identity.Timestamp(), Info: info}
	SignPacket(&p, id)
	if p.Verify() {
		t.Fatal("packet with foreign magic verified; probes from other software must be dropped")
	}
}

// TestProbeOnceRoundTrip: a responder answers a signed probe with its own
// signed identity. Answering is not the same as being trusted — whether the
// peer is allowed to synchronize is the prober's decision, recorded by its
// allowlist.
func TestProbeOnceRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	responderID, responderInfo := testInfo(t, t.TempDir())
	probed := make(chan string, 1)
	srv := &Server{
		Port:     freeUDPPort(t),
		Identity: responderID,
		Info:     responderInfo,
		OnProbe:  func(addr string, p *Packet) { probed <- p.Info.PeerID },
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		t.Fatal(err)
	}

	askerID, askerInfo := testInfo(t, t.TempDir())
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.Port))
	reply, err := ProbeOnce(ctx, addr, askerID, askerInfo, 2*time.Second)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if reply.Info.PeerID != responderID.PeerID {
		t.Fatalf("reply from %s, want the responder", reply.Info.PeerID)
	}
	if reply.Magic != Magic || reply.Protocol != Protocol {
		t.Fatalf("reply magic/protocol = %s/%d", reply.Magic, reply.Protocol)
	}
	select {
	case got := <-probed:
		if got != askerInfo.PeerID {
			t.Fatalf("responder saw peer %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("responder never saw the probe")
	}
}

func TestProbeOnceFailsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Nothing is listening: the probe must fail rather than invent a peer.
	id, info := testInfo(t, t.TempDir())
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := ProbeOnce(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(freeUDPPort(t))), id, info, 200*time.Millisecond)
		if err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("probe to a closed port succeeded")
		}
	}
}

func TestProberRoundOncePopulatesPresence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	responderID, responderInfo := testInfo(t, t.TempDir())
	srv := &Server{Port: freeUDPPort(t), Identity: responderID, Info: responderInfo}
	if err := srv.ListenAndServe(ctx); err != nil {
		t.Fatal(err)
	}
	askerID, askerInfo := testInfo(t, t.TempDir())
	table := peer.NewTable(5 * time.Second)
	prober := &Prober{
		Identity: askerID,
		Info:     askerInfo,
		Table:    table,
		Targets:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.Port))},
		Timeout:  time.Second,
	}
	prober.RoundOnce(ctx)
	online := table.Online()
	if len(online) != 1 {
		t.Fatalf("online peers after one round = %d, want 1", len(online))
	}
	if online[0].PeerID != responderID.PeerID || online[0].State != peer.Online {
		t.Fatalf("unexpected presence entry %+v", online[0])
	}

	// An unauthorized peer is not added, even when it answers.
	otherID, otherInfo := testInfo(t, t.TempDir())
	srv2 := &Server{Port: freeUDPPort(t), Identity: otherID, Info: otherInfo}
	if err := srv2.ListenAndServe(ctx); err != nil {
		t.Fatal(err)
	}
	prober.Targets = append(prober.Targets, net.JoinHostPort("127.0.0.1", strconv.Itoa(srv2.Port)))
	prober.Authorized = []string{responderID.PeerID}
	prober.RoundOnce(ctx)
	if got := len(table.Online()); got != 1 {
		t.Fatalf("online peers with allowlist = %d, want 1 (the allowlist must exclude the other)", got)
	}
}

// TestServerDropsUnauthorizedProbes: the responder enforces authorized_peers
// like the prober does. A validly signed probe from an unlisted peer gets no
// reply and never reaches OnProbe (which is what feeds presence and, from
// there, the sync engine's list of push targets).
func TestServerDropsUnauthorizedProbes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	responderID, responderInfo := testInfo(t, t.TempDir())
	friendID, friendInfo := testInfo(t, t.TempDir())
	strangerID, strangerInfo := testInfo(t, t.TempDir())
	probed := make(chan string, 4)
	srv := &Server{
		Port:       freeUDPPort(t),
		Identity:   responderID,
		Info:       responderInfo,
		Authorized: []string{friendID.PeerID},
		OnProbe:    func(addr string, p *Packet) { probed <- p.Info.PeerID },
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		t.Fatal(err)
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.Port))

	if _, err := ProbeOnce(ctx, addr, strangerID, strangerInfo, 300*time.Millisecond); err == nil {
		t.Fatal("responder answered a peer outside its allowlist")
	}
	if _, err := ProbeOnce(ctx, addr, friendID, friendInfo, 2*time.Second); err != nil {
		t.Fatalf("authorized probe: %v", err)
	}
	// The socket is served in order, so the friend's probe being the first
	// one seen proves the stranger's never reached OnProbe.
	select {
	case got := <-probed:
		if got != friendID.PeerID {
			t.Fatalf("OnProbe saw %s, want only the authorized peer", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("responder never reported the authorized probe")
	}
}

// TestServerBindsSyncAddrToSource: a probe may name any sync address, but the
// responder only trusts its port; the host is the IP the probe came from.
func TestServerBindsSyncAddrToSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	responderID, responderInfo := testInfo(t, t.TempDir())
	got := make(chan string, 1)
	srv := &Server{
		Port:     freeUDPPort(t),
		Identity: responderID,
		Info:     responderInfo,
		OnProbe:  func(addr string, p *Packet) { got <- p.Info.SyncAddr },
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		t.Fatal(err)
	}
	askerID, askerInfo := testInfo(t, t.TempDir())
	askerInfo.SyncAddr = "203.0.113.7:8909" // someone else's host
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(srv.Port))
	if _, err := ProbeOnce(ctx, addr, askerID, askerInfo, 2*time.Second); err != nil {
		t.Fatalf("probe: %v", err)
	}
	select {
	case s := <-got:
		if s != "127.0.0.1:8909" {
			t.Fatalf("observed sync addr %q, want the probe source with the advertised port", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("responder never reported the probe")
	}
}

func TestBindSyncAddr(t *testing.T) {
	from := &net.UDPAddr{IP: net.ParseIP("10.8.0.2"), Port: 40000}
	cases := []struct {
		adv  string
		want string
		ok   bool
	}{
		{"10.8.0.2:8909", "10.8.0.2:8909", true},
		{"192.0.2.50:8909", "10.8.0.2:8909", true},
		{"victim.example:9000", "10.8.0.2:9000", true},
		{"", "", false},
		{"10.8.0.2", "", false},
		{"10.8.0.2:0", "", false},
		{"10.8.0.2:http", "", false},
	}
	for _, c := range cases {
		got, ok := BindSyncAddr(c.adv, from)
		if got != c.want || ok != c.ok {
			t.Errorf("BindSyncAddr(%q) = %q, %v; want %q, %v", c.adv, got, ok, c.want, c.ok)
		}
	}
	v6 := &net.UDPAddr{IP: net.ParseIP("fd00::2")}
	if got, ok := BindSyncAddr("[fd00::9]:8909", v6); !ok || got != "[fd00::2]:8909" {
		t.Errorf("ipv6 bind = %q, %v", got, ok)
	}
}

func TestPacketJSONIsStable(t *testing.T) {
	// The wire format is signed over its own JSON encoding, so field names are
	// part of the protocol contract.
	id, info := testInfo(t, t.TempDir())
	p := Packet{Magic: Magic, Protocol: Protocol, TS: 42, Info: info}
	SignPacket(&p, id)
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"magic"`, `"protocol"`, `"ts"`, `"info"`, `"sig"`, `"peer_id"`, `"sync_addr"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("packet JSON missing %s: %s", want, b)
		}
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

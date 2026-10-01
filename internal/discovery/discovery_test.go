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

// wgDump is a captured `wg show wg0 dump` (keys replaced): the interface line
// followed by four peers. Columns are tab separated as wg(8) prints them.
const wgDump = "" +
	"eHN0LXByaXZhdGUta2V5LXBsYWNlaG9sZGVyLTAwMDA9\tAm2l1Yb0Z1o3x7Q9cBq0tVJ4mUq3bQ0+5mVj2dTnDkI=\t51820\toff\n" +
	"Hn8Qe0Y5+3xG0bVkqRzv4m6O7tJmUuS3sWlHqJp2X1k=\t(none)\t203.0.113.7:51820\t10.0.0.2/32,fd00:6c6f::2/128\t1759327200\t1234567\t7654321\t25\n" +
	"pB3sYv8hX4jK9mN2qR5tW7zA1cE6fG0iL3oU8yD4bQs=\t(none)\t198.51.100.20:43122\t10.0.0.3/32,192.168.50.0/24\t1759327100\t42\t84\toff\n" +
	"Zk7rT2wX9qL4mN6pB1sV8cF3hJ5gD0aE2iO7uY4tRx0=\t(none)\t(none)\t10.0.0.4/32\t0\t0\t0\toff\n" +
	"Qw9eR4tY7uI2oP5aS8dF1gH3jK6lZ0xC9vB2nM5qWe4=\t(none)\t[2001:db8::9]:51820\t0.0.0.0/0,::/0\t1759327000\t1\t2\toff\n"

func TestParseWireGuardDump(t *testing.T) {
	// Targets come from single-address allowed-ips, never from the public
	// endpoint column; the routed /24 and the default routes are skipped.
	got := parseWireGuardDump(wgDump)
	want := []string{
		"10.0.0.2:8911",
		"[fd00:6c6f::2]:8911",
		"10.0.0.3:8911",
		"10.0.0.4:8911",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("parseWireGuardDump = %v, want %v", got, want)
	}
}

func TestParseWireGuardDumpInterfaceOnly(t *testing.T) {
	// An interface without peers prints only its own line; its fwmark
	// column must never be mistaken for allowed-ips.
	if got := parseWireGuardDump("cHJpdg==\tcHVi\t51820\toff\n"); len(got) != 0 {
		t.Fatalf("interface-only dump yielded %v", got)
	}
	if got := parseWireGuardDump(""); len(got) != 0 {
		t.Fatalf("empty dump yielded %v", got)
	}
}

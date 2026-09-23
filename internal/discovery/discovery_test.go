package discovery

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"locus/internal/identity"
	"locus/internal/peer"
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

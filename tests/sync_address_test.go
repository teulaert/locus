package tests

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
)

// A peer whose discovery reply advertises an address nobody can reach (the
// default-route interface rather than the WireGuard one, modelled here as a
// TEST-NET host) is still synchronized with, over the address that answered
// the probe: only the advertised port is taken from the reply (LOC-10).
//
// Node b runs normally but is not a probe target itself; a probes a
// responder speaking with b's identity whose announcement names
// 192.0.2.1:<b's sync port>, exactly what b would send from a host whose
// default route is not the tunnel.
func TestUnroutableAdvertisedSyncAddrStillSyncs(t *testing.T) {
	ports := allocatePorts(t, 3)
	a := newNode(t, "peer1", ports[0], ports[:2], 0)
	b := newNode(t, "peer2", ports[1], ports[:2], 1)
	responderPort := ports[2].disc
	a.Cfg.Discovery.Peers = []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(responderPort))}
	b.Cfg.Discovery.Peers = nil
	// b never probes a, so it learns of a only through its allowlist.
	b.Cfg.AuthorizedPeers = []string{a.PeerID}

	bID, err := identity.Load(filepath.Join(b.Dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	advertised := fmt.Sprintf("192.0.2.1:%d", b.Cfg.Peer.Port)
	responder := &discovery.Server{
		Port:     responderPort,
		Identity: bID,
		Info:     peer.Info{PeerID: bID.PeerID, Hostname: "peer2", Version: "0.1.0", SyncAddr: advertised, Capabilities: []string{"sync"}},
	}
	if err := responder.ListenAndServe(ctx); err != nil {
		t.Fatal(err)
	}

	c := &cluster{t: t, nodes: []*node{a, b}}
	a.start(t)
	b.start(t)
	t.Cleanup(c.stop)

	want := fmt.Sprintf("127.0.0.1:%d", b.Cfg.Peer.Port)
	waitFor(t, 20*time.Second, func() bool {
		for _, e := range a.App.Presence.Online() {
			if e.PeerID == b.PeerID {
				return e.SyncAddr == want
			}
		}
		return false
	}, "b to be present at the probed host")

	a.write(t, "from-a.txt", "pushed to the address that answered\n")
	b.write(t, "from-b.txt", "pulled from the address that answered\n")
	waitFor(t, 30*time.Second, func() bool {
		return b.has(t, "from-a.txt") && a.has(t, "from-b.txt") && treesMatch(t, a, b)
	}, "both peers to converge despite b's unroutable advertisement")

	for _, e := range a.App.Presence.Snapshot() {
		if e.PeerID == b.PeerID && (!e.Reachable || e.SyncAddr != want) {
			t.Fatalf("b's presence entry %+v, want reachable at %s", e, want)
		}
	}
}

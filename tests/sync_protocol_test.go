package tests

import (
	"testing"
	"time"

	"github.com/teulaert/locus/internal/peer"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// Peers of the same build advertise their sync protocol in discovery, check
// it on every transfer (LOC-17), and still synchronize; status shows the
// version each peer speaks. Interop with a peer from before the protocol was
// advertised is covered by TestUnroutableAdvertisedSyncAddrStillSyncs, whose
// responder announces no sync protocol.
func TestSyncProtocolNegotiatedBetweenCurrentPeers(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	a.write(t, "from-a.txt", "negotiated\n")
	b.write(t, "from-b.txt", "and accepted\n")
	waitFor(t, 30*time.Second, func() bool {
		return b.has(t, "from-a.txt") && a.has(t, "from-b.txt") && treesMatch(t, a, b)
	}, "current-version peers to converge")

	for _, n := range c.nodes {
		for _, e := range n.App.Presence.Snapshot() {
			if e.State == peer.Incompatible || e.SyncProtocol != syncpkg.ProtocolVersion {
				t.Fatalf("%s sees peer %s as %s speaking sync protocol %d, want %d", n.Name, e.Hostname, e.State, e.SyncProtocol, syncpkg.ProtocolVersion)
			}
		}
	}
}

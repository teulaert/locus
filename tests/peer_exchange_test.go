package tests

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// LOC-62 acceptance: three peers A (always on), B and C. B and C configure
// only A; A configures nothing. After discovery B and C see each other online
// through A's peer list, exchange files directly while A is stopped, and find
// each other again after both restart with A still down, from the peers they
// learned and persisted in state.db.
func TestPeerExchangeThroughAlwaysOnPeer(t *testing.T) {
	ports := allocatePorts(t, 3)
	a := newNode(t, "server", ports[0], ports, 0)
	b := newNode(t, "client-b", ports[1], ports, 1)
	c := newNode(t, "client-c", ports[2], ports, 2)
	a.Cfg.Discovery.Peers = nil
	only := []string{fmt.Sprintf("127.0.0.1:%d", ports[0].disc)}
	b.Cfg.Discovery.Peers = only
	c.Cfg.Discovery.Peers = only
	for _, n := range []*node{a, b, c} {
		if err := n.Cfg.Save(filepath.Join(n.Dir, "config.json")); err != nil {
			t.Fatal(err)
		}
	}
	cl := &cluster{t: t, nodes: []*node{a, b, c}}
	t.Cleanup(cl.stop)
	for _, n := range cl.nodes {
		n.start(t)
	}

	sees := func(x, y *node) bool {
		for _, e := range x.App.Presence.Online() {
			if e.PeerID == y.PeerID {
				return true
			}
		}
		return false
	}
	waitFor(t, 20*time.Second, func() bool {
		return sees(b, c) && sees(c, b) && sees(a, b) && sees(a, c)
	}, "B and C to discover each other through A")

	// Give the daemons a persistence tick (every 5s) to write the address
	// books, then take A down and make sure B and C talk directly.
	waitFor(t, 20*time.Second, func() bool {
		rows, err := b.App.Store.ListLearnedPeers()
		if err != nil {
			return false
		}
		for _, r := range rows {
			if r.PeerID == c.PeerID {
				return true
			}
		}
		return false
	}, "B to persist C as a learned peer")
	a.stopServing(t)

	b.write(t, "from-b.txt", "written on b while the server is down\n")
	c.write(t, "from-c.txt", "written on c while the server is down\n")
	waitFor(t, 30*time.Second, func() bool {
		return c.has(t, "from-b.txt") && b.has(t, "from-c.txt")
	}, "B and C to sync directly with A stopped")

	// Restart both clients; A stays down.
	b.stopServing(t)
	c.stopServing(t)
	b.restart(t)
	c.restart(t)
	waitFor(t, 20*time.Second, func() bool { return sees(b, c) && sees(c, b) }, "B and C to find each other after restarting without A")
	c.write(t, "after-restart.txt", "still syncing without the server\n")
	waitFor(t, 30*time.Second, func() bool {
		return b.has(t, "after-restart.txt") && treesMatch(t, b, c)
	}, "B and C to converge after restarting without A")
}

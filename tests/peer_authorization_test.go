package tests

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// newAuthorizedCluster is newCluster with every node's authorized_peers set
// to the IDs of the other nodes.
func newAuthorizedCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{t: t}
	ports := allocatePorts(t, n)
	for i := 0; i < n; i++ {
		c.nodes = append(c.nodes, newNode(t, fmt.Sprintf("peer%d", i+1), ports[i], ports, i))
	}
	for _, nd := range c.nodes {
		for _, other := range c.nodes {
			if other != nd {
				nd.Cfg.AuthorizedPeers = append(nd.Cfg.AuthorizedPeers, other.PeerID)
			}
		}
	}
	for _, nd := range c.nodes {
		nd.start(t)
	}
	t.Cleanup(c.stop)
	c.waitConnected()
	return c
}

// An outsider on the network sends one validly self-signed discovery probe
// to a peer whose authorized_peers excludes it, advertising a sync address
// it controls. The peer must neither list it as present nor push any of its
// files there, while synchronization with the authorized peer carries on.
func TestUnauthorizedProbeDoesNotBecomeSyncTarget(t *testing.T) {
	c := newAuthorizedCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	var hits atomic.Int64
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "sink", http.StatusTeapot)
	}))
	defer sink.Close()

	intruder, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	info := peer.Info{PeerID: intruder.PeerID, Hostname: "intruder", Version: "0.1.0", SyncAddr: sink.Listener.Addr().String()}
	target := net.JoinHostPort("127.0.0.1", strconv.Itoa(a.Cfg.Discovery.Port))
	if _, err := discovery.ProbeOnce(context.Background(), target, intruder, info, 500*time.Millisecond); err == nil {
		t.Fatal("a peer with authorized_peers answered a probe from an unlisted peer")
	}

	// A file written after the probe reaching b proves a has completed sync
	// rounds since the intruder knocked.
	a.write(t, "after-probe.txt", "only for authorized peers\n")
	waitFor(t, 30*time.Second, func() bool { return b.has(t, "after-probe.txt") }, "authorized peer to receive the file")

	if a.App.Presence.Known(intruder.PeerID) {
		t.Fatal("unauthorized prober was recorded in presence")
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("peer contacted the unauthorized peer's sync address %d times", n)
	}
}

// With an empty authorized_peers list the sync service serves peers this
// node has discovered, not any holder of a key pair.
func TestEmptyAllowlistServesOnlyDiscoveredPeers(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]

	stranger, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := func() int {
		target := "/v1/manifest?rootname=root"
		req, err := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d%s", a.Cfg.Peer.Port, target), nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range syncpkg.AuthHeaders(stranger, "GET", target, nil) {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := manifest(); got != http.StatusForbidden {
		t.Fatalf("undiscovered peer got manifest status %d, want 403", got)
	}

	// Once it announces itself over discovery it is a discovered peer.
	info := peer.Info{PeerID: stranger.PeerID, Hostname: "newcomer", Version: "0.1.0", SyncAddr: "127.0.0.1:1"}
	target := net.JoinHostPort("127.0.0.1", strconv.Itoa(a.Cfg.Discovery.Port))
	if _, err := discovery.ProbeOnce(context.Background(), target, stranger, info, 2*time.Second); err != nil {
		t.Fatalf("probe: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return manifest() == http.StatusOK }, "discovered peer to be served")
}

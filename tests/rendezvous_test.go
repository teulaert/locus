package tests

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
)

// newRendezvousNode is a node configured as a discovery-only peer: no roots
// and nothing configured to probe (LOC-76).
func newRendezvousNode(t *testing.T, p portSet, all []portSet, idx int) *node {
	t.Helper()
	n := newNode(t, "rendezvous", p, all, idx)
	n.Cfg.Roots = nil
	n.Cfg.Rendezvous = true
	n.Cfg.Discovery.Peers = nil
	n.Root = ""
	if err := n.Cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := n.Cfg.Save(filepath.Join(n.Dir, "config.json")); err != nil {
		t.Fatal(err)
	}
	return n
}

// prober is a bare discovery client: an identity that probes and announces a
// discovery port, as a client peer does.
type prober struct {
	id   *identity.Identity
	info peer.Info
	port int
}

func newProber(t *testing.T, name string) *prober {
	t.Helper()
	id, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	port := freePort(t)
	return &prober{id: id, port: port, info: peer.Info{
		PeerID: id.PeerID, Hostname: name, Version: "test",
		SyncAddr: net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t))),
		Capabilities: []string{peer.SyncCapability, peer.PeerExchangeCapability,
			peer.DiscoveryPortCapability(port)},
	}}
}

func (p *prober) probe(t *testing.T, addr string) *discovery.Packet {
	t.Helper()
	reply, err := discovery.ProbeOnce(context.Background(), addr, p.id, p.info, 2*time.Second)
	if err != nil {
		t.Fatalf("probe %s: %v", addr, err)
	}
	return reply
}

func lists(reply *discovery.Packet, peerID, disc string) bool {
	for _, k := range reply.Peers {
		if k.PeerID == peerID && k.DiscAddr == disc {
			return true
		}
	}
	return false
}

// LOC-76: a rendezvous peer has no roots, answers discovery with an
// announcement that has no sync capability, runs no sync service, learns
// every peer that probes it and hands each the others; the learned list
// survives a restart.
func TestRendezvousIntroducesProbers(t *testing.T) {
	ports := allocatePorts(t, 1)
	r := newRendezvousNode(t, ports[0], ports, 0)
	cl := &cluster{t: t, nodes: []*node{r}}
	t.Cleanup(cl.stop)
	r.start(t)
	addr := fmt.Sprintf("127.0.0.1:%d", ports[0].disc)

	a, b := newProber(t, "client-a"), newProber(t, "client-b")
	first := a.probe(t, addr)
	if first.Info.Has(peer.SyncCapability) || !first.Info.Has(peer.RendezvousCapability) || first.Info.Syncs() {
		t.Fatalf("rendezvous announces %v; want rendezvous and no sync", first.Info.Capabilities)
	}
	if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ports[0].peer), time.Second); err == nil {
		c.Close()
		t.Fatal("a rendezvous peer listens on the sync port")
	}

	aDisc := fmt.Sprintf("127.0.0.1:%d", a.port)
	bDisc := fmt.Sprintf("127.0.0.1:%d", b.port)
	if reply := b.probe(t, addr); !lists(reply, a.id.PeerID, aDisc) {
		t.Fatalf("reply to B lists %+v; want A at %s", reply.Peers, aDisc)
	}
	if reply := a.probe(t, addr); !lists(reply, b.id.PeerID, bDisc) || lists(reply, a.id.PeerID, aDisc) {
		t.Fatalf("reply to A lists %+v; want B at %s and not A itself", reply.Peers, bDisc)
	}

	// The status says what this peer is.
	st, err := r.App.BuildStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !st.Rendezvous || len(st.Roots) != 0 {
		t.Fatalf("status rendezvous=%v roots=%v", st.Rendezvous, st.Roots)
	}
	if _, err := r.App.SyncNow(context.Background()); err != app.ErrRendezvous {
		t.Fatalf("SyncNow on a rendezvous peer = %v, want ErrRendezvous", err)
	}

	// Learned peers are persisted, so a restart keeps the list.
	waitFor(t, 20*time.Second, func() bool {
		rows, err := r.App.Store.ListLearnedPeers()
		return err == nil && len(rows) == 2
	}, "the rendezvous peer to persist both probers")
	r.stopServing(t)
	r.restart(t)
	c := newProber(t, "client-c")
	if reply := c.probe(t, addr); !lists(reply, a.id.PeerID, aDisc) || !lists(reply, b.id.PeerID, bDisc) {
		t.Fatalf("after a restart the reply lists %+v; want A and B", reply.Peers)
	}
}

// LOC-76 acceptance: two clients with no discovery.peers find each other
// through a rendezvous peer, the one address their routing table leads to
// (injected here as the gateway target; route parsing is covered by the
// discovery package's fixtures), and synchronize. Neither ever tries to
// synchronize with the rendezvous peer, which they show as "rendezvous".
func TestZeroConfigDiscoveryThroughRendezvous(t *testing.T) {
	ports := allocatePorts(t, 3)
	r := newRendezvousNode(t, ports[0], ports, 0)
	b := newNode(t, "client-b", ports[1], ports, 1)
	c := newNode(t, "client-c", ports[2], ports, 2)
	gateway := fmt.Sprintf("127.0.0.1:%d", ports[0].disc)
	for _, n := range []*node{b, c} {
		n.Cfg.Discovery.Peers = nil
		if err := n.Cfg.Save(filepath.Join(n.Dir, "config.json")); err != nil {
			t.Fatal(err)
		}
		n.beforeServe = func(a *app.App) {
			a.GatewayTargets = func() []string { return []string{gateway} }
		}
	}
	cl := &cluster{t: t, nodes: []*node{r, b, c}}
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
		return sees(b, c) && sees(c, b) && sees(r, b) && sees(r, c)
	}, "B and C to discover each other through the rendezvous peer")

	b.write(t, "zero-config.txt", "found without configuration\n")
	waitFor(t, 30*time.Second, func() bool {
		got, ok := c.read(t, "zero-config.txt")
		return ok && strings.Contains(got, "without configuration")
	}, "C to receive B's file")

	for _, n := range []*node{b, c} {
		var found bool
		for _, e := range n.App.Presence.Snapshot() {
			if e.PeerID != r.PeerID {
				continue
			}
			found = true
			if e.State != peer.Rendezvous || e.Failures != 0 {
				t.Fatalf("%s shows the rendezvous peer as %s with %d transfer failures (%s)", n.Name, e.State, e.Failures, e.LastError)
			}
		}
		if !found {
			t.Fatalf("%s never saw the rendezvous peer", n.Name)
		}
		if sees(n, r) {
			t.Fatalf("%s counts the rendezvous peer among its sync peers", n.Name)
		}
	}
}

// LOC-76: `locus init --rendezvous` writes a rootless configuration (also
// as a system service), status and peers say what the peer is, and the
// commands that need roots refuse with a clear message and exit 4.
func TestCLIRendezvous(t *testing.T) {
	dir := t.TempDir()
	c := &cli{t: t, binary: locusBinary(t), config: filepath.Join(dir, "config.json"), dataDir: filepath.Join(dir, "data"), home: filepath.Join(dir, "home")}
	t.Cleanup(c.requireDefaultDirsUntouched)

	if code, _, stderr := c.run("init", "--rendezvous", "--root", filepath.Join(dir, "docs")); code != 2 || !strings.Contains(stderr, "no roots") {
		t.Fatalf("init --rendezvous --root: exit %d, stderr %q; want 2 saying a rendezvous peer has no roots", code, stderr)
	}
	out := c.mustJSON("init", "--rendezvous", "--json")
	requireKeys(t, out, "config", "peer_id", "roots", "gateway_targets", "rendezvous")
	if out["rendezvous"] != true || len(out["roots"].([]any)) != 0 {
		t.Fatalf("init --rendezvous reported %v", out)
	}
	if _, ok := out["gateway_targets"].([]any); !ok {
		t.Fatalf("gateway_targets is %T, want a list", out["gateway_targets"])
	}

	st := c.mustJSON("status", "--json")
	if st["rendezvous"] != true {
		t.Fatalf("status --json = %v, want rendezvous true", st)
	}
	if code, stdout, _ := c.run("status"); code != 0 || !strings.Contains(stdout, "rendezvous peer, no roots") {
		t.Fatalf("status: exit %d, stdout %q", code, stdout)
	}
	if _, stdout, _ := c.run("peers"); !strings.Contains(stdout, "rendezvous peer, no roots") {
		t.Fatalf("peers: stdout %q", stdout)
	}
	for _, args := range [][]string{{"sync", "now"}, {"index", "rebuild"}, {"search", "invoice"}, {"conflicts"}} {
		code, stdout, stderr := c.run(args...)
		if code != 4 || !strings.Contains(stderr, "rendezvous peer: no roots") || stdout != "" {
			t.Fatalf("%s: exit %d, stdout %q, stderr %q; want exit 4 and the rendezvous message", strings.Join(args, " "), code, stdout, stderr)
		}
	}
	for _, args := range [][]string{{"sync", "status"}, {"index", "status"}} {
		if code, _, stderr := c.run(args...); code != 0 {
			t.Fatalf("%s: exit %d, stderr %q", strings.Join(args, " "), code, stderr)
		}
	}
}

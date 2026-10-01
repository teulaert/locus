// Package tests contains the end-to-end acceptance suite from
// LOCUS_DESIGN.md §23: real peers, real filesystems, real HTTP, no mocks.
package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/api"
	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/search"
)

// node is one running Locus peer with its own roots, data dir and ports.
type node struct {
	Name   string
	Root   string
	Dir    string
	Cfg    *config.Config
	App    *app.App
	APIURL string
	Token  string
	PeerID string

	stop   context.CancelFunc
	done   chan struct{}
	served bool
}

type cluster struct {
	t     *testing.T
	nodes []*node
}

// newCluster starts n peers that know each other through static discovery
// targets, which is how Locus operates over an existing WireGuard network.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	return newClusterWith(t, n, nil, nil)
}

// rootLayout chooses node i's root folder name and its configured root name
// ("" leaves the root unnamed, so it pairs by folder name).
type rootLayout func(i int) (folder, name string)

// newClusterWithRoots is newCluster with per-node root folders and names; a
// nil layout gives every node an unnamed root called "root".
func newClusterWithRoots(t *testing.T, n int, layout rootLayout) *cluster {
	t.Helper()
	return newClusterWith(t, n, layout, nil)
}

// newClusterWith is newClusterWithRoots with configure applied to every
// node's configuration before it starts, for knobs a scenario has to force.
func newClusterWith(t *testing.T, n int, layout rootLayout, configure func(*config.Config)) *cluster {
	t.Helper()
	if layout == nil {
		layout = func(int) (string, string) { return "root", "" }
	}
	c := &cluster{t: t}
	ports := allocatePorts(t, n)
	for i := 0; i < n; i++ {
		folder, name := layout(i)
		c.nodes = append(c.nodes, newNodeWithRoot(t, fmt.Sprintf("peer%d", i+1), ports[i], ports, i, folder, name))
	}
	for _, nd := range c.nodes {
		if configure != nil {
			configure(nd.Cfg)
		}
		nd.start(t)
	}
	t.Cleanup(c.stop)
	c.waitConnected()
	return c
}

type portSet struct{ peer, api, disc int }

func allocatePorts(t *testing.T, n int) []portSet {
	t.Helper()
	out := make([]portSet, n)
	for i := 0; i < n; i++ {
		out[i] = portSet{peer: freePort(t), api: freePort(t), disc: freePort(t)}
	}
	return out
}

// Test ports come from below the kernel's ephemeral range (32768 and up on
// Linux, 49152 and up on macOS and Windows). A port is only probed here and
// bound later, and a stopped node leaves its ports unbound until it restarts;
// a port from the ephemeral range can meanwhile become the source port of any
// outgoing connection on the machine (other test packages run in parallel),
// and the restart then fails with "address already in use". Nothing picks
// these ports by itself, so they stay ours for the life of the test binary.
const (
	testPortLow  = 20000
	testPortHigh = 32768
)

var (
	portMu    sync.Mutex
	portsUsed = map[int]bool{}
)

func freePort(t *testing.T) int {
	t.Helper()
	portMu.Lock()
	defer portMu.Unlock()
	for i := 0; i < 1000; i++ {
		p := testPortLow + rand.IntN(testPortHigh-testPortLow)
		if portsUsed[p] || !portFree(p) {
			continue
		}
		portsUsed[p] = true
		return p
	}
	t.Fatal("no free test port below the ephemeral range")
	return 0
}

// portFree reports whether both TCP and UDP can bind p on loopback (a node
// uses its ports for either: peer and API are TCP, discovery is UDP).
func portFree(p int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
	if err != nil {
		return false
	}
	ln.Close()
	pc, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", p))
	if err != nil {
		return false
	}
	pc.Close()
	return true
}

func newNode(t *testing.T, name string, p portSet, all []portSet, idx int) *node {
	t.Helper()
	return newNodeWithRoot(t, name, p, all, idx, "root", "")
}

// newNodeWithRoot is newNode with the root at folder (under a fresh temporary
// directory) and configured under rootName, or unnamed when rootName is "".
func newNodeWithRoot(t *testing.T, name string, p portSet, all []portSet, idx int, folder, rootName string) *node {
	t.Helper()
	base := t.TempDir()
	root := filepath.Join(base, folder)
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = []config.Root{{Path: root, Name: rootName}}
	cfg.DataDir = filepath.Join(base, "data")
	cfg.Peer.Addr = "127.0.0.1"
	cfg.Peer.Port = p.peer
	cfg.API.Addr = "127.0.0.1"
	cfg.API.Port = p.api
	cfg.API.Token = fmt.Sprintf("test-token-%d", idx)
	cfg.Discovery.Port = p.disc
	cfg.Discovery.Interval = config.Duration(300 * time.Millisecond)
	cfg.Sync.Interval = config.Duration(400 * time.Millisecond)
	cfg.ReconcileInterval = config.Duration(400 * time.Millisecond)
	cfg.Workers = 2
	for i, other := range all {
		if i == idx {
			continue
		}
		cfg.Discovery.Peers = append(cfg.Discovery.Peers, fmt.Sprintf("127.0.0.1:%d", other.disc))
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	nd := &node{Name: name, Root: root, Dir: base, Cfg: cfg, APIURL: fmt.Sprintf("http://127.0.0.1:%d", p.api), Token: cfg.API.Token}
	if err := cfg.Save(filepath.Join(base, "config.json")); err != nil {
		t.Fatal(err)
	}
	// Materialize identity so tests can reference peer IDs before serving.
	id, err := identity.Load(filepath.Join(base, "data"))
	if err != nil {
		t.Fatal(err)
	}
	nd.PeerID = id.PeerID
	return nd
}

func (n *node) start(t *testing.T) {
	t.Helper()
	logOut := io.Discard
	if os.Getenv("TEST_LOG") == "1" {
		logOut = os.Stderr
	}
	log := slog.New(slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a, err := app.New(n.Cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	n.App = a
	h, err := api.Handler(a)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.stop = cancel
	n.done = make(chan struct{})
	go func() {
		defer close(n.done)
		if err := a.Serve(ctx, app.WithAPI(h)); err != nil {
			t.Errorf("%s: serve: %v", n.Name, err)
		}
	}()
	n.served = true
	// Wait until the peer is actually accepting API traffic before the test
	// proceeds; Serve binds its listeners asynchronously.
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := http.Get(n.APIURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s API never became ready at %s", n.Name, n.APIURL)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (n *node) stopServing(t *testing.T) {
	t.Helper()
	if !n.served {
		return
	}
	n.stop()
	select {
	case <-n.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not shut down", n.Name)
	}
	n.served = false
	n.App.Close()
	n.App = nil
}

// restart brings a stopped node back with a fresh process-equivalent state:
// a new App over the same data directory, exactly like relaunching the
// binary after a laptop rejoins the WireGuard network.
func (n *node) restart(t *testing.T) {
	t.Helper()
	if n.served {
		t.Fatalf("%s already running", n.Name)
	}
	n.start(t)
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		if n.served {
			n.stop()
			<-n.done
			n.served = false
			if n.App != nil {
				n.App.Close()
			}
		}
	}
}

// waitConnected waits until every peer reports the others online.
func (c *cluster) waitConnected() {
	c.t.Helper()
	waitFor(c.t, 20*time.Second, func() bool {
		for _, n := range c.nodes {
			if len(n.App.Presence.Online()) < len(c.nodes)-1 {
				return false
			}
		}
		return true
	}, "peers to connect")
}

// waitFor polls a condition; Locus converges asynchronously, so every
// assertion is eventually-consistent by construction.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// write creates or overwrites a file inside a node's root.
func (n *node) write(t *testing.T, rel, content string) {
	t.Helper()
	full := filepath.Join(n.Root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (n *node) remove(t *testing.T, rel string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(n.Root, filepath.FromSlash(rel))); err != nil {
		t.Fatal(err)
	}
}

func (n *node) read(t *testing.T, rel string) (string, bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(n.Root, filepath.FromSlash(rel)))
	if os.IsNotExist(err) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b), true
}

func (n *node) has(t *testing.T, rel string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(n.Root, filepath.FromSlash(rel)))
	return err == nil
}

func (n *node) listRel(t *testing.T) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(n.Root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(n.Root, path)
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	return out
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// converged reports whether two nodes present identical file trees.
func treesMatch(t *testing.T, a, b *node) bool {
	t.Helper()
	fa, fb := a.listRel(t), b.listRel(t)
	if len(fa) != len(fb) {
		return false
	}
	for _, rel := range fa {
		ca, _ := a.read(t, rel)
		cb, ok := b.read(t, rel)
		if !ok || hashString(ca) != hashString(cb) {
			return false
		}
	}
	return true
}

func (c *cluster) waitForConvergence() {
	c.t.Helper()
	waitFor(c.t, 30*time.Second, func() bool {
		for _, n := range c.nodes[1:] {
			if !treesMatch(c.t, c.nodes[0], n) {
				return false
			}
		}
		return true
	}, "trees to converge")
}

// apiCall performs an authenticated request against a node's API.
func (n *node) apiCall(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, n.APIURL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+n.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// apiWithContentType sends a request with an explicit Content-Type. Uploads
// must survive whatever type the client picked: curl defaults to
// application/x-www-form-urlencoded, whose parsing would swallow the body.
func (n *node) apiWithContentType(t *testing.T, method, path, token, contentType string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, n.APIURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (n *node) apiRaw(t *testing.T, method, path, token, body string) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, n.APIURL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// searchNode runs a CLI-level search through the service layer.
func (n *node) search(t *testing.T, query string, opts search.Options) []*search.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := n.App.Search.Search(ctx, query, opts)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func (n *node) indexStatus(t *testing.T) map[string]any {
	t.Helper()
	stats, err := n.App.Store.GetIndexStats(n.App.Embedder.ID())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{
		"documents":  stats.Documents,
		"chunks":     stats.Chunks,
		"embeddings": stats.Embeddings,
		"stale":      stats.Stale,
	}
	return out
}

func decode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("malformed JSON response: %v\n%s", err, b)
	}
	return out
}

package tests

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/identity"
	syncpkg "github.com/teulaert/locus/internal/sync"
	"github.com/teulaert/locus/internal/vvector"
)

// peerClient speaks the peer protocol to one node's sync port as a given
// identity, through the real HTTP listener the app opened.
type peerClient struct {
	t    *testing.T
	base string
	id   *identity.Identity
}

func newPeerClient(t *testing.T, target *node, as *identity.Identity) *peerClient {
	return &peerClient{t: t, base: fmt.Sprintf("http://127.0.0.1:%d", target.Cfg.Peer.Port), id: as}
}

// conformingHash is the body hash a well-behaved peer signs (LOC-13): for a
// put, the content hash it declares in its signed query; for every other
// request, the hash of an empty body.
func conformingHash(t *testing.T, path string, q url.Values) []byte {
	t.Helper()
	if path != "/v1/put" {
		return sum("")
	}
	h, err := hex.DecodeString(q.Get("hash"))
	if err != nil {
		t.Fatalf("put query declares a malformed hash %q", q.Get("hash"))
	}
	return h
}

// sum is the SHA-256 of s, for signing a hash other than the declared one.
func sum(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

// do sends sendBody in a request signed over signHash, so a test can sign
// one thing and send another; a nil signHash signs the way the protocol
// expects (conformingHash). A nil identity sends no signature at all.
func (p *peerClient) do(method, path string, q url.Values, signHash, sendBody []byte) (int, http.Header, []byte) {
	p.t.Helper()
	target := path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	req, err := http.NewRequest(method, p.base+target, bytes.NewReader(sendBody))
	if err != nil {
		p.t.Fatal(err)
	}
	if p.id != nil {
		if signHash == nil {
			signHash = conformingHash(p.t, path, q)
		}
		for k, v := range syncpkg.AuthHeadersHash(p.id, method, target, signHash) {
			req.Header.Set(k, v)
		}
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		p.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func (p *peerClient) signed(method, path string, q url.Values, body []byte) (int, http.Header, []byte) {
	p.t.Helper()
	return p.do(method, path, q, nil, body)
}

// applyResult decodes the {"result": ...} reply of /v1/put and /v1/delete.
func applyResult(t *testing.T, code int, body []byte) string {
	t.Helper()
	if code != http.StatusOK {
		t.Fatalf("apply request returned %d: %s", code, body)
	}
	return fmt.Sprint(decode(t, body)["result"])
}

func putQuery(rel, content string, ver vvector.Vector, writer string) url.Values {
	return url.Values{
		"rootname": {"root"},
		"relpath":  {rel},
		"hash":     {hashString(content)},
		"size":     {fmt.Sprint(len(content))},
		"version":  {ver.String()},
		"writer":   {writer},
	}
}

// The sync server, reached through the listener a running node opened on
// its peer port, serves every endpoint to an authorized peer and refuses
// everything else: bad or missing signatures, strangers, unknown roots,
// traversal, content that does not match its hash and content over the size
// limit. Nothing refused may leave a file or a temporary behind (LOC-25).
func TestSyncServerEndToEndOverPeerPort(t *testing.T) {
	const limit = 64 << 10
	c := &cluster{t: t}
	ports := allocatePorts(t, 2)
	for i := 0; i < 2; i++ {
		c.nodes = append(c.nodes, newNode(t, fmt.Sprintf("peer%d", i+1), ports[i], ports, i))
	}
	a, b := c.nodes[0], c.nodes[1]
	a.Cfg.UploadMaxBytes = limit
	for _, nd := range c.nodes {
		nd.start(t)
	}
	t.Cleanup(c.stop)
	c.waitConnected()

	// b's own identity, as the running b would sign with it.
	bID, err := identity.Load(b.Cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := identity.Load(filepath.Join(t.TempDir(), "stranger"))
	if err != nil {
		t.Fatal(err)
	}
	asB := newPeerClient(t, a, bID)

	// whoami is the only unauthenticated endpoint.
	code, _, body := newPeerClient(t, a, nil).do(http.MethodGet, "/v1/whoami", nil, nil, nil)
	if code != http.StatusOK || decode(t, body)["peer_id"] != a.PeerID {
		t.Fatalf("whoami = %d %s", code, body)
	}

	// manifest + file: what a wrote is listed and served byte for byte.
	a.write(t, "seed.txt", "seeded on a\n")
	var m syncpkg.Manifest
	waitFor(t, 30*time.Second, func() bool {
		code, _, body := asB.signed(http.MethodGet, "/v1/manifest", url.Values{"rootname": {"root"}}, nil)
		if code != http.StatusOK {
			t.Fatalf("manifest = %d %s", code, body)
		}
		m = syncpkg.Manifest{}
		if err := json.Unmarshal(body, &m); err != nil {
			t.Fatalf("manifest is not JSON: %v", err)
		}
		for _, e := range m.Entries {
			if e.RelPath == "seed.txt" && !e.Deleted {
				return true
			}
		}
		return false
	}, "seed.txt to appear in a's manifest")
	if m.PeerID != a.PeerID || m.RootName != "root" {
		t.Fatalf("manifest identifies %q/%q, want %s/root", m.PeerID, m.RootName, a.PeerID)
	}
	code, hdr, body := asB.signed(http.MethodGet, "/v1/file", url.Values{"rootname": {"root"}, "relpath": {"seed.txt"}}, nil)
	if code != http.StatusOK || string(body) != "seeded on a\n" || hdr.Get("X-Locus-Hash") != hashString("seeded on a\n") {
		t.Fatalf("file = %d %q hash=%s", code, body, hdr.Get("X-Locus-Hash"))
	}
	if code, _, _ := asB.signed(http.MethodGet, "/v1/file", url.Values{"rootname": {"root"}, "relpath": {"missing.txt"}}, nil); code != http.StatusNotFound {
		t.Fatalf("file for a missing path = %d, want 404", code)
	}

	// put: a new file lands on a's disk.
	v1 := vvector.New().Increment(b.PeerID)
	const content = "pushed by b over the wire\n"
	if code, _, body := asB.signed(http.MethodPost, "/v1/put", putQuery("e2e/pushed.txt", content, v1, b.PeerID), []byte(content)); applyResult(t, code, body) != string(syncpkg.ResultApplied) {
		t.Fatalf("put result: %s", body)
	}
	if got, ok := a.read(t, "e2e/pushed.txt"); !ok || got != content {
		t.Fatalf("put did not install the file on a: %q ok=%v", got, ok)
	}

	// Refusals. Each must leave a's tree as it is.
	refused := []struct {
		name     string
		client   *peerClient
		path     string
		q        url.Values
		sign     []byte // nil: sign as the protocol expects
		send     []byte
		wantCode int
	}{
		{"unsigned", newPeerClient(t, a, nil), "/v1/put", putQuery("e2e/unsigned.txt", "x", v1, b.PeerID), nil, []byte("x"), http.StatusUnauthorized},
		// Signed for "good", then the declared hash and the body were both
		// swapped for "evil": the signature no longer covers the query.
		{"content tampered after signing", asB, "/v1/put", putQuery("e2e/tampered.txt", "evil", v1, b.PeerID), sum("good"), []byte("evil"), http.StatusUnauthorized},
		{"stranger", newPeerClient(t, a, stranger), "/v1/put", putQuery("e2e/stranger.txt", "x", vvector.New().Increment(stranger.PeerID), stranger.PeerID), nil, []byte("x"), http.StatusForbidden},
		{"stranger manifest", newPeerClient(t, a, stranger), "/v1/manifest", url.Values{"rootname": {"root"}}, nil, nil, http.StatusForbidden},
		{"unknown root", asB, "/v1/put", func() url.Values {
			q := putQuery("e2e/elsewhere.txt", "x", v1, b.PeerID)
			q.Set("rootname", "not-shared")
			return q
		}(), nil, []byte("x"), http.StatusNotFound},
		{"traversal", asB, "/v1/put", putQuery("../escaped.txt", "x", v1, b.PeerID), nil, []byte("x"), http.StatusBadRequest},
		{"traversal delete", asB, "/v1/delete", url.Values{"rootname": {"root"}, "relpath": {"../seed.txt"}, "version": {v1.String()}, "writer": {b.PeerID}}, nil, nil, http.StatusBadRequest},
	}
	for _, r := range refused {
		method := http.MethodPost
		if r.path == "/v1/manifest" {
			method = http.MethodGet
		}
		code, _, body := r.client.do(method, r.path, r.q, r.sign, r.send)
		if code != r.wantCode {
			t.Errorf("%s: status %d (%s), want %d", r.name, code, strings.TrimSpace(string(body)), r.wantCode)
		}
	}
	// A signature by someone else under b's name.
	forged := newPeerClient(t, a, stranger)
	q := putQuery("e2e/forged.txt", "x", v1, b.PeerID)
	target := "/v1/put?" + q.Encode()
	req, _ := http.NewRequest(http.MethodPost, forged.base+target, strings.NewReader("x"))
	for k, v := range syncpkg.AuthHeadersHash(stranger, http.MethodPost, target, sum("x")) {
		req.Header.Set(k, v)
	}
	req.Header.Set("X-Locus-Peer", b.PeerID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("signature by another key under b's peer ID: status %d, want 401", resp.StatusCode)
	}

	// A body that does not match the declared (and correctly signed) hash
	// passes authentication but is rejected by the hash check while it
	// streams in (LOC-13).
	q = putQuery("e2e/mismatch.txt", "what the hash says", v1, b.PeerID)
	if code, _, body := asB.signed(http.MethodPost, "/v1/put", q, []byte("something else entirely")); applyResult(t, code, body) != string(syncpkg.ResultRejected) {
		t.Errorf("hash mismatch was not rejected: %s", body)
	}
	// Content over the size limit is refused with 413 before its body is
	// read, since its declared size and Content-Length exceed the limit
	// (LOC-13; it used to be answered 200 "rejected").
	big := strings.Repeat("o", limit+1)
	if code, _, body := asB.signed(http.MethodPost, "/v1/put", putQuery("e2e/too-big.bin", big, v1, b.PeerID), []byte(big)); code != http.StatusRequestEntityTooLarge {
		t.Errorf("a file over upload_max_bytes: status %d (%s), want 413", code, strings.TrimSpace(string(body)))
	}
	huge := strings.Repeat("o", limit+2<<20)
	if code, _, body := asB.signed(http.MethodPost, "/v1/put", putQuery("e2e/huge.bin", huge, v1, b.PeerID), []byte(huge)); code == http.StatusOK && fmt.Sprint(decode(t, body)["result"]) != string(syncpkg.ResultRejected) {
		t.Errorf("a body far over the limit was accepted: %d %s", code, body)
	}

	for _, rel := range []string{"e2e/unsigned.txt", "e2e/tampered.txt", "e2e/stranger.txt", "e2e/elsewhere.txt", "e2e/forged.txt", "e2e/mismatch.txt", "e2e/too-big.bin", "e2e/huge.bin"} {
		if a.has(t, rel) {
			t.Errorf("refused request left %s on a", rel)
		}
	}
	if got, ok := a.read(t, "seed.txt"); !ok || got != "seeded on a\n" {
		t.Errorf("seed.txt changed under refused requests: %q ok=%v", got, ok)
	}
	if _, err := os.Stat(filepath.Join(a.Dir, "escaped.txt")); err == nil {
		t.Error("traversal put wrote outside the root")
	}
	if left := tempLeftovers(t, a.Root); len(left) != 0 {
		t.Errorf("refused transfers left temporaries: %v", left)
	}

	// delete: a newer version removes the file b pushed.
	v2 := v1.Increment(b.PeerID)
	dq := url.Values{"rootname": {"root"}, "relpath": {"e2e/pushed.txt"}, "version": {v2.String()}, "writer": {b.PeerID}, "hash": {hashString(content)}}
	if code, _, body := asB.signed(http.MethodPost, "/v1/delete", dq, nil); applyResult(t, code, body) != string(syncpkg.ResultApplied) {
		t.Fatalf("delete result: %s", body)
	}
	if a.has(t, "e2e/pushed.txt") {
		t.Fatal("delete did not remove the file on a")
	}
	// A replay of the older put is now stale.
	if code, _, body := asB.signed(http.MethodPost, "/v1/put", putQuery("e2e/pushed.txt", content, v1, b.PeerID), []byte(content)); applyResult(t, code, body) != string(syncpkg.ResultNoop) {
		t.Fatalf("stale put after delete: %s", body)
	}
	if a.has(t, "e2e/pushed.txt") {
		t.Fatal("a stale put resurrected a deleted file")
	}

	// The two running peers still agree afterwards.
	c.waitForConvergence()
}

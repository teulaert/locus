package sync

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	gosync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// countingListener counts the bytes the server reads from each connection
// it accepts: request lines, headers and bodies, everything that crossed
// the link towards it.
type countingListener struct {
	net.Listener
	mu    gosync.Mutex
	conns []*atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	n := new(atomic.Int64)
	l.mu.Lock()
	l.conns = append(l.conns, n)
	l.mu.Unlock()
	return &countingConn{Conn: c, n: n}, nil
}

// total is the bytes read from every connection so far.
func (l *countingListener) total() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var sum int64
	for _, n := range l.conns {
		sum += n.Load()
	}
	return sum
}

// perConn is the bytes read from each connection so far.
func (l *countingListener) perConn() []int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]int64, len(l.conns))
	for i, n := range l.conns {
		out[i] = n.Load()
	}
	return out
}

type countingConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// dedupePeer is a receiving engine behind a real peer server on a counting
// listener, with the paths of the requests it answered.
type dedupePeer struct {
	*engineEnv
	srv *httptest.Server
	ln  *countingListener

	mu    gosync.Mutex
	paths []string
	// afterManifest, when set, runs once a manifest has been answered.
	afterManifest func()
}

func newDedupePeer(t *testing.T, senders ...*identity.Identity) *dedupePeer {
	t.Helper()
	e := newEngineEnv(t)
	e.MaxFile = 8 << 20
	var ids []string
	for _, s := range senders {
		ids = append(ids, s.PeerID)
	}
	p := &dedupePeer{engineEnv: e}
	h := (&Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, AuthorizedPeers: ids}).Handler()
	p.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.paths = append(p.paths, r.URL.Path)
		p.mu.Unlock()
		h.ServeHTTP(w, r)
		if r.URL.Path == "/v1/manifest" && p.afterManifest != nil {
			p.afterManifest()
		}
	}))
	p.ln = &countingListener{Listener: p.srv.Listener}
	p.srv.Listener = p.ln
	p.srv.Start()
	t.Cleanup(p.srv.Close)
	return p
}

func (p *dedupePeer) requests(path string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, got := range p.paths {
		if got == path {
			n++
		}
	}
	return n
}

// bulk is n bytes of text content.
func bulk(n int) string {
	return strings.Repeat("locus push dedupe\n", n/18+1)[:n]
}

// rawPut posts a signed put of content as q describes it, with or without
// "Expect: 100-continue", the way a pusher of this build or of an earlier
// one sends it. It returns the status and the decoded reply.
func rawPut(t *testing.T, client *http.Client, base string, sender *identity.Identity, q url.Values, body io.Reader, expect bool) (int, map[string]any) {
	t.Helper()
	target := "/v1/put?" + q.Encode()
	declared, err := DeclaredContentHash(q.Get("hash"))
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, base+target, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range AuthHeadersHash(sender, http.MethodPost, target, declared) {
		req.Header.Set(k, v)
	}
	if expect {
		req.Header.Set("Expect", "100-continue")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// newClient is an HTTP client of its own, which waits long enough for 100
// Continue that a slow test machine cannot make it send a body early.
func newClient(t *testing.T) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ExpectContinueTimeout = 10 * time.Second
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}
}

func (e *engineEnv) docVersion(t *testing.T, rel string) (vvector.Vector, string) {
	t.Helper()
	doc, err := e.Store.GetDoc(e.root, rel)
	if err != nil {
		t.Fatal(err)
	}
	return doc.Version, doc.LastWriter
}

// LOC-78: a put of content the receiver already holds at the path is
// answered before its body is read: the pusher's "Expect: 100-continue"
// is never granted, so the body never crosses the link, and the version it
// carries is applied as receiving the bytes would have. The pusher counts
// it as skipped_held, neither pushed nor failed, and the next round has
// nothing left to do. The push is planned from a manifest that lacked the
// file: the receiver got it from another peer between answering the
// manifest and the put, as a joining peer does from every seed at once.
func TestPushOfHeldContentSendsNoBody(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newDedupePeer(t, sender.Identity)
	const rel = "notes/held.txt"
	content := bulk(256 << 10)
	const other = "cccccccccccccccccccccccccccccccc"
	base := vvector.New().Increment(other)
	if err := os.MkdirAll(filepath.Join(recv.root, "notes"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sender.root, "notes"), 0o755); err != nil {
		t.Fatal(err)
	}
	var once gosync.Once
	recv.afterManifest = func() { once.Do(func() { recv.putFile(t, rel, content, base, other) }) }
	// The sender holds a descendant version of the same content.
	newer := base.Increment(sender.Identity.PeerID)
	sender.putFile(t, rel, content, newer, sender.Identity.PeerID)

	var seen PeerProgress
	sender.OnPeerDone = func(string) { seen = sender.Progress()[0] }
	st, err := sender.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.SkippedHeld != 1 || st.Pushed != 0 || st.SkippedReceiving != 0 || len(st.Errors) != 0 {
		t.Fatalf("stats %+v; want the push skipped as held", st)
	}
	if got := recv.ln.total(); got > 32<<10 {
		t.Fatalf("the receiver read %d bytes for a put of %d bytes it held", got, len(content))
	}
	if recv.Received() != 0 {
		t.Fatalf("received %d puts in full", recv.Received())
	}
	ver, writer := recv.docVersion(t, rel)
	if vvector.Compare(ver, newer) != vvector.Equal || writer != sender.Identity.PeerID {
		t.Fatalf("receiver records %s by %s; want the pushed version %s", ver, identity.Short(writer), newer)
	}
	r := seen.Roots[0]
	if r.SkippedHeld != 1 || r.Done != 1 || r.Failed != 0 || r.Pushed != 0 || r.SkippedPushBytes != int64(len(content)) || seen.SkippedPushBytes != int64(len(content)) || seen.PushedBytes != 0 {
		t.Fatalf("progress = %+v", seen)
	}
	if n := recv.claimsInFlight(); n != 0 {
		t.Fatalf("%d claims left on the receiver", n)
	}

	// Settled: the next round plans nothing to push.
	recv.afterManifest = nil
	st, err = sender.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Pushed+st.SkippedHeld+st.SkippedReceiving != 0 || recv.requests("/v1/put") != 1 {
		t.Fatalf("second round %+v, %d puts in all", st, recv.requests("/v1/put"))
	}

	// The control: content the receiver lacks is read in full, so the
	// counting above measures what it claims to.
	sender.putFile(t, "fresh.txt", content, vvector.New().Increment(sender.Identity.PeerID), sender.Identity.PeerID)
	before := recv.ln.total()
	st, err = sender.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Pushed != 1 || recv.ln.total()-before < int64(len(content)) {
		t.Fatalf("fresh push: %+v, read %d bytes", st, recv.ln.total()-before)
	}
}

// Concurrent versions of the same content are merged without it, as
// adoptIdentical does when the bytes are received.
func TestPutOfHeldContentMergesConcurrentVersion(t *testing.T) {
	sender := testIdentity(t)
	recv := newDedupePeer(t, sender)
	content := "same bytes, two histories\n"
	const a, b = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	mine := vvector.New().Increment(a)
	recv.putFile(t, "f.txt", content, mine, a)
	theirs := vvector.New().Increment(b)
	q := putQuery(recv.root, "f.txt", fswatch.HashBytes([]byte(content)), int64(len(content)), b)
	q.Set("version", theirs.String())
	code, reply := rawPut(t, newClient(t), recv.srv.URL, sender, q, strings.NewReader(content), true)
	if code != http.StatusOK || reply["result"] != "noop" || reply["held"] != true {
		t.Fatalf("reply %d %v", code, reply)
	}
	ver, _ := recv.docVersion(t, "f.txt")
	if vvector.Compare(ver, vvector.MergeMax(mine, theirs)) != vvector.Equal {
		t.Fatalf("version %s; want the merge of %s and %s", ver, mine, theirs)
	}
	if recv.Received() != 0 {
		t.Fatal("the content was received")
	}
}

// Two peers push the same content at once: the receiver reads one body in
// full, and answers the other as in progress before reading any of it. The
// second pusher counts it as skipped_receiving, not failed.
func TestConcurrentPutsOfSameContentReadOneBody(t *testing.T) {
	first := testIdentity(t)
	second := newEngineEnv(t)
	recv := newDedupePeer(t, first, second.Identity)
	content := bulk(512 << 10)
	hash := fswatch.HashBytes([]byte(content))
	writer := "dddddddddddddddddddddddddddddddd"
	ver := vvector.New().Increment(writer)
	q := putQuery(recv.root, "shared.bin", hash, int64(len(content)), writer)
	q.Set("version", ver.String())

	// The first pusher sends half the body and holds the rest back.
	pr, pw := io.Pipe()
	proceed := make(chan struct{})
	go func() {
		_, _ = pw.Write([]byte(content[:len(content)/2]))
		<-proceed
		_, _ = pw.Write([]byte(content[len(content)/2:]))
		_ = pw.Close()
	}()
	type answer struct {
		code  int
		reply map[string]any
	}
	done := make(chan answer, 1)
	go func() {
		code, reply := rawPut(t, newClient(t), recv.srv.URL, first, q, pr, true)
		done <- answer{code, reply}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for recv.claimsInFlight() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the first put never started")
		}
		time.Sleep(time.Millisecond)
	}

	// A pull planned meanwhile skips it too: puts and pulls share claims.
	if _, ok := recv.claimPull(recv.root, "shared.bin", hash, "eeee"); ok {
		t.Fatal("a pull claimed content a put is bringing in")
	}

	// The second pusher, a real engine, pushes the same content now.
	if err := os.WriteFile(filepath.Join(second.root, "shared.bin"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := second.record(second.root, "shared.bin", hash, int64(len(content)), ver, writer, false); err != nil {
		t.Fatal(err)
	}
	st, err := second.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.SkippedReceiving != 1 || st.Pushed != 0 || len(st.Errors) != 0 {
		t.Fatalf("second pusher's stats %+v; want the push skipped as receiving", st)
	}

	close(proceed)
	got := <-done
	if got.code != http.StatusOK || got.reply["result"] != "applied" {
		t.Fatalf("first put answered %d %v", got.code, got.reply)
	}
	recv.assertHolds(t, map[string]string{"shared.bin": content})
	full := 0
	for _, n := range recv.ln.perConn() {
		switch {
		case n >= int64(len(content)):
			full++
		case n > 32<<10:
			t.Fatalf("a connection carried %d bytes of a %d-byte body", n, len(content))
		}
	}
	if full != 1 || recv.Received() != 1 {
		t.Fatalf("%d bodies read in full, %d puts received; want exactly one", full, recv.Received())
	}
	if n := recv.claimsInFlight(); n != 0 {
		t.Fatalf("%d claims left on the receiver", n)
	}

	// Next round the second pusher finds the content there: nothing to do.
	st, err = second.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Pushed+st.SkippedHeld+st.SkippedReceiving != 0 || len(st.Errors) != 0 {
		t.Fatalf("next round %+v", st)
	}
}

// A put of content the receiver's own round is pulling from another peer
// (a LOC-77 claim) is answered as in progress, without its body; once the
// pull is over the push goes through as usual.
func TestPutWhilePullingSameContentIsPending(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newDedupePeer(t, sender.Identity)
	content := bulk(128 << 10)
	hash := fswatch.HashBytes([]byte(content))
	sender.putFile(t, "pulled.txt", content, vvector.New().Increment(sender.Identity.PeerID), sender.Identity.PeerID)

	release, ok := recv.claimPull(recv.root, "pulled.txt", hash, "aaaa")
	if !ok {
		t.Fatal("claim refused")
	}
	st, err := sender.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.SkippedReceiving != 1 || st.Pushed != 0 || len(st.Errors) != 0 {
		t.Fatalf("stats %+v; want the push skipped as receiving", st)
	}
	if got := recv.ln.total(); got > 32<<10 {
		t.Fatalf("the receiver read %d bytes of a put it was pulling", got)
	}
	if _, err := recv.Store.GetDoc(recv.root, "pulled.txt"); err == nil {
		t.Fatal("a pending put applied something")
	}

	release()
	st, err = sender.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Pushed != 1 || st.SkippedReceiving != 0 {
		t.Fatalf("after the pull let go: %+v", st)
	}
	recv.assertHolds(t, map[string]string{"pulled.txt": content})
	if n := recv.claimsInFlight(); n != 0 {
		t.Fatalf("%d claims left on the receiver", n)
	}
}

// A large push asks /v1/put/status first; naming the path there, it learns
// the receiver is getting the content from elsewhere and sends no put at
// all, or that the receiver holds it and sends the put without a body.
func TestPutStatusReportsHeldAndPending(t *testing.T) {
	sender := newEngineEnv(t)
	sender.MaxFile = 8 << 20
	recv := newDedupePeer(t, sender.Identity)
	recv.Partials = &PartialStore{Dir: filepath.Join(t.TempDir(), "partials")}
	big := bulk(ResumableMinSize + 4096)
	hash := fswatch.HashBytes([]byte(big))
	writer := sender.Identity.PeerID
	ver := vvector.New().Increment(writer)
	sender.putFile(t, "big.bin", big, ver, writer)
	entry := sender.manifestEntry(t, "big.bin")

	release, _ := recv.claimPut(recv.root, "big.bin", hash, "aaaa")
	res, err := sender.push(context.Background(), recv.srv.URL, sender.root, "big.bin", &entry)
	if err != nil || !res.Pending || res.Held {
		t.Fatalf("push while another put holds the content: %+v, %v", res, err)
	}
	if n := recv.requests("/v1/put"); n != 0 {
		t.Fatalf("%d puts sent for content the receiver reported pending", n)
	}
	release()

	recv.putFile(t, "big.bin", big, vvector.New().Increment("cccccccccccccccccccccccccccccccc"), "cccccccccccccccccccccccccccccccc")
	before := recv.ln.total()
	res, err = sender.push(context.Background(), recv.srv.URL, sender.root, "big.bin", &entry)
	if err != nil || !res.Held || res.Result != ResultNoop {
		// The two versions are concurrent: merged, no content needed.
		t.Fatalf("push of held content: %+v, %v", res, err)
	}
	if got := recv.ln.total() - before; got > 32<<10 {
		t.Fatalf("the receiver read %d bytes for a held %d-byte object", got, len(big))
	}
	if n := recv.claimsInFlight(); n != 0 {
		t.Fatalf("%d claims left on the receiver", n)
	}
}

// Compatibility: a pusher of an earlier build sends small puts without
// "Expect: 100-continue", the body along with the request. The receiver
// reads and discards that body before it answers early, so the pusher gets
// a complete reply it understands: applied for held content (it counts it
// pushed, as it would have), noop for content in progress.
func TestEarlyReplyToPusherWithoutExpect(t *testing.T) {
	sender := testIdentity(t)
	recv := newDedupePeer(t, sender)
	content := bulk(300 << 10)
	hash := fswatch.HashBytes([]byte(content))
	const other = "cccccccccccccccccccccccccccccccc"
	recv.putFile(t, "old.txt", content, vvector.New().Increment(other), other)
	newer := vvector.New().Increment(other).Increment(sender.PeerID)
	q := putQuery(recv.root, "old.txt", hash, int64(len(content)), sender.PeerID)
	q.Set("version", newer.String())

	client := newClient(t)
	code, reply := rawPut(t, client, recv.srv.URL, sender, q, strings.NewReader(content), false)
	var res applyResponse
	raw, _ := json.Marshal(reply)
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeApplyResponse(raw, "old.txt"); code != http.StatusOK || err != nil || res.Result != ResultApplied || !res.Held {
		t.Fatalf("reply %d %v (%v)", code, reply, err)
	}
	if got := recv.ln.total(); got < int64(len(content)) {
		t.Fatalf("the receiver read %d bytes; an old pusher's body must be read off the connection", got)
	}
	if ver, _ := recv.docVersion(t, "old.txt"); vvector.Compare(ver, newer) != vvector.Equal {
		t.Fatalf("version %s, want %s", ver, newer)
	}
	if recv.Received() != 0 {
		t.Fatal("the body was staged")
	}

	// In progress: a noop, which an earlier build counts as such and
	// plans again next round.
	q2 := putQuery(recv.root, "new.txt", hash, int64(len(content)), sender.PeerID)
	release, _ := recv.claimPull(recv.root, "new.txt", hash, "aaaa")
	code, reply = rawPut(t, client, recv.srv.URL, sender, q2, strings.NewReader(content), false)
	release()
	if code != http.StatusOK || reply["result"] != "noop" || reply["pending"] != true {
		t.Fatalf("pending reply %d %v", code, reply)
	}
	// The connection is still good for the next request.
	code, reply = rawPut(t, client, recv.srv.URL, sender, q2, strings.NewReader(content), false)
	if code != http.StatusOK || reply["result"] != "applied" || reply["held"] != nil {
		t.Fatalf("put after the claim was released: %d %v", code, reply)
	}
	if n := recv.claimsInFlight(); n != 0 {
		t.Fatalf("%d claims left on the receiver", n)
	}
}

// A put whose content differs from what the receiver holds is a real
// version, received and applied as ever: the early reply is for identical
// content only, and a conflict is still detected and reported.
func TestPutOfDifferentContentIsReceived(t *testing.T) {
	sender := testIdentity(t)
	recv := newDedupePeer(t, sender)
	const a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	recv.putFile(t, "f.txt", "local text\n", vvector.New().Increment(a), a)
	theirs := "their text\n"
	q := putQuery(recv.root, "f.txt", fswatch.HashBytes([]byte(theirs)), int64(len(theirs)), sender.PeerID)
	code, reply := rawPut(t, newClient(t), recv.srv.URL, sender, q, strings.NewReader(theirs), true)
	if code != http.StatusOK || reply["result"] != "conflicted" || reply["held"] != nil || reply["conflict"] == nil {
		t.Fatalf("reply %d %v", code, reply)
	}
	if recv.Received() != 1 {
		t.Fatalf("received %d", recv.Received())
	}
	// An unrecorded local edit is not held content either.
	same := "local text\n"
	if err := os.WriteFile(filepath.Join(recv.root, "g.txt"), []byte("edited, not yet recorded\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := recv.record(recv.root, "g.txt", fswatch.HashBytes([]byte(same)), int64(len(same)), vvector.New().Increment(a), a, false); err != nil {
		t.Fatal(err)
	}
	q = putQuery(recv.root, "g.txt", fswatch.HashBytes([]byte(same)), int64(len(same)), sender.PeerID)
	q.Set("version", vvector.New().Increment(a).Increment(sender.PeerID).String())
	code, reply = rawPut(t, newClient(t), recv.srv.URL, sender, q, strings.NewReader(same), true)
	if code != http.StatusOK || reply["held"] != nil || reply["result"] != "conflicted" {
		t.Fatalf("put over an unrecorded edit: %d %v", code, reply)
	}
}

// The early reply comes after authentication and authorization, like any
// answer to a put: a stranger learns nothing about what the peer holds.
func TestEarlyReplyNeedsAnAuthorizedPeer(t *testing.T) {
	member, stranger := testIdentity(t), testIdentity(t)
	recv := newDedupePeer(t, member)
	content := "held content\n"
	const a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	recv.putFile(t, "f.txt", content, vvector.New().Increment(a), a)
	q := putQuery(recv.root, "f.txt", fswatch.HashBytes([]byte(content)), int64(len(content)), stranger.PeerID)
	q.Set("version", vvector.New().Increment(a).Increment(stranger.PeerID).String())
	if code, reply := rawPut(t, newClient(t), recv.srv.URL, stranger, q, strings.NewReader(content), true); code != http.StatusForbidden || reply["held"] != nil {
		t.Fatalf("stranger's put: %d %v", code, reply)
	}
	release, _ := recv.claimPull(recv.root, "f2.txt", fswatch.HashBytes([]byte(content)), "aaaa")
	defer release()
	q.Set("relpath", "f2.txt")
	if code, reply := rawPut(t, newClient(t), recv.srv.URL, stranger, q, strings.NewReader(content), true); code != http.StatusForbidden || reply["pending"] != nil {
		t.Fatalf("stranger's put of pending content: %d %v", code, reply)
	}
	if ver, _ := recv.docVersion(t, "f.txt"); vvector.Compare(ver, vvector.New().Increment(a)) != vvector.Equal {
		t.Fatalf("a stranger's put changed the version to %s", ver)
	}
}

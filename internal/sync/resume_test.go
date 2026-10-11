package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	gosync "sync"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/vvector"
)

// resumePeer is a receiving engine with a partial store behind a real peer
// server whose put requests are observed: how many body bytes each one
// carried and with which offset.
type resumePeer struct {
	*engineEnv
	srv *httptest.Server

	mu       gosync.Mutex
	statuses int
	puts     []observedPut
}

type observedPut struct {
	offset string
	bytes  int64
}

func newResumePeer(t *testing.T, sender *identity.Identity, maxFile int64) *resumePeer {
	t.Helper()
	e := newEngineEnv(t)
	e.MaxFile = maxFile
	e.Partials = &PartialStore{Dir: filepath.Join(t.TempDir(), "partials")}
	p := &resumePeer{engineEnv: e}
	h := (&Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, AuthorizedPeers: []string{sender.PeerID}}).Handler()
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/put":
			body := &countingReader{r: r.Body}
			r.Body = io.NopCloser(body)
			h.ServeHTTP(w, r)
			p.mu.Lock()
			p.puts = append(p.puts, observedPut{offset: r.URL.Query().Get("offset"), bytes: body.n})
			p.mu.Unlock()
			return
		case "/v1/put/status":
			p.mu.Lock()
			p.statuses++
			p.mu.Unlock()
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *resumePeer) observed() (int, []observedPut) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statuses, append([]observedPut(nil), p.puts...)
}

// failingReader yields r and then fails, like a sender that goes away
// mid-upload.
type failingReader struct{ r io.Reader }

func (f *failingReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if err == io.EOF {
		return n, errors.New("sender went away")
	}
	return n, err
}

// sendPut posts a signed put of the object described by q (with offset,
// when non-zero) carrying body, returning the status and decoded reply.
func sendSignedPut(t *testing.T, base string, sender *identity.Identity, q url.Values, offset int64, body io.Reader) (int, map[string]any, error) {
	t.Helper()
	q = cloneValues(q)
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out, nil
}

func cloneValues(q url.Values) url.Values {
	out := url.Values{}
	for k, v := range q {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// partialFiles lists what the partial store directory holds.
func partialFiles(t *testing.T, ps *PartialStore) []string {
	t.Helper()
	ents, err := os.ReadDir(ps.Dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

// interruptPut sends the first cut bytes of the file at path as a put and
// then breaks the connection, returning once the receiver has finished with
// the request.
func interruptPut(t *testing.T, p *resumePeer, sender *identity.Identity, q url.Values, path string, cut int64) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, before := p.observed()
	if _, _, err := sendSignedPut(t, p.srv.URL, sender, q, 0, &failingReader{r: io.LimitReader(f, cut)}); err == nil {
		t.Fatal("an interrupted put succeeded")
	}
	// The handler finishes (and releases the partial) after the client
	// has given up; wait for it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, puts := p.observed(); len(puts) > len(before) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("receiver never finished the interrupted put")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// LOC-67: a push cut off mid-transfer is continued from what the receiver
// holds, not sent again from zero, and the installed file has the declared
// hash.
func TestPushResumesInterruptedUpload(t *testing.T) {
	sender := newEngineEnv(t)
	const size = 3 << 20
	recv := newResumePeer(t, sender.Identity, 8<<20)
	sender.MaxFile = 8 << 20
	path := filepath.Join(sender.root, "video.mov")
	hash := writePattern(t, path, size)
	ver := vvector.New().Increment(sender.Identity.PeerID)
	if err := sender.record(sender.root, "video.mov", hash, size, ver, sender.Identity.PeerID, false); err != nil {
		t.Fatal(err)
	}
	entry := sender.manifestEntry(t, "video.mov")
	q := putQuery(sender.root, "video.mov", hash, size, sender.Identity.PeerID)
	q.Set("version", entry.Version)

	interruptPut(t, recv, sender.Identity, q, path, 1200<<10)
	offset, err := recv.Partials.Offset(hash)
	if err != nil || offset <= 0 || offset > 1200<<10 {
		t.Fatalf("receiver holds %d bytes of the interrupted put (%v), want 1..%d", offset, err, 1200<<10)
	}
	if _, err := os.Stat(filepath.Join(recv.root, "video.mov")); !os.IsNotExist(err) {
		t.Fatalf("an interrupted put installed the file: %v", err)
	}
	assertNoTempFiles(t, recv.root)

	res, err := sender.Push(context.Background(), recv.srv.URL, sender.root, "video.mov", &entry)
	if err != nil || res != ResultApplied {
		t.Fatalf("resumed push: %v, %v", res, err)
	}
	statuses, puts := recv.observed()
	if statuses != 1 || len(puts) != 2 {
		t.Fatalf("%d status queries and %d puts, want 1 and 2", statuses, len(puts))
	}
	if got := puts[1]; got.offset != strconv.FormatInt(offset, 10) || got.bytes != size-offset {
		t.Fatalf("continuation carried offset %q and %d bytes, want %d and %d", got.offset, got.bytes, offset, size-offset)
	}
	if got, _, _ := fswatch.HashFile(filepath.Join(recv.root, "video.mov")); got != hash {
		t.Fatalf("receiver installed %s, want %s", got, hash)
	}
	if doc, err := recv.Store.GetDoc(recv.root, "video.mov"); err != nil || doc.Hash != hash || doc.Size != size {
		t.Fatalf("receiver recorded %+v (%v)", doc, err)
	}
	if left := partialFiles(t, recv.Partials); len(left) != 0 {
		t.Fatalf("partials left after install: %v", left)
	}
	assertNoTempFiles(t, recv.root)
}

// A large push to a fresh receiver goes through the partial store whole and
// leaves nothing behind in it.
func TestPushLargeFileWithoutPartial(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newResumePeer(t, sender.Identity, 8<<20)
	const size = 2 << 20
	hash := writePattern(t, filepath.Join(sender.root, "a.bin"), size)
	if err := sender.record(sender.root, "a.bin", hash, size, vvector.New().Increment(sender.Identity.PeerID), sender.Identity.PeerID, false); err != nil {
		t.Fatal(err)
	}
	entry := sender.manifestEntry(t, "a.bin")
	if res, err := sender.Push(context.Background(), recv.srv.URL, sender.root, "a.bin", &entry); err != nil || res != ResultApplied {
		t.Fatalf("push: %v, %v", res, err)
	}
	if _, puts := recv.observed(); len(puts) != 1 || puts[0].offset != "" || puts[0].bytes != size {
		t.Fatalf("puts %+v, want one whole put", puts)
	}
	if got, _, _ := fswatch.HashFile(filepath.Join(recv.root, "a.bin")); got != hash {
		t.Fatalf("receiver installed %s, want %s", got, hash)
	}
	if left := partialFiles(t, recv.Partials); len(left) != 0 {
		t.Fatalf("partials left after install: %v", left)
	}
}

// A continuation whose bytes do not complete the declared object is
// rejected once the object is whole, and the partial is discarded so the
// next attempt starts clean.
func TestPutTamperedContinuationIsRejected(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newResumePeer(t, sender.Identity, 8<<20)
	const size = 2 << 20
	path := filepath.Join(sender.root, "a.bin")
	hash := writePattern(t, path, size)
	q := putQuery(recv.root, "a.bin", hash, size, sender.Identity.PeerID)

	interruptPut(t, recv, sender.Identity, q, path, 1<<20)
	offset, err := recv.Partials.Offset(hash)
	if err != nil || offset <= 0 {
		t.Fatalf("no partial after the interrupted put: %d, %v", offset, err)
	}
	tampered := bytes.Repeat([]byte{'x'}, int(size-offset))
	code, out, err := sendSignedPut(t, recv.srv.URL, sender.Identity, q, offset, bytes.NewReader(tampered))
	if err != nil || code != http.StatusOK || out["result"] != string(ResultRejected) {
		t.Fatalf("tampered continuation: %d %v %v, want a rejected apply", code, out, err)
	}
	if n, _ := recv.Partials.Offset(hash); n != 0 {
		t.Fatalf("receiver kept %d bytes of a tampered object", n)
	}
	if _, err := os.Stat(filepath.Join(recv.root, "a.bin")); !os.IsNotExist(err) {
		t.Fatalf("a tampered object was installed: %v", err)
	}
	assertNoTempFiles(t, recv.root)
	assertNoRecords(t, recv.engineEnv)

	// A continuation that does not start where the partial ends is
	// refused outright (409), and the sender then starts from zero.
	interruptPut(t, recv, sender.Identity, q, path, 1<<20)
	offset, _ = recv.Partials.Offset(hash)
	if code, _, err := sendSignedPut(t, recv.srv.URL, sender.Identity, q, offset-1, strings.NewReader("x")); err != nil || code != http.StatusConflict {
		t.Fatalf("misaligned continuation: %d %v, want 409", code, err)
	}
}

// upload_max_bytes bounds the whole object, however many continuations it
// arrives in.
func TestPutSizeCapAcrossContinuations(t *testing.T) {
	sender := newEngineEnv(t)
	const max = 3 << 20
	recv := newResumePeer(t, sender.Identity, max)
	path := filepath.Join(sender.root, "a.bin")
	hash := writePattern(t, path, max+1)
	q := putQuery(recv.root, "a.bin", hash, max+1, sender.Identity.PeerID)
	if code, _, err := sendSignedPut(t, recv.srv.URL, sender.Identity, q, 0, strings.NewReader("x")); err != nil || code != http.StatusRequestEntityTooLarge {
		t.Fatalf("declared size over the cap: %d %v, want 413", code, err)
	}
	// A sender that understates the size, then leaves it out, is bounded
	// by the cap on the object as a whole.
	q.Set("size", strconv.Itoa(2<<20))
	interruptPut(t, recv, sender.Identity, q, path, 2<<20-1)
	q.Del("size")
	offset, _ := recv.Partials.Offset(hash)
	if offset <= 0 {
		t.Fatal("no partial after the interrupted put")
	}
	if code, _, err := sendSignedPut(t, recv.srv.URL, sender.Identity, q, max+1, strings.NewReader("x")); err != nil || code != http.StatusRequestEntityTooLarge {
		t.Fatalf("offset over the cap: %d %v, want 413", code, err)
	}
	// A continuation whose Content-Length takes the object over the cap is
	// refused before a byte of it is read.
	body := &countingReader{r: io.LimitReader(&patternReader{}, max)}
	cq := cloneValues(q)
	cq.Set("offset", strconv.FormatInt(offset, 10))
	req := streamedPut(t, sender.Identity, cq, body)
	req.ContentLength = max - offset + 1
	rec := httptest.NewRecorder()
	recv.srv.Config.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || body.n != 0 {
		t.Fatalf("continuation over the cap: %d after reading %d bytes, want 413 before reading", rec.Code, body.n)
	}
	// Streamed without a length: cut off one byte past the cap, and the
	// partial is discarded.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if code, _, err := sendSignedPut(t, recv.srv.URL, sender.Identity, q, offset, struct{ io.Reader }{f}); err != nil || code != http.StatusRequestEntityTooLarge {
		t.Fatalf("streamed continuation over the cap: %d %v, want 413", code, err)
	}
	if n, _ := recv.Partials.Offset(hash); n != 0 {
		t.Fatalf("receiver kept %d bytes of an oversized object", n)
	}
	if _, err := os.Stat(filepath.Join(recv.root, "a.bin")); !os.IsNotExist(err) {
		t.Fatalf("an oversized object was installed: %v", err)
	}
}

// A peer that predates resumable puts answers the status query 404 and gets
// the plain, whole put it has always understood.
func TestPushFallsBackToWholePutForOldPeer(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newEngineEnv(t)
	recv.MaxFile = 8 << 20
	h := (&Server{Engine: recv.Engine, Identity: recv.Identity, Roots: []string{recv.root}, AuthorizedPeers: []string{sender.Identity.PeerID}}).Handler()
	var mu gosync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+" offset="+r.URL.Query().Get("offset"))
		mu.Unlock()
		if r.URL.Path != "/v1/put" && r.URL.Path != "/v1/manifest" {
			http.NotFound(w, r) // the old route table
			return
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	const size = 2 << 20
	hash := writePattern(t, filepath.Join(sender.root, "a.bin"), size)
	if err := sender.record(sender.root, "a.bin", hash, size, vvector.New().Increment(sender.Identity.PeerID), sender.Identity.PeerID, false); err != nil {
		t.Fatal(err)
	}
	entry := sender.manifestEntry(t, "a.bin")
	if res, err := sender.Push(context.Background(), srv.URL, sender.root, "a.bin", &entry); err != nil || res != ResultApplied {
		t.Fatalf("push to an old peer: %v, %v", res, err)
	}
	want := []string{"GET /v1/put/status offset=", "POST /v1/put offset="}
	if strings.Join(seen, "|") != strings.Join(want, "|") {
		t.Fatalf("requests %v, want %v", seen, want)
	}
	if got, _, _ := fswatch.HashFile(filepath.Join(recv.root, "a.bin")); got != hash {
		t.Fatalf("old peer installed %s, want %s", got, hash)
	}
}

// When the receiver no longer holds the partial it reported (or another
// upload of it is running), the continuation is refused and the sender
// sends the whole file instead.
func TestPushRestartsWhenContinuationRefused(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newResumePeer(t, sender.Identity, 8<<20)
	const size = 2 << 20
	path := filepath.Join(sender.root, "a.bin")
	hash := writePattern(t, path, size)
	if err := sender.record(sender.root, "a.bin", hash, size, vvector.New().Increment(sender.Identity.PeerID), sender.Identity.PeerID, false); err != nil {
		t.Fatal(err)
	}
	entry := sender.manifestEntry(t, "a.bin")
	q := putQuery(recv.root, "a.bin", hash, size, sender.Identity.PeerID)
	interruptPut(t, recv, sender.Identity, q, path, 1<<20)
	// Hold the object's lock, as an upload still in progress would.
	if !recv.Partials.lock(hash) {
		t.Fatal("partial still locked")
	}
	res, err := sender.Push(context.Background(), recv.srv.URL, sender.root, "a.bin", &entry)
	recv.Partials.unlock(hash)
	if err != nil || res != ResultApplied {
		t.Fatalf("push: %v, %v", res, err)
	}
	_, puts := recv.observed()
	if len(puts) != 3 || puts[1].offset == "" || puts[2].offset != "" || puts[2].bytes != size {
		t.Fatalf("puts %+v, want a refused continuation and then a whole put", puts)
	}
	if got, _, _ := fswatch.HashFile(filepath.Join(recv.root, "a.bin")); got != hash {
		t.Fatalf("receiver installed %s, want %s", got, hash)
	}
}

// Partials nobody has written to for MaxAge are removed; fresh ones, and
// one an upload is writing, are kept.
func TestSweepRemovesStalePartials(t *testing.T) {
	ps := &PartialStore{Dir: t.TempDir(), MaxAge: time.Hour}
	h := func(s string) string { return fswatch.HashBytes([]byte(s)) }
	old := time.Now().Add(-2 * time.Hour)
	files := map[string]bool{ // name -> expected to survive
		h("stale") + ".part":         false,
		h("stale-tmp") + ".1234.tmp": false,
		h("fresh") + ".part":         true,
		h("busy") + ".part":          true,
		"unrelated.txt":              true,
	}
	for name, keep := range files {
		p := filepath.Join(ps.Dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if !keep || strings.HasPrefix(name, h("busy")) || name == "unrelated.txt" {
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	ps.lock(h("busy"))
	n, err := ps.Sweep(time.Now())
	ps.unlock(h("busy"))
	if err != nil || n != 2 {
		t.Fatalf("swept %d (%v), want 2", n, err)
	}
	for name, keep := range files {
		_, err := os.Stat(filepath.Join(ps.Dir, name))
		if keep != (err == nil) {
			t.Errorf("%s: kept=%v, want %v", name, err == nil, keep)
		}
	}
	if n, err := (&PartialStore{Dir: filepath.Join(ps.Dir, "missing")}).Sweep(time.Now()); err != nil || n != 0 {
		t.Fatalf("sweep of a missing directory: %d, %v", n, err)
	}
}

// Receive bounds the object by the cap even when no size is declared, and
// keeps what arrived of a broken-off body for the continuation.
func TestPartialStoreReceive(t *testing.T) {
	ps := &PartialStore{Dir: t.TempDir()}
	content := []byte(strings.Repeat("0123456789", 1000))
	hash := fswatch.HashBytes(content)

	if _, _, err := ps.Receive(hash, 0, -1, 0, &failingReader{r: bytes.NewReader(content[:4000])}); err == nil {
		t.Fatal("a broken-off body was accepted")
	}
	if n, _ := ps.Offset(hash); n != 4000 {
		t.Fatalf("kept %d bytes, want 4000", n)
	}
	// Ends cleanly but short of the declared size: kept, incomplete.
	if _, _, err := ps.Receive(hash, 4000, int64(len(content)), 0, bytes.NewReader(content[4000:6000])); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("short continuation: %v, want ErrIncomplete", err)
	}
	if _, _, err := ps.Receive(hash, 5000, int64(len(content)), 0, bytes.NewReader(content[5000:])); !errors.Is(err, ErrPartialOffset) {
		t.Fatalf("misaligned continuation: %v, want ErrPartialOffset", err)
	}
	// While another upload holds the object, a fresh one goes to a private
	// file and a continuation is refused.
	ps.lock(hash)
	if _, _, err := ps.Receive(hash, 6000, int64(len(content)), 0, bytes.NewReader(content[6000:])); !errors.Is(err, ErrPartialOffset) {
		t.Fatalf("continuation of a busy object: %v, want ErrPartialOffset", err)
	}
	f, release, err := ps.Receive(hash, 0, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("fresh upload of a busy object: %v", err)
	}
	if got, _ := io.ReadAll(f); !bytes.Equal(got, content) {
		t.Fatal("private upload returned other content")
	}
	release()
	ps.unlock(hash)
	if n, _ := ps.Offset(hash); n != 6000 {
		t.Fatalf("the busy upload's partial changed to %d bytes", n)
	}

	f, release, err = ps.Receive(hash, 6000, int64(len(content)), 0, bytes.NewReader(content[6000:]))
	if err != nil {
		t.Fatalf("completing continuation: %v", err)
	}
	if got, _ := io.ReadAll(f); !bytes.Equal(got, content) {
		t.Fatal("completed object differs from the content")
	}
	release()
	if left := partialFiles(t, ps); len(left) != 0 {
		t.Fatalf("left after release: %v", left)
	}

	// No declared size: bounded by the cap across continuations.
	if _, _, err := ps.Receive(hash, 0, -1, 5000, &failingReader{r: bytes.NewReader(content[:3000])}); err == nil {
		t.Fatal("a broken-off body was accepted")
	}
	if _, _, err := ps.Receive(hash, 3000, -1, 5000, bytes.NewReader(content[3000:])); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("continuation past the cap: %v, want ErrTooLarge", err)
	}
	if left := partialFiles(t, ps); len(left) != 0 {
		t.Fatalf("an oversized partial was kept: %v", left)
	}
}

// Content transfers run after the actions that move nothing, smallest
// first, and otherwise in plan order.
func TestScheduleSendsSmallFilesFirst(t *testing.T) {
	ver := vvector.New().Increment("aaaa").String()
	local := []ManifestEntry{
		{RelPath: "big.key", Size: 400 << 20, Hash: "h1", Version: ver},
		{RelPath: "gone.txt", Hash: "h2", Version: ver, Deleted: true},
		{RelPath: "note.md", Size: 2 << 10, Hash: "h3", Version: ver},
		{RelPath: "video.mov", Size: 159 << 20, Hash: "h4", Version: ver},
	}
	remote := []ManifestEntry{
		{RelPath: "gone.txt", Hash: "h2", Version: vvector.New().String()},
		{RelPath: "pulled.pdf", Size: 3 << 20, Hash: "h5", Version: ver},
	}
	var got []string
	for _, a := range Schedule(Plan(local, remote)) {
		got = append(got, a.RelPath)
	}
	want := []string{"gone.txt", "note.md", "pulled.pdf", "video.mov", "big.key"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("schedule %v, want %v", got, want)
	}
}

// Once a round's context drains, the transfer in progress completes and no
// other one starts.
func TestRoundDrainFinishesCurrentTransferOnly(t *testing.T) {
	e := newEngineEnv(t)
	for i, name := range []string{"a.txt", "b.txt"} {
		e.putFile(t, name, strings.Repeat("x", i+1), vvector.New().Increment(e.Identity.PeerID), e.Identity.PeerID)
	}
	manifest := `{"peer_id":"` + remoteWriter + `","root_name":"` + RootName(e.root) + `","entries":[]}`
	started, release := make(chan struct{}), make(chan struct{})
	var mu gosync.Mutex
	var puts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/manifest" {
			_, _ = w.Write([]byte(manifest))
			return
		}
		mu.Lock()
		puts = append(puts, r.URL.Query().Get("relpath"))
		first := len(puts) == 1
		mu.Unlock()
		if first {
			close(started)
			<-release
		}
		_, _ = w.Write([]byte(`{"result":"applied"}`))
	}))
	t.Cleanup(srv.Close)

	drain := make(chan struct{})
	ctx := WithDrain(context.Background(), drain)
	done := make(chan *Stats)
	go func() {
		st, _ := e.Round(ctx, peerAt(srv))
		done <- st
	}()
	select {
	case <-started:
	case st := <-done:
		t.Fatalf("round ended without a transfer: %+v", st)
	}
	close(drain)
	close(release)
	st := <-done
	mu.Lock()
	defer mu.Unlock()
	if len(puts) != 1 || puts[0] != "a.txt" || st.Pushed != 1 {
		t.Fatalf("puts %v, stats %+v; want only the transfer in progress (a.txt) completed", puts, st)
	}
}

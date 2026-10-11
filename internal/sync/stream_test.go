package sync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/vvector"
)

// countingReader records how much of a request body a handler consumed.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// patternReader yields an endless, deterministic, non-repeating-looking byte
// stream without holding it in memory.
type patternReader struct{ i uint64 }

func (p *patternReader) Read(b []byte) (int, error) {
	for k := range b {
		p.i++
		b[k] = byte(p.i*2654435761>>13) ^ byte(p.i)
	}
	return len(b), nil
}

// writePattern writes size pattern bytes to path and returns their hash.
func writePattern(t *testing.T, path string, size int64) string {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(f, io.LimitReader(&patternReader{}, size)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	hash, _, err := fswatch.HashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

// allocatedDuring returns the bytes the whole process allocated while f ran.
func allocatedDuring(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// putQuery is the query of a put of content with the given hash and size.
func putQuery(root, rel, hash string, size int64, writer string) url.Values {
	return url.Values{
		"rootname": {RootName(root)},
		"relpath":  {rel},
		"version":  {vvector.New().Increment(writer).String()},
		"hash":     {hash},
		"size":     {strconv.FormatInt(size, 10)},
		"writer":   {writer},
	}
}

// streamedPut builds a put signed the way Push signs it: over the declared
// content hash, not over the body it carries.
func streamedPut(t *testing.T, sender *identity.Identity, q url.Values, body io.Reader) *http.Request {
	t.Helper()
	target := "/v1/put?" + q.Encode()
	declared, err := DeclaredContentHash(q.Get("hash"))
	if err != nil {
		declared = emptyBodyHash
	}
	req := httptest.NewRequest(http.MethodPost, target, body)
	for k, v := range AuthHeadersHash(sender, http.MethodPost, target, declared) {
		req.Header.Set(k, v)
	}
	return req
}

// LOC-13: the peer server used to read up to MaxFile+1 MiB of every request
// body before checking its signature, so anyone who could reach the peer
// port could make it buffer a gigabyte per connection. A put is now
// authenticated, authorized and size-checked before a byte of it is read.
func TestServerRefusesPutBeforeReadingBody(t *testing.T) {
	e := newEngineEnv(t)
	member, stranger := testIdentity(t), testIdentity(t)
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, AuthorizedPeers: []string{member.PeerID}}
	h := s.Handler()
	content := strings.Repeat("x", 4096)
	hash := fswatch.HashBytes([]byte(content))

	cases := []struct {
		name string
		req  func(body io.Reader) *http.Request
		want int
	}{
		{"unsigned", func(body io.Reader) *http.Request {
			return httptest.NewRequest(http.MethodPost, "/v1/put?"+putQuery(e.root, "a.txt", hash, 4096, stranger.PeerID).Encode(), body)
		}, http.StatusUnauthorized},
		{"signed over another hash", func(body io.Reader) *http.Request {
			target := "/v1/put?" + putQuery(e.root, "a.txt", hash, 4096, member.PeerID).Encode()
			req := httptest.NewRequest(http.MethodPost, target, body)
			other, _ := DeclaredContentHash(fswatch.HashBytes([]byte("other")))
			for k, v := range AuthHeadersHash(member, http.MethodPost, target, other) {
				req.Header.Set(k, v)
			}
			return req
		}, http.StatusUnauthorized},
		{"no declared hash", func(body io.Reader) *http.Request {
			return streamedPut(t, member, putQuery(e.root, "a.txt", "", 4096, member.PeerID), body)
		}, http.StatusUnauthorized},
		{"unauthorized peer", func(body io.Reader) *http.Request {
			return streamedPut(t, stranger, putQuery(e.root, "a.txt", hash, 4096, stranger.PeerID), body)
		}, http.StatusForbidden},
		{"declared size over the limit", func(body io.Reader) *http.Request {
			return streamedPut(t, member, putQuery(e.root, "a.txt", hash, e.MaxFile+1, member.PeerID), body)
		}, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		body := &countingReader{r: strings.NewReader(content)}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, tc.req(body))
		if rec.Code != tc.want {
			t.Errorf("%s: status %d (%s), want %d", tc.name, rec.Code, strings.TrimSpace(rec.Body.String()), tc.want)
		}
		if body.n != 0 {
			t.Errorf("%s: server read %d body bytes before refusing", tc.name, body.n)
		}
	}
	if _, err := os.Stat(filepath.Join(e.root, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("a refused put wrote the file: %v", err)
	}
	assertNoTempFiles(t, e.root)
	assertNoRecords(t, e)
}

// LOC-13: with the signature over the declared hash, the body's integrity
// rests on Install, which must refuse a body that does not hash to it (or
// that runs past the size limit) and leave neither the file nor a temporary
// file behind. A peer that still signs the body it sends is refused at once
// when that body is not the declared content.
func TestServerRejectsBodyNotMatchingDeclaredHash(t *testing.T) {
	e := newEngineEnv(t)
	sender := testIdentity(t)
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, AuthorizedPeers: []string{sender.PeerID}}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	declared := "the content the sender declared"
	q := putQuery(e.root, "a.txt", fswatch.HashBytes([]byte(declared)), int64(len(declared)), sender.PeerID)
	target := "/v1/put?" + q.Encode()

	send := func(body io.Reader, headers map[string]string) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, srv.URL+target, body)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	declaredHash, _ := DeclaredContentHash(q.Get("hash"))
	signed := AuthHeadersHash(sender, http.MethodPost, target, declaredHash)

	// Tampered: same length, different bytes.
	if code, out := send(strings.NewReader(strings.ToUpper(declared)), signed); code != http.StatusOK || out["result"] != string(ResultRejected) {
		t.Errorf("tampered body: status %d, reply %v; want a rejected apply", code, out)
	}
	// Oversized behind a small declared size: cut off at the limit.
	if code, _ := send(io.LimitReader(&patternReader{}, e.MaxFile+1), signed); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: status %d, want 413", code)
	}
	// Signed the old way, over the tampered body itself.
	tampered := []byte("not what was declared")
	if code, _ := send(strings.NewReader(string(tampered)), AuthHeaders(sender, http.MethodPost, target, tampered)); code != http.StatusUnauthorized {
		t.Errorf("body-signed tampered put: status %d, want 401", code)
	}

	if _, err := os.Stat(filepath.Join(e.root, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("a mismatching body was installed: %v", err)
	}
	assertNoTempFiles(t, e.root)
	assertNoRecords(t, e)

	// The honest body, signed either way, still lands.
	if code, out := send(strings.NewReader(declared), AuthHeaders(sender, http.MethodPost, target, []byte(declared))); code != http.StatusOK || out["result"] != string(ResultApplied) {
		t.Fatalf("honest put: status %d, reply %v", code, out)
	}
	if got, _ := os.ReadFile(filepath.Join(e.root, "a.txt")); string(got) != declared {
		t.Fatalf("honest put installed %q", got)
	}
}

// LOC-13: a transfer much larger than any buffer Locus keeps goes through
// Push, the peer server and Pull with allocations that do not grow with the
// file. Before, each hop held the whole file (and the server one more copy
// for the signature check).
func TestStreamedTransfersUseBoundedMemory(t *testing.T) {
	const size = 32 << 20
	const budget = size / 4 // generous for HTTP, sqlite and GC noise; a buffered hop alone is 32 MiB

	sender, receiver, puller := newEngineEnv(t), newEngineEnv(t), newEngineEnv(t)
	for _, e := range []*engineEnv{sender, receiver, puller} {
		e.MaxFile = 64 << 20
	}
	// The put is large enough to be received through a resumable partial
	// object, which must stream as well (LOC-67).
	receiver.Partials = &PartialStore{Dir: filepath.Join(t.TempDir(), "partials")}
	srv := httptest.NewServer((&Server{
		Engine: receiver.Engine, Identity: receiver.Identity, Roots: []string{receiver.root},
		AuthorizedPeers: []string{sender.Identity.PeerID, puller.Identity.PeerID},
	}).Handler())
	t.Cleanup(srv.Close)
	ctx := context.Background()

	hash := writePattern(t, filepath.Join(sender.root, "big.bin"), size)
	ver := vvector.New().Increment(sender.Identity.PeerID)
	if err := sender.record(sender.root, "big.bin", hash, size, ver, sender.Identity.PeerID, false); err != nil {
		t.Fatal(err)
	}
	entry := sender.manifestEntry(t, "big.bin")

	var res Result
	var err error
	if n := allocatedDuring(func() { res, err = sender.Push(ctx, srv.URL, sender.root, "big.bin", &entry) }); err != nil || res != ResultApplied {
		t.Fatalf("push: %v, %v", res, err)
	} else if t.Logf("push allocated %d bytes", n); n > budget {
		t.Errorf("push of %d bytes allocated %d bytes", size, n)
	}
	if got, _, _ := fswatch.HashFile(filepath.Join(receiver.root, "big.bin")); got != hash {
		t.Fatalf("receiver holds %s, want %s", got, hash)
	}

	remote := receiver.manifestEntry(t, "big.bin")
	if n := allocatedDuring(func() {
		res, err = puller.Pull(ctx, srv.URL, receiver.Identity.PeerID, puller.root, "big.bin", &remote)
	}); err != nil || res != ResultApplied {
		t.Fatalf("pull: %v, %v", res, err)
	} else if t.Logf("pull allocated %d bytes", n); n > budget {
		t.Errorf("pull of %d bytes allocated %d bytes", size, n)
	}
	if got, _, _ := fswatch.HashFile(filepath.Join(puller.root, "big.bin")); got != hash {
		t.Fatalf("puller holds %s, want %s", got, hash)
	}
	for _, e := range []*engineEnv{receiver, puller} {
		assertNoTempFiles(t, e.root)
	}
}

// A pull is bounded by MaxFile while streaming, and an oversized or
// corrupted download leaves nothing behind.
func TestPullRefusesOversizedOrCorruptContent(t *testing.T) {
	e := newEngineEnv(t)
	const size = 4096
	e.MaxFile = size - 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, io.LimitReader(&patternReader{}, size))
	}))
	t.Cleanup(srv.Close)
	ver := vvector.New().Increment("bbbb").String()
	remote := &ManifestEntry{RelPath: "a.bin", Size: size, Hash: fswatch.HashBytes([]byte("other")), Version: ver, LastWriter: "bbbb"}

	if _, err := e.Pull(context.Background(), srv.URL, "bbbb", e.root, "a.bin", remote); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversized pull: %v, want ErrTooLarge", err)
	}
	e.MaxFile = 1 << 20
	if _, err := e.Pull(context.Background(), srv.URL, "bbbb", e.root, "a.bin", remote); !errors.Is(err, ErrContentMismatch) {
		t.Errorf("corrupt pull: %v, want ErrContentMismatch", err)
	}
	if _, err := os.Stat(filepath.Join(e.root, "a.bin")); !os.IsNotExist(err) {
		t.Fatalf("a refused pull installed the file: %v", err)
	}
	assertNoTempFiles(t, e.root)
	assertNoRecords(t, e)
}

// InstallStream checks a declared size before the rename, and stops reading
// as soon as the content runs past it.
func TestInstallStreamChecksDeclaredSize(t *testing.T) {
	root := t.TempDir()
	body := &countingReader{r: io.LimitReader(&patternReader{}, 1<<20)}
	if _, _, err := InstallStream(root, "a.bin", body, "", 100, 1<<30); !errors.Is(err, ErrContentMismatch) {
		t.Fatalf("long body: %v, want ErrContentMismatch", err)
	}
	if body.n > 64<<10 {
		t.Errorf("read %d bytes of a body declared as 100", body.n)
	}
	if _, _, err := InstallStream(root, "a.bin", strings.NewReader("short"), "", 100, 1<<30); !errors.Is(err, ErrContentMismatch) {
		t.Fatalf("short body: %v, want ErrContentMismatch", err)
	}
	if _, err := os.Stat(filepath.Join(root, "a.bin")); !os.IsNotExist(err) {
		t.Fatalf("a mismatching body was installed: %v", err)
	}
	assertNoTempFiles(t, root)
	n, hash, err := InstallStream(root, "a.bin", strings.NewReader("exact"), "", 5, 1<<30)
	if err != nil || n != 5 || hash != fswatch.HashBytes([]byte("exact")) {
		t.Fatalf("exact body: %d %s %v", n, hash, err)
	}
}

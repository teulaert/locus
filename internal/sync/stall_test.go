package sync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/vvector"
)

// stalledPut starts a put of content (declared in full) from sender that
// delivers the first half of the body and then stops sending without
// closing. It returns a channel with the put's final status.
func stalledPut(t *testing.T, base string, sender *identity.Identity, root, rel string, content []byte) <-chan int {
	t.Helper()
	pr, pw := io.Pipe()
	t.Cleanup(func() { pw.Close() })
	q := putQuery(root, rel, fswatch.HashBytes(content), int64(len(content)), sender.PeerID)
	target := "/v1/put?" + q.Encode()
	declared, _ := DeclaredContentHash(q.Get("hash"))
	req, err := http.NewRequest(http.MethodPost, base+target, pr)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range AuthHeadersHash(sender, http.MethodPost, target, declared) {
		req.Header.Set(k, v)
	}
	status := make(chan int, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- -1
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	if _, err := pw.Write(content[:len(content)/2]); err != nil {
		t.Fatal(err)
	}
	return status
}

// LOC-69: one sender stalling mid-body on file X held X's path lock for as
// long as it stalled, so another peer's put of X and this peer's own pull of
// X waited behind it, and with them the whole round. The body is now
// received before the lock is taken: neither waits for the stalled sender,
// and the stalled put is given up after the idle timeout.
func TestStalledUploadDoesNotBlockOtherTransfersOfThePath(t *testing.T) {
	recv := newEngineEnv(t)
	recv.MaxFile = 8 << 20
	recv.Partials = &PartialStore{Dir: filepath.Join(t.TempDir(), "partials")}
	stalled, other := testIdentity(t), newEngineEnv(t)
	s := &Server{Engine: recv.Engine, Identity: recv.Identity, Roots: []string{recv.root},
		AuthorizedPeers: []string{stalled.PeerID, other.Identity.PeerID}, BodyIdleTimeout: 2 * time.Second}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	for _, size := range []int{64 << 10, 2 << 20} { // a plain put and a resumable one
		rel := "x-" + strconv.Itoa(size) + ".bin"
		content := make([]byte, size)
		_, _ = io.ReadFull(&patternReader{}, content)
		status := stalledPut(t, srv.URL, stalled, recv.root, rel, content)
		time.Sleep(100 * time.Millisecond) // let the stalled put reach the receiver

		// Another peer pushes its own version of X.
		other.putFile(t, rel, "other's version of x", vvector.New().Increment(other.Identity.PeerID), other.Identity.PeerID)
		entry := other.manifestEntry(t, rel)
		start := time.Now()
		if res, err := other.Push(context.Background(), srv.URL, other.root, rel, &entry); err != nil || res != ResultApplied {
			t.Fatalf("%s: push behind a stalled put: %v, %v", rel, res, err)
		}
		if took := time.Since(start); took > time.Second {
			t.Errorf("%s: push waited %s behind a stalled put", rel, took)
		}
		select {
		case code := <-status:
			t.Fatalf("%s: stalled put ended (%d) before the other push was checked", rel, code)
		default:
		}

		// This peer pulls a newer X from a third peer while the stall goes on.
		newer := []byte("newest x")
		third := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(newer) }))
		doc, err := recv.Store.GetDoc(recv.root, rel)
		if err != nil {
			t.Fatal(err)
		}
		remote := &ManifestEntry{RelPath: rel, Size: int64(len(newer)), Hash: fswatch.HashBytes(newer), Version: doc.Version.Increment("cccc").String(), LastWriter: "cccc"}
		start = time.Now()
		if res, err := recv.Pull(context.Background(), third.URL, "cccc", recv.root, rel, remote); err != nil || res != ResultApplied {
			t.Fatalf("%s: pull behind a stalled put: %v, %v", rel, res, err)
		}
		third.Close()
		if took := time.Since(start); took > time.Second {
			t.Errorf("%s: pull waited %s behind a stalled put", rel, took)
		}

		select {
		case code := <-status:
			if code != http.StatusRequestTimeout {
				t.Errorf("%s: stalled put answered %d, want 408", rel, code)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: stalled put never timed out", rel)
		}
		if got, _ := os.ReadFile(filepath.Join(recv.root, rel)); !bytes.Equal(got, newer) {
			t.Fatalf("%s: holds %q, want the pulled version", rel, got)
		}
	}
	// What arrived of the stalled resumable put is kept for its sender.
	big := make([]byte, 2<<20)
	_, _ = io.ReadFull(&patternReader{}, big)
	if n, _ := recv.Partials.Offset(fswatch.HashBytes(big)); n <= 0 {
		t.Errorf("stalled resumable put left %d bytes to resume from", n)
	}
	assertNoTempFiles(t, recv.root)
}

// A peer that stops sending a file mid-download is given up on after the
// transfer idle timeout, as a transport failure, and nothing is left behind.
func TestPullGivesUpOnStalledDownload(t *testing.T) {
	e := newEngineEnv(t)
	e.TransferIdleTimeout = 300 * time.Millisecond
	content := bytes.Repeat([]byte("z"), 64<<10)
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		_, _ = w.Write(content[:len(content)/2])
		w.(http.Flusher).Flush()
		<-unblock
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(unblock) })
	remote := &ManifestEntry{RelPath: "x.bin", Size: int64(len(content)), Hash: fswatch.HashBytes(content), Version: vvector.New().Increment("cccc").String(), LastWriter: "cccc"}

	start := time.Now()
	_, err := e.Pull(context.Background(), srv.URL, "cccc", e.root, "x.bin", remote)
	if !errors.Is(err, errTransferStalled) || !IsTransport(err) {
		t.Fatalf("stalled pull: %v, want a stalled transport error", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("stalled pull took %s with a 300ms idle timeout", took)
	}
	if _, err := os.Stat(filepath.Join(e.root, "x.bin")); !os.IsNotExist(err) {
		t.Fatalf("a stalled pull installed the file: %v", err)
	}
	assertNoTempFiles(t, e.root)
	// The path is free: an apply of it does not wait.
	done := make(chan struct{})
	go func() {
		e.putFile(t, "x.bin", "local", vvector.New().Increment(e.Identity.PeerID), e.Identity.PeerID)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("path still locked after a stalled pull")
	}
}

// A received file is staged outside the root and moved into place, and a
// peer cannot create a file named like a Locus temporary.
func TestReceivedFilesAreStagedOutsideTheRoot(t *testing.T) {
	e := newEngineEnv(t)
	e.Partials = &PartialStore{Dir: filepath.Join(t.TempDir(), "partials")}
	content := []byte("staged content")
	st, release, err := e.Partials.Stage(fswatch.HashBytes(content), int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if filepath.Dir(st.path) != e.Partials.Dir {
		t.Fatalf("staged at %s, want under %s", st.path, e.Partials.Dir)
	}
	meta := Meta{Hash: st.Hash, Size: st.Size, Version: vvector.New().Increment("cccc"), LastWriter: "cccc"}
	if res, err := e.Apply(e.root, "a.txt", meta, st, "cccc"); err != nil || res != ResultApplied {
		t.Fatalf("apply staged: %v, %v", res, err)
	}
	if !st.moved {
		t.Error("staged content on the same filesystem was copied, not renamed")
	}
	if got, _ := os.ReadFile(filepath.Join(e.root, "a.txt")); !bytes.Equal(got, content) {
		t.Fatalf("installed %q", got)
	}
	if fi, err := os.Stat(filepath.Join(e.root, "a.txt")); err != nil || fi.Mode().Perm() != 0o644 {
		t.Fatalf("installed file mode %v (%v), want 0644", fi.Mode(), err)
	}
	release()
	if _, err := os.Stat(filepath.Join(e.root, "a.txt")); err != nil {
		t.Fatalf("releasing a moved staged file removed the document: %v", err)
	}
	if left := partialFiles(t, e.Partials); len(left) != 0 {
		t.Fatalf("staging left %v", left)
	}

	bad := fswatch.TempPrefix + "planted"
	if res, err := e.Apply(e.root, "dir/"+bad, meta, bytes.NewReader(content), "cccc"); err == nil || res != ResultRejected {
		t.Fatalf("apply of a temporary file name: %v, %v; want rejected", res, err)
	}
}

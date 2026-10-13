package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	gosync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// seedPeer is a fake peer serving the same files under one root, counting
// the file requests it answers. fail, when set, decides per path whether a
// file request is refused with a 500 instead.
type seedPeer struct {
	srv    *httptest.Server
	served atomic.Int64
	fail   func(rel string) bool
	// beforeManifest, when set, runs before the manifest is answered.
	beforeManifest func()
}

func newSeedPeer(t *testing.T, rootName string, files map[string]string) *seedPeer {
	t.Helper()
	const writer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	var entries []ManifestEntry
	for rel, body := range files {
		entries = append(entries, ManifestEntry{
			RelPath:    rel,
			Size:       int64(len(body)),
			Hash:       fswatch.HashBytes([]byte(body)),
			Version:    vvector.New().Increment(writer).String(),
			LastWriter: writer,
		})
	}
	sp := &seedPeer{}
	sp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("rootname") != rootName {
			http.Error(w, "unknown root", http.StatusNotFound)
			return
		}
		switch r.URL.Path {
		case "/v1/manifest":
			if sp.beforeManifest != nil {
				sp.beforeManifest()
			}
			_ = json.NewEncoder(w).Encode(Manifest{PeerID: writer, RootName: rootName, Entries: entries})
		case "/v1/file":
			rel := r.URL.Query().Get("relpath")
			if sp.fail != nil && sp.fail(rel) {
				http.Error(w, "broken", http.StatusInternalServerError)
				return
			}
			sp.served.Add(1)
			// A file takes a moment to send, as it does over a real link.
			time.Sleep(2 * time.Millisecond)
			_, _ = w.Write([]byte(files[rel]))
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(sp.srv.Close)
	return sp
}

func seedFiles(n int) map[string]string {
	files := make(map[string]string, n)
	for i := 0; i < n; i++ {
		files[fmt.Sprintf("dir%d/file%03d.txt", i%7, i)] = fmt.Sprintf("content of file %d\n", i)
	}
	return files
}

func (e *engineEnv) assertHolds(t *testing.T, files map[string]string) {
	t.Helper()
	for rel, body := range files {
		got, err := os.ReadFile(filepath.Join(e.root, filepath.FromSlash(rel)))
		if err != nil || string(got) != body {
			t.Fatalf("%s: %q, %v; want %q", rel, got, err, body)
		}
	}
}

// LOC-77: a new peer used to pull every file from every online peer at
// once, since each peer's round planned against the same local manifest.
// A pull is now claimed before it starts, and a round that finds the file
// claimed by another peer's round skips it: two seeds holding the same N
// files serve about N files between them, not 2N.
func TestConcurrentRoundsPullEachFileOnce(t *testing.T) {
	e := newEngineEnv(t)
	const n = 120
	files := seedFiles(n)
	a := newSeedPeer(t, RootName(e.root), files)
	b := newSeedPeer(t, RootName(e.root), files)

	st, err := e.Round(context.Background(), []*peer.Entry{entryAt("aaaa", a.srv), entryAt("bbbb", b.srv)})
	if err != nil {
		t.Fatal(err)
	}
	e.assertHolds(t, files)
	served := a.served.Load() + b.served.Load()
	t.Logf("served: a=%d b=%d total=%d for %d files; stats %+v", a.served.Load(), b.served.Load(), served, n, st)
	if served > n+2 {
		t.Fatalf("seeds served %d files for %d", served, n)
	}
	if len(st.Errors) != 0 {
		t.Fatalf("errors: %v", st.Errors)
	}
	if st.Pulled+st.SkippedInFlight < n || st.Pulled > n {
		t.Fatalf("pulled %d, skipped %d; want %d pulled in all", st.Pulled, st.SkippedInFlight, n)
	}
	if left := e.claimsInFlight(); left != 0 {
		t.Fatalf("%d claims left after the round", left)
	}

	// The next round finds everything in place and moves no content.
	before := a.served.Load() + b.served.Load()
	if _, err := e.Round(context.Background(), []*peer.Entry{entryAt("aaaa", a.srv), entryAt("bbbb", b.srv)}); err != nil {
		t.Fatal(err)
	}
	if after := a.served.Load() + b.served.Load(); after != before {
		t.Fatalf("a settled round served %d more files", after-before)
	}
}

// A pull that fails releases its claim, so another peer's round can take
// the file over: here the first peer fails every file, and the second
// peer's round, which plans only once the first has failed and let go of
// its claims, fetches all of them.
func TestFailedPullReleasesClaim(t *testing.T) {
	e := newEngineEnv(t)
	files := seedFiles(10)
	a := newSeedPeer(t, RootName(e.root), files)
	b := newSeedPeer(t, RootName(e.root), files)
	var failures atomic.Int64
	a.fail = func(string) bool { failures.Add(1); return true }
	// b answers its manifest once a has failed every file and every claim
	// is released again.
	var once gosync.Once
	b.beforeManifest = func() {
		once.Do(func() {
			deadline := time.Now().Add(10 * time.Second)
			for failures.Load() < int64(len(files)) || e.claimsInFlight() != 0 {
				if time.Now().After(deadline) {
					t.Error("the first peer's pulls never all failed")
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	}

	st, err := e.Round(context.Background(), []*peer.Entry{entryAt("aaaa", a.srv), entryAt("bbbb", b.srv)})
	if err != nil {
		t.Fatal(err)
	}
	e.assertHolds(t, files)
	if b.served.Load() != int64(len(files)) || st.Pulled != len(files) || len(st.Errors) != len(files) {
		t.Fatalf("b served %d, stats %+v; want every file from b and one error per file from a", b.served.Load(), st)
	}
	if left := e.claimsInFlight(); left != 0 {
		t.Fatalf("%d claims left after the round", left)
	}
}

// A pull another round holds is skipped, counted as such and reported in
// progress, and the file is fetched once that round lets go of it.
func TestPullHeldByAnotherRoundIsSkipped(t *testing.T) {
	e := newEngineEnv(t)
	files := map[string]string{"one.txt": "only file\n"}
	b := newSeedPeer(t, RootName(e.root), files)
	entry := ManifestEntry{RelPath: "one.txt", Hash: fswatch.HashBytes([]byte(files["one.txt"]))}
	release, ok := e.claimPull(e.root, entry.RelPath, entry.Hash, "aaaa")
	if !ok {
		t.Fatal("first claim refused")
	}
	if _, again := e.claimPull(e.root, entry.RelPath, entry.Hash, "cccc"); again {
		t.Fatal("a second claim on the same file was granted")
	}

	var seen PeerProgress
	e.OnPeerDone = func(string) { seen = e.Progress()[0] }
	st, err := e.Round(context.Background(), []*peer.Entry{entryAt("bbbb", b.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.SkippedInFlight != 1 || st.Pulled != 0 || len(st.Errors) != 0 || b.served.Load() != 0 {
		t.Fatalf("stats %+v, served %d; want the pull skipped", st, b.served.Load())
	}
	if r := seen.Roots[0]; r.SkippedInFlight != 1 || r.Done != 1 || r.Failed != 0 || seen.SkippedBytes != int64(len(files["one.txt"])) {
		t.Fatalf("progress = %+v", seen)
	}

	release()
	release() // releasing twice is harmless
	st, err = e.Round(context.Background(), []*peer.Entry{entryAt("bbbb", b.srv)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Pulled != 1 || st.SkippedInFlight != 0 {
		t.Fatalf("after release: %+v", st)
	}
	e.assertHolds(t, files)
}

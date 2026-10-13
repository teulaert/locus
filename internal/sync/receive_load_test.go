package sync

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"slices"
	gosync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// putTiming is one put as its pusher saw it: how long it took in all and,
// for a put whose body was asked for, how long the receiver took from
// having the request's headers to its go-ahead (100 Continue) for the
// body. A put answered without its body (LOC-78) has early set.
type putTiming struct {
	took, goAhead time.Duration
	early         bool
}

// loadPeer is a receiving peer server.
type loadPeer struct {
	*engineEnv
	srv *httptest.Server
}

func newLoadPeer(t *testing.T, senders ...*identity.Identity) *loadPeer {
	t.Helper()
	e := newEngineEnv(t)
	var ids []string
	for _, s := range senders {
		ids = append(ids, s.PeerID)
	}
	p := &loadPeer{engineEnv: e}
	p.srv = httptest.NewServer((&Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, AuthorizedPeers: ids}).Handler())
	t.Cleanup(p.srv.Close)
	return p
}

// timedPush pushes one file as a round does and times it, watching the
// request on the wire through an httptrace.
func timedPush(t *testing.T, sender *engineEnv, base, rel string, me *ManifestEntry) (Result, putTiming) {
	t.Helper()
	var (
		mu               gosync.Mutex
		headers, goAhead time.Time
	)
	trace := &httptrace.ClientTrace{
		WroteHeaders:   func() { mu.Lock(); headers = time.Now(); mu.Unlock() },
		Got100Continue: func() { mu.Lock(); goAhead = time.Now(); mu.Unlock() },
	}
	start := time.Now()
	res, err := sender.Push(httptrace.WithClientTrace(context.Background(), trace), base, sender.root, rel, me)
	pt := putTiming{took: time.Since(start)}
	if err != nil {
		t.Errorf("push %s: %v", rel, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if goAhead.IsZero() {
		pt.early = true
	} else {
		pt.goAhead = goAhead.Sub(headers)
	}
	return res, pt
}

// summary splits timings into put latencies and go-ahead waits, counting
// the puts answered early.
func summary(pts []putTiming) (took, goAhead []time.Duration, early int) {
	for _, pt := range pts {
		took = append(took, pt.took)
		if pt.early {
			early++
		} else {
			goAhead = append(goAhead, pt.goAhead)
		}
	}
	return took, goAhead, early
}

// busyWriter keeps state.db's writer connection as busy as a loaded
// receiver's is: a reconciler pass over records new files (a record read
// and a record write per file), pulls from other peers record what they
// fetched (a record read and three writes each: provenance, record, clear
// provenance) and index workers look up the records of the files they
// index. Every write holds the writer for commit, a write transaction kept
// open standing in for the fsync a real disk takes. It runs until stop is
// closed and returns how many writes were made.
func busyWriter(t *testing.T, st *store.Store, root string, commit time.Duration, stop <-chan struct{}) (wait func() (written int)) {
	t.Helper()
	var written atomic.Int64
	var wg gosync.WaitGroup
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
	write := func(rel string) bool {
		tx, err := st.State.Begin()
		if err != nil {
			t.Error(err)
			return false
		}
		if _, err := tx.Exec(`INSERT INTO docs (root, relpath, size, hash, version, last_writer, deleted, updated_at) VALUES (?, ?, 1, 'h', '{}', 'w', 0, 0)
			ON CONFLICT (root, relpath) DO UPDATE SET updated_at = updated_at + 1`, root, rel); err != nil {
			t.Error(err)
			tx.Rollback()
			return false
		}
		time.Sleep(commit)
		if err := tx.Commit(); err != nil {
			t.Error(err)
			return false
		}
		written.Add(1)
		return true
	}
	writers := []struct {
		name   string
		writes int
	}{{"reconciled", 1}, {"pulled-a", 3}, {"pulled-b", 3}}
	for _, w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stopped(); i++ {
				rel := fmt.Sprintf("%s/%05d.txt", w.name, i)
				_, _ = st.GetDoc(root, rel)
				for k := 0; k < w.writes; k++ {
					if !write(rel) {
						return
					}
				}
			}
		}()
	}
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stopped(); i++ {
				_, _ = st.GetDoc(root, fmt.Sprintf("reconciled/%05d.txt", i%3000))
				time.Sleep(time.Millisecond)
			}
		}()
	}
	return func() int { wg.Wait(); return int(written.Load()) }
}

// pushAll pushes each named file of sender to base one after another, as
// a round does for one peer, each expected to be applied.
func pushAll(t *testing.T, sender *engineEnv, base string, rels []string) []putTiming {
	t.Helper()
	m, err := sender.LocalManifest(sender.root)
	if err != nil {
		t.Fatal(err)
	}
	byPath := make(map[string]ManifestEntry, len(m))
	for _, me := range m {
		byPath[me.RelPath] = me
	}
	var out []putTiming
	for _, rel := range rels {
		me := byPath[rel]
		res, pt := timedPush(t, sender, base, rel, &me)
		if res != ResultApplied {
			t.Errorf("push %s: %s", rel, res)
		}
		out = append(out, pt)
	}
	return out
}

// tinySenders makes n senders that each hold perSender tiny files of
// their own, ready to push.
func tinySenders(t *testing.T, n, perSender int) ([]*engineEnv, [][]string) {
	t.Helper()
	var senders []*engineEnv
	var files [][]string
	for s := 0; s < n; s++ {
		e := newEngineEnv(t)
		if err := os.MkdirAll(filepath.Join(e.root, "tiny"), 0o755); err != nil {
			t.Fatal(err)
		}
		var rels []string
		for i := 0; i < perSender; i++ {
			rel := fmt.Sprintf("tiny/s%d-%04d.txt", s, i)
			e.putFile(t, rel, fmt.Sprintf("tiny file %d of sender %d\n", i, s), vvector.New().Increment(e.Identity.PeerID), e.Identity.PeerID)
			rels = append(rels, rel)
		}
		senders = append(senders, e)
		files = append(files, rels)
	}
	return senders, files
}

// pushConcurrently runs one pusher per sender at once and returns every
// put's timing and the wall time it all took.
func pushConcurrently(t *testing.T, senders []*engineEnv, files [][]string, base string) ([]putTiming, time.Duration) {
	t.Helper()
	var (
		mu  gosync.Mutex
		all []putTiming
		wg  gosync.WaitGroup
	)
	start := time.Now()
	for i, s := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lat := pushAll(t, s, base, files[i])
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return all, time.Since(start)
}

// quantile returns the q-quantile (0..1) of ds.
func quantile(ds []time.Duration, q float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[int(q*float64(len(s)-1))]
}

// LOC-83: a receiver whose state.db writer is kept busy by a reconciler
// pass, pulls and index workers still takes small puts at tens of
// milliseconds each. A put's checks before its body (is this content here
// already, LOC-78) read the record through the read pool, so no put waits
// for the writer before the receiver asks for its body; only recording
// what arrived does.
func TestPutsStayFastWhileWriterIsBusy(t *testing.T) {
	senders, files := tinySenders(t, 2, 100)
	recv := newLoadPeer(t, senders[0].Identity, senders[1].Identity)

	stop := make(chan struct{})
	wait := busyWriter(t, recv.Store, recv.root, 5*time.Millisecond, stop)
	pts, wall := pushConcurrently(t, senders, files, recv.srv.URL)
	close(stop)
	written := wait()

	took, goAhead, early := summary(pts)
	rate := float64(len(took)) / wall.Seconds()
	t.Logf("200 tiny puts from 2 pushers against a busy writer (%d other writes meanwhile): %.1f puts/s; latency p50 %v p90 %v max %v; go-ahead for the body p50 %v max %v (%d answered early)",
		written, rate, quantile(took, .5), quantile(took, .9), quantile(took, 1), quantile(goAhead, .5), quantile(goAhead, 1), early)
	if early != 0 {
		t.Fatalf("%d puts answered early; want every body asked for", early)
	}
	if p50 := quantile(took, .5); p50 > 150*time.Millisecond {
		t.Errorf("median put latency %v; want tens of milliseconds", p50)
	}
	if max := quantile(goAhead, 1); max > 50*time.Millisecond {
		t.Errorf("a put waited %v for the go-ahead for its body; the checks before it must not queue for the writer", max)
	}
}

// LOC-83: a put whose receiver is held in a long write transaction still
// gets its go-ahead at once: the checks before the body do not wait for
// the writer, so the pusher neither waits for the transaction nor falls
// back on the transport's ExpectContinueTimeout. The record write after
// the body has to wait, which is what a write is for. A put of content the
// receiver already holds at that version needs no write at all, and is
// answered early (LOC-78) while the writer is still held.
func TestPutAsksForBodyWhileWriterIsHeld(t *testing.T) {
	senders, files := tinySenders(t, 1, 6)
	recv := newLoadPeer(t, senders[0].Identity)
	// The receiver holds the last file already, at the sender's version.
	held := files[0][5]
	files[0] = files[0][:5]
	if err := os.MkdirAll(filepath.Join(recv.root, "tiny"), 0o755); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(senders[0].root, held))
	if err != nil {
		t.Fatal(err)
	}
	recv.putFile(t, held, string(content), vvector.New().Increment(senders[0].Identity.PeerID), senders[0].Identity.PeerID)
	heldEntry := senders[0].manifestEntry(t, held)

	const hold = 1500 * time.Millisecond
	locked := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		tx, err := recv.Store.State.Begin()
		if err != nil {
			t.Error(err)
			close(locked)
			return
		}
		if _, err := tx.Exec(`INSERT INTO meta (key, value) VALUES ('loc83', 'held') ON CONFLICT (key) DO UPDATE SET value = excluded.value`); err != nil {
			t.Error(err)
		}
		close(locked)
		time.Sleep(hold)
		_ = tx.Commit()
	}()
	<-locked
	res, heldPut := timedPush(t, senders[0], recv.srv.URL, held, &heldEntry)
	if res != ResultNoop || !heldPut.early {
		t.Fatalf("push of held content: %s, %+v; want it answered early", res, heldPut)
	}
	pts := pushAll(t, senders[0], recv.srv.URL, files[0])
	<-finished
	took, goAhead, early := summary(pts)
	t.Logf("puts while the writer was held for %v: held content answered in %v; others took %v; go-ahead for the body max %v (%d answered early)", hold, heldPut.took, took, quantile(goAhead, 1), early)
	if early != 0 {
		t.Fatalf("%d puts of new content answered early", early)
	}
	if heldPut.took > 100*time.Millisecond {
		t.Errorf("a put of held content took %v while the writer was held", heldPut.took)
	}
	if max := quantile(goAhead, 1); max > 100*time.Millisecond {
		t.Errorf("a put waited %v for the go-ahead for its body while the writer was held", max)
	}
}

// LOC-83: with a prompt receiver a pusher's "Expect: 100-continue" costs
// a round trip, never the transport's ExpectContinueTimeout: tiny files
// from concurrent pushers go well above ten puts a second on loopback.
func TestTinyPutsAreNotPacedByExpectContinue(t *testing.T) {
	senders, files := tinySenders(t, 2, 50)
	recv := newLoadPeer(t, senders[0].Identity, senders[1].Identity)
	pts, wall := pushConcurrently(t, senders, files, recv.srv.URL)
	took, goAhead, _ := summary(pts)
	rate := float64(len(took)) / wall.Seconds()
	t.Logf("100 tiny puts from 2 pushers: %.1f puts/s; latency p50 %v max %v; go-ahead max %v", rate, quantile(took, .5), quantile(took, 1), quantile(goAhead, 1))
	if rate < 10 {
		t.Errorf("%.1f puts/s; want more than 10", rate)
	}
	if max := quantile(took, 1); max >= PeerExpectContinueTimeout {
		t.Errorf("a put took %v: the pusher waited out ExpectContinueTimeout", max)
	}
}

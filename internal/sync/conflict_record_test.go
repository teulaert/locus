package sync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// keepAndOther orders two concurrent versions by the conflict rule.
func keepAndOther(a, b Version) (keep, other Version) {
	if IsIncomingWinner(a, b) {
		return b, a
	}
	return a, b
}

// The reply to a put that conflicts carries the conflict that put recorded,
// not the newest open conflict on the path (LOC-72).
func TestPutReplyCarriesTheConflictItRecorded(t *testing.T) {
	b := newEngineEnv(t)
	const writerA, writerB = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	b.putFile(t, "plan.txt", "edited on b", vvector.New().Increment(writerB), writerB)
	// An unrelated, newer open conflict on the same path.
	if _, err := b.Store.PutConflict(&store.Conflict{ID: "docs:plan.txt:11111111:22222222", Root: b.root, RelPath: "plan.txt",
		LocalPath: "plan.txt", LocalHash: "1111111111", ConflictHash: "2222222222", LocalWriter: writerB, ConflictWriter: writerA,
		CreatedAt: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	s := &Server{Engine: b.Engine, Identity: b.Identity, Roots: []string{b.root}, Known: func(string) bool { return true }}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	body := []byte("edited on a")
	q := url.Values{
		"rootname": {RootName(b.root)},
		"relpath":  {"plan.txt"},
		"version":  {vvector.New().Increment(writerA).String()},
		"hash":     {fswatch.HashBytes(body)},
		"size":     {strconv.Itoa(len(body))},
		"writer":   {writerA},
	}
	code, out := postAs(t, srv, testIdentity(t), "/v1/put", q, body)
	if code != http.StatusOK || out["result"] != string(ResultConflicted) {
		t.Fatalf("put: status %d, reply %v; want conflicted", code, out)
	}
	raw, _ := json.Marshal(out["conflict"])
	var got store.Conflict
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	keep, other := keepAndOther(Version{Writer: writerB, Hash: fswatch.HashBytes([]byte("edited on b"))}, Version{Writer: writerA, Hash: fswatch.HashBytes(body)})
	if got.LocalHash != keep.Hash || got.ConflictHash != other.Hash {
		t.Fatalf("reply conflict = %+v, want the one this put recorded (keep %.8s, other %.8s)", got, keep.Hash, other.Hash)
	}
	if want := ConflictID("docs", "plan.txt", keep.Hash, other.Hash); got.ID != want {
		t.Fatalf("reply conflict id = %q, want %q", got.ID, want)
	}
}

// Two peers that both record one conflict give it the same ID, derived from
// the root's name rather than its local path, and the pusher adopting the
// receiver's report lists it once (LOC-72).
func TestConflictRecordedOnceWithSameIDOnBothPeers(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	idA, idB := a.Identity.PeerID, b.Identity.PeerID
	base := vvector.New().Increment(idB)
	a.putFile(t, "plan.txt", "ancestor", base, idB)
	b.putFile(t, "plan.txt", "ancestor", base, idB)
	a.putFile(t, "plan.txt", "edited on a", base.Increment(idA), idA)
	b.putFile(t, "plan.txt", "edited on b", base.Increment(idB), idB)

	serve := func(e *engineEnv) *peer.Entry {
		s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, Known: func(string) bool { return true }}
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		return &peer.Entry{Info: peer.Info{PeerID: e.Identity.PeerID, SyncAddr: strings.TrimPrefix(srv.URL, "http://")}, Reachable: true}
	}
	pa, pb := serve(a), serve(b)
	for i := 0; i < 3; i++ {
		if st, err := a.Round(context.Background(), []*peer.Entry{pb}); err != nil || len(st.Errors) != 0 {
			t.Fatalf("round a->b: %+v, %v", st, err)
		}
		if st, err := b.Round(context.Background(), []*peer.Entry{pa}); err != nil || len(st.Errors) != 0 {
			t.Fatalf("round b->a: %+v, %v", st, err)
		}
	}
	keep, other := keepAndOther(Version{Writer: idA, Hash: fswatch.HashBytes([]byte("edited on a"))}, Version{Writer: idB, Hash: fswatch.HashBytes([]byte("edited on b"))})
	want := ConflictID("docs", "plan.txt", keep.Hash, other.Hash)
	for name, e := range map[string]*engineEnv{"a": a, "b": b} {
		list, err := e.Store.ListAllConflicts()
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0].ID != want || list[0].Root != e.root {
			t.Fatalf("conflicts on %s = %+v, want one with id %q in its own root", name, list, want)
		}
	}
}

// A conflict a peer on an earlier build reports carries its own root path in
// the ID. Adopting it derives the ID again, and a conflict already recorded
// here under any ID is not recorded a second time (LOC-72).
func TestAdoptConflictDerivesIDAndDeduplicates(t *testing.T) {
	a := newEngineEnv(t)
	const writerA, writerB = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	reported := func(rel string) *store.Conflict {
		return &store.Conflict{ID: "/home/elsewhere/docs:" + rel + ":aaaaaaaa:bbbbbbbb", Root: "/home/elsewhere/docs", RelPath: rel,
			LocalPath: rel, ConflictPath: rel + ".conflict", LocalHash: "aaaaaaaaaa", ConflictHash: "bbbbbbbbbb",
			LocalWriter: writerA, ConflictWriter: writerB, CreatedAt: time.Now().Unix()}
	}
	a.adoptConflict(a.root, reported("new.txt"))
	list, err := a.Store.ListConflicts()
	if err != nil || len(list) != 1 {
		t.Fatalf("conflicts = %+v (%v), want the adopted one", list, err)
	}
	if want := ConflictID("docs", "new.txt", "aaaaaaaaaa", "bbbbbbbbbb"); list[0].ID != want || list[0].Root != a.root {
		t.Fatalf("adopted conflict = %+v, want id %q in root %s", list[0], want, a.root)
	}

	// Known here under a legacy ID: adopting the report changes nothing.
	legacy := reported("old.txt")
	legacy.ID, legacy.Root = a.root+":old.txt:aaaaaaaa:bbbbbbbb", a.root
	if _, err := a.Store.PutConflict(legacy); err != nil {
		t.Fatal(err)
	}
	a.adoptConflict(a.root, reported("old.txt"))
	a.adoptConflict(a.root, reported("old.txt"))
	all, err := a.Store.ListAllConflicts()
	if err != nil || len(all) != 2 {
		t.Fatalf("conflicts = %+v (%v), want new.txt and old.txt once each", all, err)
	}
}

package sync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	gosync "sync"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// manifestAnswers serves e's sync server and records the status and
// Content-Encoding of every manifest it answers.
type manifestAnswers struct {
	mu      gosync.Mutex
	answers []string
}

func (m *manifestAnswers) take() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.answers
	m.answers = nil
	return out
}

type recordingWriter struct {
	http.ResponseWriter
	code int
}

func (w *recordingWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func serveRecorded(t *testing.T, e *engineEnv) (*peer.Entry, *manifestAnswers) {
	t.Helper()
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, Known: func(string) bool { return true }}
	rec := &manifestAnswers{}
	h := s.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &recordingWriter{ResponseWriter: w, code: http.StatusOK}
		h.ServeHTTP(rw, r)
		if r.URL.Path == "/v1/manifest" {
			rec.mu.Lock()
			rec.answers = append(rec.answers, http.StatusText(rw.code)+" "+w.Header().Get("Content-Encoding"))
			rec.mu.Unlock()
		}
	}))
	t.Cleanup(srv.Close)
	return &peer.Entry{Info: peer.Info{PeerID: e.Identity.PeerID, SyncAddr: strings.TrimPrefix(srv.URL, "http://")}, Reachable: true}, rec
}

// An unchanged manifest is answered 304 and the one fetched before planned
// against; a changed one is sent again, gzipped, and synchronized.
func TestRoundFetchesUnchangedManifestOnce(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	b.putFile(t, "one.txt", "one\n", vvector.New().Increment(b.Identity.PeerID), b.Identity.PeerID)
	pb, rec := serveRecorded(t, b)

	cleanRound(t, a, pb)
	cleanRound(t, a, pb)
	if got := rec.take(); len(got) != 2 || got[0] != "OK gzip" || got[1] != "Not Modified " {
		t.Fatalf("manifest answers %q, want a gzipped one then not modified", got)
	}

	b.putFile(t, "two.txt", "two\n", vvector.New().Increment(b.Identity.PeerID), b.Identity.PeerID)
	cleanRound(t, a, pb)
	if got := rec.take(); len(got) != 1 || got[0] != "OK gzip" {
		t.Fatalf("manifest answers after a change %q, want it sent again", got)
	}
	a.assertHolds(t, map[string]string{"one.txt": "one\n", "two.txt": "two\n"})
}

// A fetcher that does not ask for gzip, as builds before it, gets the plain
// manifest.
func TestManifestPlainWithoutAcceptEncoding(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	b.putFile(t, "one.txt", "one\n", vvector.New().Increment(b.Identity.PeerID), b.Identity.PeerID)
	pb, _ := serveRecorded(t, b)
	base := "http://" + pb.SyncAddr
	target := Target(base, "/v1/manifest", map[string][]string{"rootname": {b.NameOf(b.root)}})
	resp, err := a.send(context.Background(), http.MethodGet, base, target, nil, emptyBodyHash)
	if err != nil {
		t.Fatal(err)
	}
	body, err := a.readBody(context.Background(), resp)
	if err != nil {
		t.Fatal(err)
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" {
		t.Fatalf("manifest sent with Content-Encoding %q to a fetcher that did not ask", enc)
	}
	if m, err := DecodeManifest(body, b.NameOf(b.root)); err != nil || len(m.Entries) != 1 {
		t.Fatalf("plain manifest: %+v, %v", m, err)
	}
}

// The peer with the higher ID leaves the interval rounds to the one with the
// lower ID while that one fetches its manifests and its own last round is
// settled, and resumes them when the leader stops, its manifests change or
// followerRefresh has passed since its own last round.
func TestInitiatingLeavesLedPeers(t *testing.T) {
	x, y := newEngineEnv(t), newEngineEnv(t)
	leader, follower := x, y
	if y.Identity.PeerID < x.Identity.PeerID {
		leader, follower = y, x
	}
	now := time.Now()
	for _, e := range []*engineEnv{leader, follower} {
		e.FollowWindow = 30 * time.Second
		e.now = func() time.Time { return now }
	}
	pl, pf := servePeer(t, leader), servePeer(t, follower)
	initiates := func(e *engineEnv, p *peer.Entry) bool { return len(e.Initiating([]*peer.Entry{p})) == 1 }

	if !initiates(follower, pl) {
		t.Fatal("a follower that has not run a round with its leader left it out")
	}
	cleanRound(t, follower, pl)
	if !initiates(follower, pl) {
		t.Fatal("a follower left out a leader that has not fetched its manifest")
	}
	cleanRound(t, leader, pf)
	if !initiates(follower, pl) {
		t.Fatal("a follower left out a leader before a round of its own began a second after its manifests were seen")
	}
	now = now.Add(time.Second)
	cleanRound(t, follower, pl)
	if initiates(follower, pl) {
		t.Fatal("a follower started a round with an active leader")
	}
	if !initiates(leader, pf) {
		t.Fatal("a leader left out its follower")
	}

	// A change of the follower's own, a deletion say, is followed by
	// rounds of its own until one began in a later second than the change:
	// pruning compares whole seconds, and that round's record is the one
	// it waits for.
	follower.putFile(t, "mine.txt", "mine\n", vvector.New().Increment(follower.Identity.PeerID), follower.Identity.PeerID)
	if !initiates(follower, pl) {
		t.Fatal("a follower with a change of its own left the round to its leader")
	}
	cleanRound(t, follower, pl)
	cleanRound(t, leader, pf)
	if !initiates(follower, pl) {
		t.Fatal("a follower stopped after a round in the second of its change")
	}
	now = now.Add(time.Second)
	cleanRound(t, follower, pl)
	if initiates(follower, pl) {
		t.Fatal("a follower kept starting rounds once its change was exchanged")
	}

	now = now.Add(31 * time.Second)
	if !initiates(follower, pl) {
		t.Fatal("a follower kept leaving rounds to a leader that stopped fetching")
	}
	cleanRound(t, leader, pf)
	if initiates(follower, pl) {
		t.Fatal("a follower did not leave rounds to a leader that came back")
	}
	now = now.Add(followerRefresh)
	cleanRound(t, leader, pf)
	if !initiates(follower, pl) {
		t.Fatal("a follower did not run its own round after followerRefresh")
	}

	follower.FollowWindow = 0
	cleanRound(t, follower, pl)
	cleanRound(t, leader, pf)
	if !initiates(follower, pl) {
		t.Fatal("with no FollowWindow a peer was left out")
	}
}

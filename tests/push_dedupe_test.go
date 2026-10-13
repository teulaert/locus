package tests

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
)

// slowLink delays each peer request to the paths it names, as a real link
// makes every file take a moment: on loopback one seed pushes a few hundred
// small files before the other's round has even planned, and the overlap
// a joining peer sees on a real network never happens.
type slowLink struct {
	rt    http.RoundTripper
	delay time.Duration
	paths map[string]bool
}

func (s slowLink) RoundTrip(r *http.Request) (*http.Response, error) {
	if s.paths[r.URL.Path] {
		time.Sleep(s.delay)
	}
	return s.rt.RoundTrip(r)
}

// overSlowLink makes a node's sync requests to paths take delay longer.
func overSlowLink(nd *node, delay time.Duration, paths ...string) {
	set := map[string]bool{}
	for _, p := range paths {
		set[p] = true
	}
	nd.beforeServe = func(a *app.App) {
		a.Sync.HTTPClient = &http.Client{Transport: slowLink{rt: http.DefaultTransport.(*http.Transport).Clone(), delay: delay, paths: set}}
	}
}

// joinTwoSeeds seeds two peers with n files, then brings an empty third
// peer online, with the seeds' sync intervals as interval(i) says, and
// returns how many copies of a file the joiner was sent in all: the files
// the seeds served it (its pulls) plus the puts it received in full (their
// pushes). Each restart resets the counters, so the seeding itself is not
// counted.
func joinTwoSeeds(t *testing.T, n int, interval func(seed int) time.Duration) (served, received int64) {
	t.Helper()
	c := newCluster(t, 3)
	c.dumpOnFailure()
	seedA, seedB, joiner := c.nodes[0], c.nodes[1], c.nodes[2]
	joiner.stopServing(t)

	body := strings.Repeat("locus push dedupe\n", 200) // ~3.5 KB each
	for i := 0; i < n; i++ {
		seedA.write(t, fmt.Sprintf("set%d/doc%03d.txt", i%9, i), fmt.Sprintf("%d\n%s", i, body))
	}
	waitFor(t, 60*time.Second, func() bool {
		return len(seedB.listRel(t)) == n && treesMatch(t, seedA, seedB)
	}, "the second seed to hold every file")
	// Let the seeds' version vectors settle, so the joiner sees one version.
	time.Sleep(2 * time.Second)

	for i, s := range []*node{seedA, seedB} {
		s.stopServing(t)
		s.Cfg.Sync.Interval = config.Duration(interval(i))
		overSlowLink(s, 10*time.Millisecond, "/v1/put")
		s.restart(t)
	}
	overSlowLink(joiner, 10*time.Millisecond, "/v1/file")
	joiner.restart(t)
	waitFor(t, 60*time.Second, func() bool {
		return len(joiner.listRel(t)) == n && treesMatch(t, seedA, joiner)
	}, "the joining peer to hold every file")
	// A few more rounds move nothing.
	time.Sleep(2 * time.Second)

	a, _ := seedA.App.Sync.Served()
	b, _ := seedB.App.Sync.Served()
	got := joiner.App.Sync.Received()
	t.Logf("the seeds served %d + %d = %d files for %d; the joiner received %d puts; %d copies in all",
		a, b, a+b, n, got, a+b+got)
	if !treesMatch(t, seedA, joiner) || !treesMatch(t, seedB, joiner) {
		t.Fatal("peers diverged after converging")
	}
	return a + b, got
}

// LOC-78: a peer joining two seeds that hold the same files used to be
// pushed every file by both, on top of what it pulled, since each seed's
// round planned a push of everything the joiner lacked. A put of content
// the joiner already holds, or is already receiving, is now answered
// before its body is sent, so with ordinary sync intervals on every peer
// the joiner is sent about one copy of the root, not two or more.
func TestJoiningPeerIsSentEachFileOnce(t *testing.T) {
	const n = 200
	served, received := joinTwoSeeds(t, n, func(int) time.Duration { return 400 * time.Millisecond })
	if total := served + received; total > n+n/10 {
		t.Fatalf("the joiner was sent %d copies of %d files (%d pulled, %d pushed)", total, n, served, received)
	}
}

// The mixed case: the joiner pulls from both seeds while only one of them
// pushes (the other starts no rounds of its own). Pulls and puts claim the
// same content, so the joiner still gets about one copy.
func TestJoiningPeerPullAndPushOverlap(t *testing.T) {
	const n = 200
	served, received := joinTwoSeeds(t, n, func(seed int) time.Duration {
		if seed == 0 {
			return time.Hour
		}
		return 400 * time.Millisecond
	})
	if total := served + received; total > n+n/10 {
		t.Fatalf("the joiner was sent %d copies of %d files (%d pulled, %d pushed)", total, n, served, received)
	}
}

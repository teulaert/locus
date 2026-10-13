package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/config"
)

// LOC-77: a peer joining two peers that hold the same files used to pull
// every file from both at once, since each peer's round planned against the
// same empty manifest. Pulls are claimed first, so the two seeds serve the
// joining peer about one copy of the root between them, not two.
func TestJoiningPeerPullsEachFileOnce(t *testing.T) {
	c := newCluster(t, 3)
	c.dumpOnFailure()
	seedA, seedB, joiner := c.nodes[0], c.nodes[1], c.nodes[2]
	joiner.stopServing(t)

	const n = 200
	body := strings.Repeat("locus pull dedupe\n", 200) // ~3.5 KB each
	for i := 0; i < n; i++ {
		seedA.write(t, fmt.Sprintf("set%d/doc%03d.txt", i%9, i), fmt.Sprintf("%d\n%s", i, body))
	}
	waitFor(t, 60*time.Second, func() bool {
		return len(seedB.listRel(t)) == n && treesMatch(t, seedA, seedB)
	}, "the second seed to hold every file")
	// Let the seeds' version vectors settle, so the joiner sees one version.
	time.Sleep(2 * time.Second)

	// The seeds stop starting rounds of their own, so they push nothing
	// to the joiner: everything it gets, it pulls, from both at once.
	for _, s := range []*node{seedA, seedB} {
		s.stopServing(t)
		s.Cfg.Sync.Interval = config.Duration(time.Hour)
		s.restart(t)
	}
	joiner.restart(t)
	waitFor(t, 60*time.Second, func() bool {
		return len(joiner.listRel(t)) == n && treesMatch(t, seedA, joiner)
	}, "the joining peer to hold every file")
	// A few more rounds move nothing.
	time.Sleep(2 * time.Second)

	a, _ := seedA.App.Sync.Served()
	b, _ := seedB.App.Sync.Served()
	t.Logf("the seeds served %d + %d = %d files for %d; the joiner received %d puts", a, b, a+b, n, joiner.App.Sync.Received())
	if a+b > n+n/20 {
		t.Fatalf("the seeds served %d files for %d: the joiner pulled files from both", a+b, n)
	}
	if !treesMatch(t, seedA, joiner) || !treesMatch(t, seedB, joiner) {
		t.Fatal("peers diverged after converging")
	}
}

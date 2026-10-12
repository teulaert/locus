package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/store"
)

// Three peers edit one file while apart. Once they meet, every version
// survives exactly once in every tree, each peer lists each conflict once,
// and a conflict recorded on several peers has the same ID and sibling
// there: the ID uses the root's name, not the peer's root folder (LOC-72),
// and the sibling is named by the losing writer's short peer ID, not by a
// hostname only some peers know (LOC-73).
func TestThreePeerConflictListedOncePerPeer(t *testing.T) {
	c := newClusterWithRoots(t, 3, func(i int) (string, string) { return fmt.Sprintf("folder%d", i+1), "shared" })
	c.dumpOnFailure()

	c.nodes[0].write(t, "plan.txt", "common ancestor\n")
	c.waitForConvergence()
	for _, n := range c.nodes {
		n.stopServing(t)
	}
	versions := map[string]bool{}
	for i, n := range c.nodes {
		v := fmt.Sprintf("plan as edited on peer %d\n", i+1)
		n.write(t, "plan.txt", v)
		versions[hashString(v)] = true
	}
	for _, n := range c.nodes {
		n.restart(t)
	}
	c.waitConnected()

	// Converged: identical trees, each version present exactly once in
	// each, and every conflict record pointing at a copy on disk.
	settled := func() bool {
		for _, n := range c.nodes[1:] {
			if !treesMatch(t, c.nodes[0], n) {
				return false
			}
		}
		for _, n := range c.nodes {
			seen := map[string]int{}
			for _, rel := range n.listRel(t) {
				if rel == "plan.txt" || strings.HasPrefix(rel, "plan.conflict-") {
					s, _ := n.read(t, rel)
					seen[hashString(s)]++
				}
			}
			for h := range versions {
				if seen[h] != 1 {
					return false
				}
			}
			list, err := n.App.Store.ListConflicts()
			if err != nil {
				t.Fatal(err)
			}
			for _, cf := range list {
				if cf.ConflictPath != "" && !n.has(t, cf.ConflictPath) {
					return false
				}
			}
		}
		return true
	}
	waitFor(t, 60*time.Second, settled, "three peers to converge on one copy of every version")
	// A few more rounds must neither add records nor copies.
	time.Sleep(2 * time.Second)
	if !settled() {
		t.Fatal("peers diverged again after converging")
	}

	byKey := map[string]*store.Conflict{}
	total := 0
	for _, n := range c.nodes {
		list, err := n.App.Store.ListAllConflicts()
		if err != nil {
			t.Fatal(err)
		}
		ids := map[string]bool{}
		for _, cf := range list {
			if ids[cf.ID] {
				t.Fatalf("%s lists conflict %s twice", n.Name, cf.ID)
			}
			ids[cf.ID] = true
			total++
			if !strings.HasPrefix(cf.ID, "shared:plan.txt:") || strings.Contains(cf.ID, "folder") {
				t.Fatalf("%s: conflict id %q does not use the root name", n.Name, cf.ID)
			}
			if want := "plan.conflict-" + identity.Short(cf.ConflictWriter) + "-" + cf.ConflictHash[:8] + ".txt"; cf.ConflictPath != want {
				t.Fatalf("%s: sibling %q, want %q", n.Name, cf.ConflictPath, want)
			}
			key := cf.LocalHash + "/" + cf.ConflictHash
			if other, ok := byKey[key]; ok && (other.ID != cf.ID || other.ConflictPath != cf.ConflictPath) {
				t.Fatalf("one conflict recorded differently on two peers: %+v vs %+v", other, cf)
			}
			byKey[key] = cf
		}
	}
	if total == 0 {
		t.Fatal("no peer recorded a conflict")
	}
}

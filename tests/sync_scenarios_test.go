package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"locus/internal/store"
)

// Scenario 1 — Two online peers converge after creating/editing/deleting files.
func TestScenario01_TwoOnlinePeersConverge(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	a.write(t, "readme.md", "# Project\n\ninitial content\n")
	a.write(t, "docs/spec.txt", "the specification\n")
	c.waitForConvergence()

	// Edit propagates.
	a.write(t, "readme.md", "# Project\n\nupdated content after review\n")
	c.waitForConvergence()

	// Delete propagates.
	a.remove(t, "docs/spec.txt")
	c.waitForConvergence()

	// Changes flow both ways: peers are equal.
	b.write(t, "from-b.txt", "written on the second peer\n")
	c.waitForConvergence()
	if got, ok := a.read(t, "from-b.txt"); !ok || !strings.Contains(got, "second peer") {
		t.Fatalf("reverse propagation failed: %q ok=%v", got, ok)
	}
}

// Scenario 2 — A laptop is offline, files change elsewhere, then it
// reconnects and converges.
func TestScenario02_OfflinePeerReconnects(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	a.write(t, "shared.txt", "common ancestor\n")
	c.waitForConvergence()

	// The laptop goes away for a while.
	b.stopServing(t)
	for i := 0; i < 5; i++ {
		a.write(t, "work/note1.txt", "written while peer was offline")
		a.write(t, "work/note2.txt", "more offline work")
		time.Sleep(20 * time.Millisecond)
	}
	a.remove(t, "shared.txt")
	// A keeps syncing into the void without erroring.
	time.Sleep(1200 * time.Millisecond)
	if stats := a.App.LastSync(); stats != nil && len(stats.Errors) > 0 {
		t.Logf("round errors while offline (tolerated): %v", stats.Errors)
	}

	// Reconnect: it must catch up without any manual intervention.
	b.restart(t)
	c.waitConnected()
	waitFor(t, 30*time.Second, func() bool { return treesMatch(t, a, b) }, "offline peer to catch up")

	for _, rel := range []string{"work/note1.txt", "work/note2.txt"} {
		if !b.has(t, rel) {
			t.Fatalf("file %s did not arrive after reconnect", rel)
		}
	}
	if b.has(t, "shared.txt") {
		t.Fatal("deletion made while peer was offline must apply after reconnect")
	}
}

// Scenario 3 — Two offline peers edit the same file and both versions survive
// as a conflict.
func TestScenario03_OfflineConcurrentEditsPreserveBothSides(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	ancestor := "common ancestor text\n"
	a.write(t, "proposal.txt", ancestor)
	c.waitForConvergence()

	// Both peers go offline and edit independently.
	a.stopServing(t)
	b.stopServing(t)
	a.write(t, "proposal.txt", "version edited on peer one")
	b.write(t, "proposal.txt", "version edited on peer two")

	a.restart(t)
	b.restart(t)
	c.waitConnected()

	waitFor(t, 30*time.Second, func() bool {
		if conflictCount(t, a) == 0 || conflictCount(t, b) == 0 {
			return false
		}
		// A record is only meaningful once the copy it points at is on disk.
		for _, n := range c.nodes {
			list, err := n.App.Store.ListConflicts()
			if err != nil || len(list) == 0 {
				return false
			}
			for _, cf := range list {
				if !n.has(t, cf.ConflictPath) {
					return false
				}
			}
		}
		return true
	}, "conflicts to be recorded on both peers with both copies on disk")

	for _, n := range c.nodes {
		bothSidesPreserved(t, n, "proposal.txt", "version edited on peer one", "version edited on peer two")
	}
	// The trees must converge to identical state (deterministic winner and
	// identically named sibling copies).
	waitFor(t, 30*time.Second, func() bool { return treesMatch(t, a, b) }, "conflict state to converge")
}

func conflictCount(t *testing.T, n *node) int {
	t.Helper()
	list, err := n.App.Store.ListConflicts()
	if err != nil {
		t.Fatal(err)
	}
	return len(list)
}

// bothSidesPreserved asserts the invariant that no content is ever silently
// discarded: the normal path plus sibling conflict copies must contain every
// divergent version.
func bothSidesPreserved(t *testing.T, n *node, rel string, wantA, wantB string) {
	t.Helper()
	dir := filepath.Dir(filepath.Join(n.Root, filepath.FromSlash(rel)))
	base := strings.TrimSuffix(filepath.Base(rel), filepath.Ext(rel))
	ext := filepath.Ext(rel)
	contents := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var siblings []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasPrefix(e.Name(), base) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		contents[string(b)] = true
		if strings.Contains(e.Name(), ".conflict-") {
			siblings = append(siblings, e.Name())
			// Sibling naming convention: name.conflict-<host>-<hash8>.ext
			if !strings.HasSuffix(e.Name(), ext) {
				t.Errorf("sibling %s lost its extension", e.Name())
			}
		}
	}
	if !contents[wantA] || !contents[wantB] {
		t.Fatalf("%s: both divergent versions must survive on %s; found %d distinct contents, siblings=%v", rel, n.Name, len(contents), siblings)
	}
	if len(siblings) == 0 {
		t.Fatalf("%s: expected a sibling conflict file on %s", rel, n.Name)
	}
}

// Scenario 6 — Locus is killed mid-transfer; no partial document becomes
// visible as a valid document.
func TestScenario06_KilledMidTransferLeavesNoPartialDocument(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	// A reasonably large document so a transfer takes measurable time.
	payload := strings.Repeat("partial-transfer-payload-", 200000) // ~5 MB
	a.write(t, "big.bin", payload)
	wantHash := hashString(payload)

	// Kill the receiving peer while the transfer is in flight, then wipe any
	// temporary spillover the way a crashed process would leave it and verify
	// the next round still produces a byte-identical result.
	time.Sleep(200 * time.Millisecond)
	b.stopServing(t)
	leftovers := tempLeftovers(t, b.Root)
	for _, p := range leftovers {
		// Simulate crash debris: a half-written temporary file.
		if err := os.WriteFile(p, []byte("garbage that must never be renamed into place"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	b.restart(t)
	c.waitConnected()
	waitFor(t, 60*time.Second, func() bool {
		got, ok := b.read(t, "big.bin")
		return ok && hashString(got) == wantHash
	}, "large file to transfer completely")

	// No temporary artifacts may remain next to the installed document.
	if got := tempLeftovers(t, b.Root); len(got) != 0 {
		t.Fatalf("transfer debris left behind: %v", got)
	}
	if treesMatch(t, a, b) {
		return
	}
	t.Fatal("trees did not converge after interrupted transfer")
}

func tempLeftovers(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), ".locus-tmp-") {
			out = append(out, path)
		}
		return nil
	})
	return out
}

// Conflict records must be inspectable and resolvable without losing data.
func TestConflictCLIRecordsAreComplete(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	a.write(t, "doc.txt", "ancestor\n")
	c.waitForConvergence()
	a.stopServing(t)
	b.stopServing(t)
	a.write(t, "doc.txt", "side one")
	b.write(t, "doc.txt", "side two")
	a.restart(t)
	b.restart(t)
	c.waitConnected()
	waitFor(t, 30*time.Second, func() bool { return conflictCount(t, a) > 0 }, "conflict record")
	// The record must always point at a real preserved copy on disk.
	waitFor(t, 30*time.Second, func() bool {
		for _, n := range c.nodes {
			list, err := n.App.Store.ListConflicts()
			if err != nil || len(list) == 0 {
				return false
			}
			for _, cf := range list {
				if !n.has(t, cf.ConflictPath) {
					return false
				}
			}
		}
		return true
	}, "conflict records to point at existing copies on both peers")

	list, err := a.App.Store.ListConflicts()
	if err != nil || len(list) == 0 {
		t.Fatalf("expected conflicts, got %v (%v)", list, err)
	}
	var rec *store.Conflict
	for _, cf := range list {
		if cf.RelPath == "doc.txt" {
			rec = cf
		}
	}
	if rec == nil {
		t.Fatalf("conflict for doc.txt not recorded: %+v", list)
	}
	if rec.LocalHash == "" || rec.ConflictHash == "" || rec.LocalWriter == "" || rec.ConflictWriter == "" {
		t.Fatalf("conflict record must identify both sides: %+v", rec)
	}
	if rec.ConflictPath == "" {
		t.Fatal("conflict record must point at the preserved sibling copy")
	}
	if _, err := a.App.Store.GetConflict(rec.ID); err != nil {
		t.Fatalf("conflict must be fetchable by id: %v", err)
	}
	// Resolving keeps the normal-path version and stops reporting it, but the
	// preserved sibling copy stays on disk.
	if err := a.App.Store.ResolveConflict(rec.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.App.Store.GetConflict(rec.ID); err != nil {
		t.Fatal("resolved conflict must remain in history")
	}
	if !a.has(t, rec.ConflictPath) {
		t.Fatalf("resolving must not delete the preserved copy %s", rec.ConflictPath)
	}
	if conflictCount(t, a) != 0 {
		t.Fatal("resolved conflicts must not be reported as unresolved")
	}
}

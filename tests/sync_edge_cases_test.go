package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/config"
)

// contentsUnder returns every file's content in a node's tree.
func contentsUnder(t *testing.T, n *node) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, rel := range n.listRel(t) {
		if s, ok := n.read(t, rel); ok {
			out[s] = true
		}
	}
	return out
}

// allTreesMatch reports whether every node presents the first node's tree.
func allTreesMatch(t *testing.T, c *cluster) bool {
	t.Helper()
	for _, n := range c.nodes[1:] {
		if !treesMatch(t, c.nodes[0], n) {
			return false
		}
	}
	return true
}

// Three peers edit the same ancestor while offline. Every version survives
// on every peer, the trees converge to the same names, and the normal path
// holds the same version everywhere (LOC-25).
func TestThreeWayConflictPreservesEveryVersion(t *testing.T) {
	c := newCluster(t, 3)
	c.dumpOnFailure()
	c.nodes[0].write(t, "plan.txt", "common ancestor\n")
	c.waitForConvergence()

	for _, n := range c.nodes {
		n.stopServing(t)
	}
	edits := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		edits[i] = "offline edit made on " + n.Name
		n.write(t, "plan.txt", edits[i])
	}
	for _, n := range c.nodes {
		n.restart(t)
	}
	c.waitConnected()

	waitFor(t, 45*time.Second, func() bool {
		for _, n := range c.nodes {
			have := contentsUnder(t, n)
			for _, e := range edits {
				if !have[e] {
					return false
				}
			}
		}
		return allTreesMatch(t, c)
	}, "all three versions on every peer and identical trees")

	normal, _ := c.nodes[0].read(t, "plan.txt")
	for _, n := range c.nodes {
		if got, _ := n.read(t, "plan.txt"); got != normal {
			t.Fatalf("normal path on %s = %q, on %s = %q", n.Name, got, c.nodes[0].Name, normal)
		}
		if conflictCount(t, n) == 0 {
			t.Fatalf("%s recorded no conflict", n.Name)
		}
		for _, rel := range n.listRel(t) {
			if rel != "plan.txt" && !strings.Contains(rel, ".conflict-") {
				t.Fatalf("%s holds an unexpected file %s", n.Name, rel)
			}
		}
	}
	if got := len(c.nodes[0].listRel(t)); got != len(edits) {
		t.Fatalf("want the normal path plus %d siblings, got %v", len(edits)-1, c.nodes[0].listRel(t))
	}
}

// One peer deletes a file while another edits it, both offline. The edit
// must survive on both peers once they reconnect: a deletion never silently
// discards content written concurrently with it (LOC-25). Which peer acts
// first matters, so both orders are forced: the passive peer comes back with
// an hour-long sync interval and only answers the other's round.
func TestDeleteVersusEditKeepsTheEdit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		deleterPushes bool
	}{
		{"deleter pushes the delete", true},
		{"editor pushes the edit", false},
	} {
		t.Run(tc.name, func(t *testing.T) { deleteVersusEdit(t, tc.deleterPushes) })
	}
}

func deleteVersusEdit(t *testing.T, deleterPushes bool) {
	c := newCluster(t, 2)
	deleter, editor := c.nodes[0], c.nodes[1]
	deleter.write(t, "report.txt", "common ancestor\n")
	c.waitForConvergence()
	waitFor(t, 30*time.Second, func() bool { return docsAgree(t, deleter, editor, "report.txt") }, "ancestor records to agree")

	deleter.stopServing(t)
	editor.stopServing(t)
	deleter.remove(t, "report.txt")
	const edit = "edited while the other peer deleted it\n"
	editor.write(t, "report.txt", edit)

	active, passive := deleter, editor
	if !deleterPushes {
		active, passive = editor, deleter
	}
	interval := passive.Cfg.Sync.Interval
	passive.Cfg.Sync.Interval = config.Duration(time.Hour)
	passive.restart(t)
	waitFor(t, 30*time.Second, func() bool {
		d, err := passive.App.Store.GetDoc(passive.Root, "report.txt")
		if err != nil {
			return false
		}
		if passive == deleter {
			return d.Deleted
		}
		return d.Hash == hashString(edit)
	}, "the passive peer to record its offline change")
	active.restart(t)
	c.waitConnected()

	survived := func(n *node) bool { return contentsUnder(t, n)[edit] }
	// settle waits until the edit is on both peers with matching trees, or
	// is gone from both, which nothing can undo.
	settle := func() {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			on, ond := survived(editor), survived(deleter)
			if (on && ond && treesMatch(t, deleter, editor)) || (!on && !ond) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	lost := func() {
		if !survived(editor) || !survived(deleter) {
			t.Fatalf("a concurrent delete discarded the edit (still on editor: %v, on deleter: %v); an edit must beat a delete (LOC-51)", survived(editor), survived(deleter))
		}
	}
	settle()
	lost()

	// Back to normal operation on both sides; the edit must still be there
	// after the passive peer's own rounds have run.
	passive.stopServing(t)
	passive.Cfg.Sync.Interval = interval
	passive.restart(t)
	c.waitConnected()
	time.Sleep(2 * time.Second) // several sync and reconcile intervals, to catch a late re-delete
	settle()
	lost()
	waitFor(t, 30*time.Second, func() bool { return treesMatch(t, deleter, editor) }, "trees to converge after delete-vs-edit")
	waitFor(t, 30*time.Second, func() bool { return docsAgree(t, deleter, editor, "report.txt") }, "records to agree after delete-vs-edit")
	for _, n := range []*node{deleter, editor} {
		d, err := n.App.Store.GetDoc(n.Root, "report.txt")
		if err != nil {
			t.Fatal(err)
		}
		if d.Deleted || d.Hash != hashString(edit) {
			t.Fatalf("record on %s = deleted %v, hash %.8s; want the live edit", n.Root, d.Deleted, d.Hash)
		}
	}
}

// A rename is propagated (as delete plus create): the new path arrives with
// the same bytes, the old path disappears, and nothing is reported as a
// conflict (LOC-25).
func TestRenamePropagates(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	const body = "a document that will be renamed and moved\n"
	a.write(t, "drafts/old-name.md", body)
	c.waitForConvergence()

	if err := os.MkdirAll(filepath.Join(a.Root, "final"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(a.Root, "drafts", "old-name.md"), filepath.Join(a.Root, "final", "new-name.md")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		got, ok := b.read(t, "final/new-name.md")
		return ok && got == body && !b.has(t, "drafts/old-name.md")
	}, "the renamed file to replace the old one on the peer")
	c.waitForConvergence()

	// A rename back on the other peer travels the opposite way.
	if err := os.Rename(filepath.Join(b.Root, "final", "new-name.md"), filepath.Join(b.Root, "drafts", "old-name.md")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		got, ok := a.read(t, "drafts/old-name.md")
		return ok && got == body && !a.has(t, "final/new-name.md")
	}, "the rename back to reach the first peer")
	for _, n := range c.nodes {
		if got := conflictCount(t, n); got != 0 {
			t.Fatalf("%s recorded %d conflicts for a plain rename", n.Name, got)
		}
	}
}

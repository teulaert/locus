package tests

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// Roots pair by configured name, not by folder name: ~/Documents on one
// machine syncs with ~/Sync/Docs on another when both are named alike. A
// third peer with a bare-string root whose basename equals that name (a
// config written before named roots) joins the same root.
func TestNamedRootsPairAcrossDifferentFolderNames(t *testing.T) {
	c := newClusterWithRoots(t, 3, func(i int) (string, string) {
		switch i {
		case 0:
			return "Documents", "shared"
		case 1:
			return filepath.Join("Sync", "Docs"), "shared"
		default:
			return "shared", ""
		}
	})
	a, b, old := c.nodes[0], c.nodes[1], c.nodes[2]
	if filepath.Base(a.Root) == filepath.Base(b.Root) {
		t.Fatalf("test setup: roots %s and %s should differ in basename", a.Root, b.Root)
	}

	a.write(t, "notes/plan.md", "# Plan\n\nwritten under Documents\n")
	b.write(t, "from-docs.txt", "written under Sync/Docs\n")
	old.write(t, "from-old-peer.txt", "written by a peer with a bare-string root\n")
	c.waitForConvergence()
	if got, ok := old.read(t, "notes/plan.md"); !ok || !strings.Contains(got, "under Documents") {
		t.Fatalf("bare-string peer missing content: %q ok=%v", got, ok)
	}
	if !a.has(t, "from-old-peer.txt") || !b.has(t, "from-old-peer.txt") {
		t.Fatal("content from the bare-string peer did not reach the named roots")
	}

	a.remove(t, "from-docs.txt")
	c.waitForConvergence()
	if b.has(t, "from-docs.txt") || old.has(t, "from-docs.txt") {
		t.Fatal("deletion did not propagate between differently named folders")
	}

	// Status reports the name peers pair by next to each local path.
	for _, n := range c.nodes {
		st, err := n.App.BuildStatus(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Roots) != 1 || st.Roots[0].Name != "shared" || st.Roots[0].Path != n.Root {
			t.Fatalf("%s status roots = %+v, want name shared at %s", n.Name, st.Roots, n.Root)
		}
	}
}

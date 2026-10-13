package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// listDirs returns every directory below a node's root, slash-separated.
func (n *node) listDirs(t *testing.T) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(n.Root, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == n.Root {
			return nil
		}
		rel, _ := filepath.Rel(n.Root, path)
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	sort.Strings(out)
	return out
}

// A folder renamed on one peer arrives elsewhere as deletions plus
// creations. Once the peers converge, every one of them holds only the new
// tree: the directories the deletions emptied are gone, not left behind as
// an empty skeleton beside the renamed folder (LOC-74). An empty folder a
// user made on one peer is not touched.
func TestThreePeerFolderRenameLeavesNoEmptySkeleton(t *testing.T) {
	c := newCluster(t, 3)
	c.dumpOnFailure()

	files := []string{
		"Meivakantie 2026/Visa Indonesië/visum.pdf",
		"Meivakantie 2026/Vliegtickets/Archief/ticket-1.pdf",
		"Meivakantie 2026/Vliegtickets/Archief/ticket-2.pdf",
		"Meivakantie 2026/Vliegtickets/boarding.pdf",
		"notities.txt",
	}
	for i, f := range files {
		c.nodes[0].write(t, "Postvak In/"+f, fmt.Sprintf("document %d\n", i))
	}
	c.nodes[0].write(t, "elders.txt", "outside the renamed folder\n")
	c.waitForConvergence()
	userEmpty := filepath.Join(c.nodes[1].Root, "Leeg")
	if err := os.Mkdir(userEmpty, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.Rename(filepath.Join(c.nodes[0].Root, "Postvak In"), filepath.Join(c.nodes[0].Root, "Inbox")); err != nil {
		t.Fatal(err)
	}

	want := []string{"Inbox", "Inbox/Meivakantie 2026", "Inbox/Meivakantie 2026/Visa Indonesië",
		"Inbox/Meivakantie 2026/Vliegtickets", "Inbox/Meivakantie 2026/Vliegtickets/Archief"}
	settled := func() bool {
		for i, n := range c.nodes {
			for _, f := range files {
				if !n.has(t, "Inbox/"+f) {
					return false
				}
			}
			if len(n.listRel(t)) != len(files)+1 {
				return false
			}
			dirs := n.listDirs(t)
			if i == 1 {
				dirs = without(dirs, "Leeg")
			}
			if strings.Join(dirs, "|") != strings.Join(want, "|") {
				return false
			}
		}
		return true
	}
	waitFor(t, 60*time.Second, settled, "every peer to hold only the renamed tree")
	for _, n := range c.nodes {
		if n.has(t, "Postvak In") {
			t.Fatalf("%s still has Postvak In: %v", n.Name, n.listDirs(t))
		}
	}
	if st, err := os.Stat(userEmpty); err != nil || !st.IsDir() {
		t.Fatalf("the user's empty folder was removed: %v", err)
	}
	// More rounds and reconcile passes change nothing.
	time.Sleep(2 * time.Second)
	if !settled() {
		t.Fatal("peers changed after converging")
	}
}

func without(list []string, drop string) []string {
	out := list[:0:0]
	for _, s := range list {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}

package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/search"
)

// gitRepo lays out a small git repository with its working tree, plus
// editor state, inside a node's root.
func gitRepo(t *testing.T, n *node) {
	t.Helper()
	n.write(t, "project/README.md", "# Project\n\nthe working tree readme about lighthouse keepers\n")
	n.write(t, "project/src/main.go", "package main\n")
	n.write(t, "project/.git/config", "[core]\n\trepositoryformatversion = 0\n# zanzibarquokka\n")
	n.write(t, "project/.git/HEAD", "ref: refs/heads/main\n")
	n.write(t, "project/.git/refs/heads/main", "0123456789abcdef0123456789abcdef01234567\n")
	n.write(t, "project/.git/objects/01/23456789abcdef", "blob zanzibarquokka\n")
	n.write(t, ".obsidian/workspace.json", `{"active": "zanzibarquokka"}`)
	n.write(t, ".hidden.txt", "zanzibarquokka dotfile\n")
}

var hiddenPaths = []string{
	"project/.git/config",
	"project/.git/HEAD",
	"project/.git/refs/heads/main",
	"project/.git/objects/01/23456789abcdef",
	".obsidian/workspace.json",
	".hidden.txt",
}

// LOC-60: a root that contains a git repository syncs the working tree and
// nothing under .git (nor any other hidden path); hidden files are neither
// recorded, offered to peers nor indexed. A root that opts in with
// sync_hidden syncs them like any other file.
func TestHiddenPathsAreNotSynced(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	gitRepo(t, a)

	waitFor(t, 30*time.Second, func() bool {
		return b.has(t, "project/README.md") && b.has(t, "project/src/main.go")
	}, "the working tree to reach the peer")

	// Edits under .git while running (a commit, a lock file) are ignored
	// too. A sentinel written afterwards proves rounds have carried on past
	// them.
	a.write(t, "project/.git/index.lock", "lock\n")
	a.write(t, "project/.git/config", "[core]\n\tbare = false\n# zanzibarquokka\n")
	a.write(t, "project/after.txt", "written after the .git edits\n")
	waitFor(t, 30*time.Second, func() bool { return b.has(t, "project/after.txt") }, "the sentinel to reach the peer")
	time.Sleep(2 * a.Cfg.Sync.Interval.D())

	for _, rel := range append(hiddenPaths, "project/.git/index.lock") {
		if b.has(t, rel) {
			t.Errorf("hidden path %s reached the peer", rel)
		}
	}
	if b.has(t, "project/.git") {
		t.Error("a .git directory was created on the peer")
	}
	for _, n := range c.nodes {
		docs, err := n.App.Store.ListDocs(n.Root, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range docs {
			if strings.Contains(d.RelPath, "/.") || strings.HasPrefix(d.RelPath, ".") {
				t.Errorf("%s recorded hidden path %s", n.Name, d.RelPath)
			}
		}
	}

	// Indexed: the working tree is searchable, nothing hidden is.
	waitFor(t, 30*time.Second, func() bool {
		return len(a.search(t, "lighthouse keepers", search.Options{Limit: 10})) > 0
	}, "the working tree to be indexed")
	for _, r := range a.search(t, "zanzibarquokka", search.Options{Limit: 10}) {
		t.Errorf("hidden content is searchable: %s", r.Path)
	}

	// The peer's own hidden files stay put and stay local as well.
	b.write(t, ".obsidian/workspace.json", `{"active": "other machine"}`)
	b.write(t, "from-b.txt", "sentinel from b\n")
	waitFor(t, 30*time.Second, func() bool { return a.has(t, "from-b.txt") }, "b's sentinel to reach a")
	time.Sleep(2 * a.Cfg.Sync.Interval.D())
	if got, _ := a.read(t, ".obsidian/workspace.json"); !strings.Contains(got, "zanzibarquokka") {
		t.Errorf("a's editor state was overwritten by b's: %q", got)
	}
	if got, _ := b.read(t, ".obsidian/workspace.json"); !strings.Contains(got, "other machine") {
		t.Errorf("b's editor state was overwritten by a's: %q", got)
	}
}

func TestSyncHiddenOptIn(t *testing.T) {
	c := newClusterWith(t, 2, nil, func(cfg *config.Config) {
		cfg.Roots[0].SyncHidden = true
	})
	a, b := c.nodes[0], c.nodes[1]
	gitRepo(t, a)
	waitFor(t, 30*time.Second, func() bool {
		for _, rel := range hiddenPaths {
			if !b.has(t, rel) {
				return false
			}
		}
		return true
	}, "hidden paths to reach a peer that syncs them")
}

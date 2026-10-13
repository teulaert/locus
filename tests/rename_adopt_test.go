package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// renameAcross writes n files under from on the first of peers nodes, waits
// until every peer holds them, renames the folder to to on the first peer's
// disk (the reconciler records it as a deletion and a creation per file),
// and waits until every peer holds only the renamed tree. It returns, for
// the rename alone, the file content served (pulls) and puts received in
// full across all peers, and the files each other peer adopted from its
// own copy.
func renameAcross(t *testing.T, peers, n int, from, to string) (servedBytes, received int64, adopted []int64) {
	t.Helper()
	c := newCluster(t, peers)
	c.dumpOnFailure()
	renamer := c.nodes[0]
	files := map[string]string{}
	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("sub%d/doc%03d.txt", i%5, i)
		files[rel] = fmt.Sprintf("document %d\n%s", i, strings.Repeat("rename detection\n", 500))
		renamer.write(t, from+"/"+rel, files[rel])
	}
	waitFor(t, 60*time.Second, func() bool {
		for _, nd := range c.nodes[1:] {
			if len(nd.listRel(t)) != n || !treesMatch(t, renamer, nd) {
				return false
			}
		}
		return true
	}, "every peer to hold the folder")
	// Let the version vectors settle.
	time.Sleep(2 * time.Second)

	counters := func() (served, recv int64, adopted []int64) {
		for _, nd := range c.nodes {
			_, b := nd.App.Sync.Served()
			served += b
			recv += nd.App.Sync.Received()
			a, _ := nd.App.Sync.Adopted()
			adopted = append(adopted, a)
		}
		return
	}
	served0, recv0, adopted0 := counters()

	if err := os.Rename(filepath.Join(renamer.Root, from), filepath.Join(renamer.Root, to)); err != nil {
		t.Fatal(err)
	}
	settled := func() bool {
		for _, nd := range c.nodes {
			if nd.has(t, from) || len(nd.listRel(t)) != n {
				return false
			}
			for rel, body := range files {
				if got, ok := nd.read(t, to+"/"+rel); !ok || got != body {
					return false
				}
			}
		}
		return true
	}
	waitFor(t, 60*time.Second, settled, "every peer to hold only the renamed folder")
	// More rounds change nothing.
	time.Sleep(2 * time.Second)
	if !settled() {
		t.Fatal("peers changed after converging")
	}
	for _, nd := range c.nodes {
		list, err := nd.App.Store.ListConflicts()
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 0 {
			t.Fatalf("%s recorded %d conflicts", nd.Name, len(list))
		}
	}
	served1, recv1, adopted1 := counters()
	for i := range adopted1 {
		adopted = append(adopted, adopted1[i]-adopted0[i])
	}
	var size int64
	for _, body := range files {
		size += int64(len(body))
	}
	t.Logf("rename of %d files (%d bytes) across %d peers: %d content bytes served, %d puts received in full, adopted per peer %v",
		n, size, peers, served1-served0, recv1-recv0, adopted)
	return served1 - served0, recv1 - recv0, adopted
}

// LOC-82: a folder renamed on one peer used to be transferred again in
// full to the other, as a deletion and a fresh file per path. The other
// peer adopts each file from the copy it holds under the old path before
// that path is deleted, and the old folder is pruned (LOC-74).
func TestRenamedFolderIsAdoptedNotTransferred(t *testing.T) {
	const n = 100
	served, received, adopted := renameAcross(t, 2, n, "Zakelijk", "Business")
	if served != 0 || received != 0 {
		t.Fatalf("the rename moved content: %d bytes served, %d puts received", served, received)
	}
	if adopted[0] != 0 || adopted[1] != n {
		t.Fatalf("adopted %v; want %d on the second peer", adopted, n)
	}
}

// Three peers: both peers that did not rename adopt every file locally,
// whether the rename reaches them by their own pull from the renamer, by
// the renamer's push, or by the other peer's, and nothing conflicts.
func TestThreePeerRenameIsAdoptedByBothOthers(t *testing.T) {
	const n = 100
	served, received, adopted := renameAcross(t, 3, n, "Prive", "Private")
	// Before LOC-82 this moved about twice the folder's content (every
	// file pulled or pushed to both other peers). Two files of slack keep
	// an unlucky interleaving of three peers' rounds from failing it.
	const slack = 2
	if received > slack || served > slack*9000 {
		t.Fatalf("the rename moved content: %d bytes served, %d puts received", served, received)
	}
	if adopted[0] != 0 || adopted[1] < n-slack || adopted[2] < n-slack {
		t.Fatalf("adopted %v; want %d on each other peer", adopted, n)
	}
}

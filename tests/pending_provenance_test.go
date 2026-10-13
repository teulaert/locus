package tests

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// Scenario 6 boundary — the crash window between the atomic rename and the
// state record. A file that only arrived from a peer must keep the remote
// author and version, because a document claimed to be authored here corrupts
// both the version vector and the deterministic conflict winner (ONO-30).
func TestReceivedFileKeepsRemoteAuthorAfterLostRecord(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	b.stopServing(t)

	const rel = "arrived.txt"
	body := "authored on the first peer\n"
	a.write(t, rel, body)
	// Let A finish rounds into the empty network so its own record is settled.
	time.Sleep(time.Second)

	// B is left exactly as a killed process leaves it: the file is on disk and
	// its provenance is recorded, but no document row exists.
	if err := os.WriteFile(filepath.Join(b.Root, rel), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(b.Dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	ver := vvector.New().Increment(a.PeerID)
	if err := st.PutPendingArrival(&store.PendingArrival{
		Root: b.Root, RelPath: rel, Hash: fswatch.HashBytes([]byte(body)), Version: ver.String(), Writer: a.PeerID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetDoc(b.Root, rel); err == nil {
		t.Fatal("test setup expected no document row on the receiving peer")
	}
	st.Close()

	b.restart(t)
	c.waitConnected()
	waitFor(t, 30*time.Second, func() bool { return treesMatch(t, a, b) }, "file to match on both peers")

	doc, err := b.App.Store.GetDoc(b.Root, rel)
	if err != nil {
		t.Fatalf("receiving peer did not adopt the file: %v", err)
	}
	if doc.LastWriter != a.PeerID {
		t.Fatalf("received file was attributed to %s, want the remote author", doc.LastWriter)
	}
	if doc.Version.String() != ver.String() {
		t.Fatalf("version = %s, want the remote %s", doc.Version.String(), ver.String())
	}
	if _, err := b.App.Store.GetPendingArrival(b.Root, rel); err != store.ErrNotFound {
		t.Fatalf("adopted provenance was not consumed: %v", err)
	}

	// Work done here is still ours: adoption must not swallow local authorship.
	// The edit is noticed by B's watcher and then pushed, so the records are
	// compared once they agree, not at the first moment the trees match.
	b.write(t, rel, "edited on the second peer\n")
	c.waitForConvergence()
	var remote *store.Doc
	waitFor(t, 30*time.Second, func() bool {
		var err error
		doc, err = b.App.Store.GetDoc(b.Root, rel)
		if err != nil || doc.LastWriter != b.PeerID {
			return false
		}
		remote, err = a.App.Store.GetDoc(a.Root, rel)
		return err == nil && remote.LastWriter == b.PeerID && remote.Version.String() == doc.Version.String()
	}, "the local edit's authorship to reach the other peer")
	if doc.LastWriter != b.PeerID {
		t.Fatalf("local edit attributed to %s", doc.LastWriter)
	}
	if remote.LastWriter != b.PeerID || remote.Version.String() != doc.Version.String() {
		t.Fatalf("authorship did not converge: remote %+v local %+v", remote, doc)
	}
}

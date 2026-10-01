package sync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/vvector"
)

// The applyDelete and applyConflict branches, one case each, driven through
// Apply on a real root and state database (LOC-25).

const branchRemote = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// dominates reports whether v is causally at or after every vector in of.
func dominates(v vvector.Vector, of ...vvector.Vector) bool {
	for _, o := range of {
		if c := vvector.Compare(v, o); c != vvector.After && c != vvector.Equal {
			return false
		}
	}
	return true
}

func TestApplyDeleteWithoutRecordLeavesTombstone(t *testing.T) {
	e := newEngineEnv(t)
	ver := vvector.New().Increment(branchRemote)
	res, err := e.Apply(e.root, "never-seen.txt", Meta{Deleted: true, Version: ver, LastWriter: branchRemote}, nil, branchRemote)
	if err != nil || res != ResultApplied {
		t.Fatalf("apply = %s, %v; want applied", res, err)
	}
	doc, err := e.Store.GetDoc(e.root, "never-seen.txt")
	if err != nil || !doc.Deleted || doc.Version.String() != ver.String() {
		t.Fatalf("want a tombstone at %s, got %+v, %v", ver, doc, err)
	}
	if _, err := os.Stat(filepath.Join(e.root, "never-seen.txt")); !os.IsNotExist(err) {
		t.Fatalf("a delete created a file: %v", err)
	}
}

func TestApplyDescendantDeleteRemovesFile(t *testing.T) {
	e := newEngineEnv(t)
	base := vvector.New().Increment(branchRemote)
	e.putFile(t, "f.txt", "to be deleted", base, branchRemote)
	next := base.Increment(branchRemote)
	res, err := e.Apply(e.root, "f.txt", Meta{Deleted: true, Version: next, LastWriter: branchRemote}, nil, branchRemote)
	if err != nil || res != ResultApplied {
		t.Fatalf("apply = %s, %v; want applied", res, err)
	}
	if _, err := os.Stat(filepath.Join(e.root, "f.txt")); !os.IsNotExist(err) {
		t.Fatalf("file survived a descendant delete: %v", err)
	}
	doc, err := e.Store.GetDoc(e.root, "f.txt")
	if err != nil || !doc.Deleted || doc.Version.String() != next.String() {
		t.Fatalf("want a tombstone at %s, got %+v, %v", next, doc, err)
	}
}

func TestApplyStaleDeleteIsNoop(t *testing.T) {
	e := newEngineEnv(t)
	old := vvector.New().Increment(branchRemote)
	newer := old.Increment(e.Identity.PeerID)
	e.putFile(t, "f.txt", "edited after the delete was issued", newer, e.Identity.PeerID)
	res, err := e.Apply(e.root, "f.txt", Meta{Deleted: true, Version: old, LastWriter: branchRemote}, nil, branchRemote)
	if err != nil || res != ResultNoop {
		t.Fatalf("apply = %s, %v; want noop", res, err)
	}
	if got, _ := readOrEmpty(t, filepath.Join(e.root, "f.txt")); got != "edited after the delete was issued" {
		t.Fatalf("stale delete touched the file: %q", got)
	}
	e.assertRecordMatchesDisk(t, "f.txt")
}

// A delete concurrent with a recorded local edit keeps the edit, records the
// conflict, and leaves a vector that dominates both sides so the delete does
// not fire again on the next round.
func TestApplyConcurrentDeleteKeepsRecordedEdit(t *testing.T) {
	e := newEngineEnv(t)
	base := vvector.New().Increment(branchRemote)
	local := base.Increment(e.Identity.PeerID)
	e.putFile(t, "f.txt", "local edit", local, e.Identity.PeerID)
	del := base.Increment(branchRemote)
	res, err := e.Apply(e.root, "f.txt", Meta{Deleted: true, Version: del, LastWriter: branchRemote, Hash: fswatch.HashBytes([]byte("ancestor"))}, nil, branchRemote)
	if err != nil || res != ResultConflicted {
		t.Fatalf("apply = %s, %v; want conflicted", res, err)
	}
	if got, _ := readOrEmpty(t, filepath.Join(e.root, "f.txt")); got != "local edit" {
		t.Fatalf("concurrent delete removed the edit: %q", got)
	}
	doc, err := e.Store.GetDoc(e.root, "f.txt")
	if err != nil || doc.Deleted {
		t.Fatalf("record after delete-vs-edit = %+v, %v; want the live edit", doc, err)
	}
	if !dominates(doc.Version, local, del) {
		t.Fatalf("merged version %s does not dominate %s and %s", doc.Version, local, del)
	}
	e.assertRecordMatchesDisk(t, "f.txt")
	list, _ := e.Store.ListConflicts()
	if len(list) != 1 || list[0].RelPath != "f.txt" {
		t.Fatalf("conflicts = %+v, want one for f.txt", list)
	}
}

// When the local side wins the normal path, the incoming version is stored
// as a sibling and the local file is left alone.
func TestApplyConflictIncomingLosesBecomesSibling(t *testing.T) {
	e := newEngineEnv(t)
	localBody, incomingBody := "divergent local text", "divergent incoming text"
	loses := func(local, incoming string) bool {
		return !IsIncomingWinner(Version{Writer: e.Identity.PeerID, Hash: fswatch.HashBytes([]byte(local))}, Version{Writer: branchRemote, Hash: fswatch.HashBytes([]byte(incoming))})
	}
	if !loses(localBody, incomingBody) {
		localBody, incomingBody = incomingBody, localBody
	}
	if !loses(localBody, incomingBody) {
		t.Fatal("test setup: the incoming side must lose")
	}
	base := vvector.New().Increment(branchRemote)
	local := base.Increment(e.Identity.PeerID)
	e.putFile(t, "notes.md", localBody, local, e.Identity.PeerID)
	meta := Meta{Hash: fswatch.HashBytes([]byte(incomingBody)), Size: int64(len(incomingBody)), Version: base.Increment(branchRemote), LastWriter: branchRemote}

	res, err := e.Apply(e.root, "notes.md", meta, strings.NewReader(incomingBody), branchRemote)
	if err != nil || res != ResultConflicted {
		t.Fatalf("apply = %s, %v; want conflicted", res, err)
	}
	if got, _ := readOrEmpty(t, filepath.Join(e.root, "notes.md")); got != localBody {
		t.Fatalf("normal path = %q, want the winning local %q", got, localBody)
	}
	list, _ := e.Store.ListConflicts()
	if len(list) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(list))
	}
	c := list[0]
	if !strings.HasSuffix(c.ConflictPath, ".md") || !strings.Contains(c.ConflictPath, ".conflict-") {
		t.Fatalf("sibling name %q does not follow name.conflict-<host>-<hash>.ext", c.ConflictPath)
	}
	if got, ok := readOrEmpty(t, filepath.Join(e.root, filepath.FromSlash(c.ConflictPath))); !ok || got != incomingBody {
		t.Fatalf("sibling %s = %q (exists=%v), want the incoming version", c.ConflictPath, got, ok)
	}
	if c.LocalHash != fswatch.HashBytes([]byte(localBody)) || c.ConflictHash != meta.Hash {
		t.Fatalf("conflict record does not describe both sides: %+v", c)
	}
	doc, err := e.Store.GetDoc(e.root, "notes.md")
	if err != nil {
		t.Fatal(err)
	}
	if !dominates(doc.Version, local, meta.Version) || vvector.Compare(doc.Version, meta.Version) == vvector.Equal {
		t.Fatalf("merged version %s must strictly dominate %s and %s", doc.Version, local, meta.Version)
	}
	e.assertRecordMatchesDisk(t, "notes.md")
	assertNoTempFiles(t, e.root)

	// The same push arriving again (a retried round) adds nothing.
	res, err = e.Apply(e.root, "notes.md", meta, strings.NewReader(incomingBody), branchRemote)
	if err != nil || res != ResultNoop {
		t.Fatalf("repeated push = %s, %v; want noop", res, err)
	}
	if list, _ := e.Store.ListConflicts(); len(list) != 1 {
		t.Fatalf("repeated push recorded %d conflicts, want 1", len(list))
	}
}

// Delete-vs-edit seen from the deleting peer: this peer recorded the delete,
// the other edited the same ancestor, and the edit arrives. The edit must
// survive here, on disk, with a record that describes it.
func TestApplyEditOverLocalTombstoneKeepsEdit(t *testing.T) {
	e := newEngineEnv(t)
	base := vvector.New().Increment(branchRemote)
	e.putFile(t, "f.txt", "ancestor", base, branchRemote)
	if err := os.Remove(filepath.Join(e.root, "f.txt")); err != nil {
		t.Fatal(err)
	}
	if err := e.record(e.root, "f.txt", fswatch.HashBytes([]byte("ancestor")), 0, base.Increment(e.Identity.PeerID), e.Identity.PeerID, true); err != nil {
		t.Fatal(err)
	}
	edit := "edited on the other peer"
	meta := Meta{Hash: fswatch.HashBytes([]byte(edit)), Size: int64(len(edit)), Version: base.Increment(branchRemote), LastWriter: branchRemote}
	res, err := e.Apply(e.root, "f.txt", meta, strings.NewReader(edit), branchRemote)
	if err != nil || res == ResultRejected {
		t.Fatalf("apply = %s, %v", res, err)
	}
	if _, ok := e.rootContents(t)[meta.Hash]; !ok {
		t.Fatal("the incoming edit was not installed over the local tombstone (LOC-51)")
	}
	e.assertRecordMatchesDisk(t, "f.txt")
	doc, err := e.Store.GetDoc(e.root, "f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Deleted || doc.LastWriter != branchRemote {
		t.Fatalf("record = deleted %v, writer %s; want the live edit by %s", doc.Deleted, doc.LastWriter, branchRemote)
	}
	// The recorded version must dominate both the tombstone and the edit,
	// so neither the old delete nor a re-sent edit can override it.
	tomb := base.Increment(e.Identity.PeerID)
	for name, v := range map[string]vvector.Vector{"tombstone": tomb, "edit": meta.Version} {
		if c := vvector.Compare(doc.Version, v); c != vvector.After && c != vvector.Equal {
			t.Fatalf("recorded version %s does not dominate the %s %s", doc.Version, name, v)
		}
	}
	// A later reconcile must not turn the edit back into a delete: the
	// stale tombstone, re-sent, is causally older and changes nothing.
	del := Meta{Hash: fswatch.HashBytes([]byte("ancestor")), Version: tomb, LastWriter: e.Identity.PeerID, Deleted: true}
	if res, err := e.Apply(e.root, "f.txt", del, nil, branchRemote); err != nil || res != ResultNoop {
		t.Fatalf("re-sent tombstone: apply = %s, %v; want noop", res, err)
	}
	if _, ok := e.rootContents(t)[meta.Hash]; !ok {
		t.Fatal("the re-sent tombstone removed the edit")
	}
}

// A descendant edit records the size Install measured, not the size the
// peer declared, and content without a hash to check it against is refused
// before anything is installed (LOC-30).
func TestApplyDescendantRecordsMeasuredSize(t *testing.T) {
	e := newEngineEnv(t)
	base := vvector.New().Increment(branchRemote)
	e.putFile(t, "f.txt", "first", base, branchRemote)
	next := base.Increment(branchRemote)
	body := "a longer second version"

	unhashed := Meta{Size: int64(len(body)), Version: next, LastWriter: branchRemote}
	if res, err := e.Apply(e.root, "f.txt", unhashed, strings.NewReader(body), branchRemote); err == nil || res != ResultRejected {
		t.Fatalf("apply without hash = %s, %v; want rejected", res, err)
	}
	if got, _ := readOrEmpty(t, filepath.Join(e.root, "f.txt")); got != "first" {
		t.Fatalf("unhashed content was installed: %q", got)
	}

	meta := Meta{Hash: fswatch.HashBytes([]byte(body)), Size: 1, Version: next, LastWriter: branchRemote}
	if res, err := e.Apply(e.root, "f.txt", meta, strings.NewReader(body), branchRemote); err != nil || res != ResultApplied {
		t.Fatalf("apply = %s, %v; want applied", res, err)
	}
	doc, err := e.Store.GetDoc(e.root, "f.txt")
	if err != nil || doc.Size != int64(len(body)) {
		t.Fatalf("want recorded size %d, got %+v, %v", len(body), doc, err)
	}
	e.assertRecordMatchesDisk(t, "f.txt")
}

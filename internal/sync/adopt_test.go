package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// renameLocally renames the folder from to to under e's root on disk and
// records what reconciliation would: a creation of each new path (a fresh
// version by e) and a tombstone of each old one.
func renameLocally(t *testing.T, e *engineEnv, from, to string) {
	t.Helper()
	docs, err := e.Store.ListDocs(e.root, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(e.root, from), filepath.Join(e.root, to)); err != nil {
		t.Fatal(err)
	}
	for _, d := range docs {
		rest, ok := strings.CutPrefix(d.RelPath, from+"/")
		if !ok {
			continue
		}
		if err := e.record(e.root, to+"/"+rest, d.Hash, d.Size, vvector.New().Increment(e.Identity.PeerID), e.Identity.PeerID, false); err != nil {
			t.Fatal(err)
		}
		if err := e.record(e.root, d.RelPath, d.Hash, 0, d.Version.Increment(e.Identity.PeerID), e.Identity.PeerID, true); err != nil {
			t.Fatal(err)
		}
	}
}

// renameFiles writes n files of about size bytes under dir in e's root,
// recorded as e's.
func renameFiles(t *testing.T, e *engineEnv, dir string, n, size int) map[string]string {
	t.Helper()
	files := map[string]string{}
	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("%s/sub%d/doc%03d.txt", dir, i%4, i)
		body := fmt.Sprintf("document %d\n%s", i, strings.Repeat("x", size+i))
		writeNested(t, e, rel, body)
		files[rel] = body
	}
	return files
}

// LOC-82: a folder renamed on one peer reaches the other as a tombstone
// per old path and a creation per new one. The other peer already holds
// every byte under the old paths, so it adopts the content from them, in
// the same round and before the deletions remove them, and fetches
// nothing; the old folder is pruned as any emptied one (LOC-74).
func TestRenamedFolderIsAdoptedNotFetched(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	const n = 100
	files := renameFiles(t, a, "Zakelijk", n, 1000)
	pa := servePeer(t, a)
	cleanRound(t, b, pa)
	b.assertHolds(t, files)
	servedBefore, bytesBefore := a.Served()
	if servedBefore != n {
		t.Fatalf("the first round served %d files", servedBefore)
	}

	renameLocally(t, a, "Zakelijk", "Business")
	var seen PeerProgress
	b.OnPeerDone = func(string) { seen = b.Progress()[0] }
	st, err := b.Round(context.Background(), []*peer.Entry{pa})
	if err != nil || len(st.Errors) != 0 {
		t.Fatalf("round: %+v, %v", st, err)
	}
	served, servedBytes := a.Served()
	if served != servedBefore || servedBytes != bytesBefore {
		t.Fatalf("the renamer served %d files (%d bytes) for the rename", served-servedBefore, servedBytes-bytesBefore)
	}
	var size int64
	renamed := map[string]string{}
	for rel, body := range files {
		renamed["Business/"+strings.TrimPrefix(rel, "Zakelijk/")] = body
		size += int64(len(body))
	}
	if st.Adopted != n || st.AdoptedBytes != size || st.Pulled != 0 || st.Deleted != n || st.Conflicted != 0 {
		t.Fatalf("stats %+v; want %d adopted, %d deleted", st, n, n)
	}
	if files, bytes := b.Adopted(); files != n || bytes != size {
		t.Fatalf("Adopted() = %d, %d", files, bytes)
	}
	r := seen.Roots[0]
	if r.Adopted != n || r.AdoptedBytes != size || r.Pulled != 0 || r.Failed != 0 || seen.Adopted != n || seen.AdoptedBytes != size || seen.PulledBytes != 0 || seen.AdoptedPushBytes != 0 {
		t.Fatalf("progress %+v", seen)
	}
	b.assertHolds(t, renamed)
	assertGone(t, filepath.Join(b.root, "Zakelijk"))
	for rel := range renamed {
		b.assertRecordMatchesDisk(t, rel)
		if got := links(t, filepath.Join(b.root, rel)); got != 1 {
			t.Fatalf("%s has %d links", rel, got)
		}
		ver, writer := b.docVersion(t, rel)
		if writer != a.Identity.PeerID || vvector.Compare(ver, vvector.New().Increment(a.Identity.PeerID)) != vvector.Equal {
			t.Fatalf("%s recorded as %s by %s; want the incoming version", rel, ver, writer)
		}
	}
	assertNoTempFiles(t, b.root)

	// Settled: nothing left to do either way.
	st, err = b.Round(context.Background(), []*peer.Entry{pa})
	if err != nil || st.Adopted+st.Pulled+st.Pushed+st.Deleted != 0 {
		t.Fatalf("second round %+v, %v", st, err)
	}
}

// A local copy whose bytes changed since they were recorded (an edit the
// reconciler has not seen yet) is not adopted: its content is verified
// again, and the file is fetched instead. The edited copy is left alone.
func TestAdoptionVerifiesTheLocalCopy(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	const body = "the content both peers hold\n"
	writeNested(t, a, "orig/file.txt", body)
	pa := servePeer(t, a)
	cleanRound(t, b, pa)

	// b edits its copy without the edit being recorded; a copies the
	// file to a new path.
	edited := filepath.Join(b.root, "orig/file.txt")
	if err := os.WriteFile(edited, []byte("edited on b, not yet recorded\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeNested(t, a, "copy/file.txt", body)
	served, _ := a.Served()
	st, err := b.Round(context.Background(), []*peer.Entry{pa})
	if err != nil || len(st.Errors) != 0 {
		t.Fatalf("round: %+v, %v", st, err)
	}
	if st.Adopted != 0 || st.Pulled != 1 {
		t.Fatalf("stats %+v; want the copy fetched", st)
	}
	if got, _ := a.Served(); got != served+1 {
		t.Fatalf("served %d files; want 1", got-served)
	}
	b.assertHolds(t, map[string]string{"copy/file.txt": body, "orig/file.txt": "edited on b, not yet recorded\n"})
	b.assertRecordMatchesDisk(t, "copy/file.txt")
	assertNoTempFiles(t, b.root)
}

// The same holds for a rename: an old file edited meanwhile is not the
// renamed content. The new path is fetched, and the edit survives the
// deletion as a conflict, as without adoption.
func TestRenameOfLocallyEditedFileFetchesIt(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	files := renameFiles(t, a, "old", 3, 1000)
	pa := servePeer(t, a)
	cleanRound(t, b, pa)
	const rel = "old/sub0/doc000.txt"
	if err := os.WriteFile(filepath.Join(b.root, rel), []byte("edited on b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	renameLocally(t, a, "old", "new")
	served, _ := a.Served()
	st, err := b.Round(context.Background(), []*peer.Entry{pa})
	if err != nil || len(st.Errors) != 0 {
		t.Fatalf("round: %+v, %v", st, err)
	}
	if st.Adopted != 2 || st.Pulled != 1 {
		t.Fatalf("stats %+v; want 2 adopted, 1 pulled", st)
	}
	if got, _ := a.Served(); got != served+1 {
		t.Fatalf("served %d files; want 1", got-served)
	}
	for r, body := range files {
		b.assertHolds(t, map[string]string{"new/" + strings.TrimPrefix(r, "old/"): body})
	}
	b.assertHolds(t, map[string]string{rel: "edited on b\n"})
	newRel := "new/sub0/doc000.txt"
	if got := links(t, filepath.Join(b.root, newRel)); got != 1 {
		t.Fatalf("%s has %d links", newRel, got)
	}
}

// Content held in another root is adopted from there.
func TestAdoptionFromAnotherRoot(t *testing.T) {
	a := newEngineEnv(t)
	b, private := twoRootEngine(t)
	const body = "a scan filed in both roots\n"
	if err := os.WriteFile(filepath.Join(private, "scan.txt"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := b.record(private, "scan.txt", fswatch.HashBytes([]byte(body)), int64(len(body)), vvector.New().Increment(b.Identity.PeerID), b.Identity.PeerID, false); err != nil {
		t.Fatal(err)
	}
	writeNested(t, a, "inbox/scan.txt", body)
	pa := servePeer(t, a)
	st, err := b.Round(context.Background(), []*peer.Entry{pa})
	if err != nil || len(st.Errors) != 0 {
		t.Fatalf("round: %+v, %v", st, err)
	}
	if st.Adopted != 1 || st.Pulled != 0 {
		t.Fatalf("stats %+v; want the file adopted", st)
	}
	if served, _ := a.Served(); served != 0 {
		t.Fatalf("served %d files", served)
	}
	b.assertHolds(t, map[string]string{"inbox/scan.txt": body})
	b.assertRecordMatchesDisk(t, "inbox/scan.txt")
	// A copy, not a link: both files stay, as two documents.
	if got := links(t, filepath.Join(private, "scan.txt")); got != 1 {
		t.Fatalf("the source has %d links", got)
	}
}

// A put of content the receiver holds under another path is answered
// without its body: the receiver adopts its own copy, and the pusher
// counts the push as adopted, neither pushed nor failed.
func TestPushOfContentHeldElsewhereSendsNoBody(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newDedupePeer(t, sender.Identity)
	content := bulk(256 << 10)
	if err := os.MkdirAll(filepath.Join(recv.root, "old"), 0o755); err != nil {
		t.Fatal(err)
	}
	recv.putFile(t, "old/held.txt", content, vvector.New().Increment(recv.Identity.PeerID), recv.Identity.PeerID)
	writeNested(t, sender, "new/held.txt", content)

	var seen PeerProgress
	sender.OnPeerDone = func(string) { seen = sender.Progress()[0] }
	st, err := sender.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil || len(st.Errors) != 0 {
		t.Fatalf("round: %+v, %v", st, err)
	}
	// Both ways: the receiver adopts the pushed file, and the sender the
	// one it pulls back, which it holds too.
	if st.Adopted != 2 || st.Pushed != 0 || st.Pulled != 0 || st.SkippedHeld != 0 || st.AdoptedBytes != 2*int64(len(content)) {
		t.Fatalf("stats %+v; want the push and the pull adopted", st)
	}
	if served, _ := recv.Served(); served != 0 {
		t.Fatalf("the receiver served %d files", served)
	}
	if got := recv.ln.total(); got > 32<<10 {
		t.Fatalf("the receiver read %d bytes for a put of %d bytes it held elsewhere", got, len(content))
	}
	if recv.Received() != 0 {
		t.Fatalf("received %d puts in full", recv.Received())
	}
	if files, _ := recv.Adopted(); files != 1 {
		t.Fatalf("receiver adopted %d files", files)
	}
	recv.assertHolds(t, map[string]string{"new/held.txt": content, "old/held.txt": content})
	recv.assertRecordMatchesDisk(t, "new/held.txt")
	ver, writer := recv.docVersion(t, "new/held.txt")
	if writer != sender.Identity.PeerID || vvector.Compare(ver, vvector.New().Increment(sender.Identity.PeerID)) != vvector.Equal {
		t.Fatalf("receiver records %s by %s", ver, writer)
	}
	if seen.Adopted != 2 || seen.AdoptedPushBytes != int64(len(content)) || seen.PushedBytes != 0 || seen.PulledBytes != 0 || seen.Roots[0].Pushed != 0 {
		t.Fatalf("progress %+v", seen)
	}
	if n := recv.claimsInFlight(); n != 0 {
		t.Fatalf("%d claims left on the receiver", n)
	}
}

// A folder renamed on the pushing side: the creations are pushed before
// the deletions, so the receiver adopts each file from the old path it is
// about to delete, and reads no file content at all.
func TestPushedRenameIsAdoptedByReceiver(t *testing.T) {
	sender := newEngineEnv(t)
	recv := newDedupePeer(t, sender.Identity)
	const n = 40
	files := renameFiles(t, sender, "Prive", n, 16<<10)
	cleanRound(t, sender, entryAt(recv.Identity.PeerID, recv.srv))
	recv.assertHolds(t, files)

	renameLocally(t, sender, "Prive", "Private")
	before := recv.ln.total()
	st, err := sender.Round(context.Background(), []*peer.Entry{entryAt(recv.Identity.PeerID, recv.srv)})
	if err != nil || len(st.Errors) != 0 {
		t.Fatalf("round: %+v, %v", st, err)
	}
	if st.Adopted != n || st.Pushed != 0 || st.Deleted != n {
		t.Fatalf("stats %+v; want %d adopted and deleted", st, n)
	}
	var size int64
	for _, body := range files {
		size += int64(len(body))
	}
	// Requests and replies only: well under the content's size.
	t.Logf("the receiver read %d bytes for %d files (%d bytes) renamed", recv.ln.total()-before, n, size)
	if got := recv.ln.total() - before; got >= size/2 {
		t.Fatalf("the receiver read %d bytes for %d bytes of renamed content", got, size)
	}
	for rel, body := range files {
		recv.assertHolds(t, map[string]string{"Private/" + strings.TrimPrefix(rel, "Prive/"): body})
	}
	assertGone(t, filepath.Join(recv.root, "Prive"))
}

// Schedule runs each pull or push whose content a deletion in the same
// plan removes right before that deletion, naming the file to adopt from;
// one pull per deleted file may link it. Everything else keeps its order.
func TestScheduleAdoptsRenamesBeforeDeleting(t *testing.T) {
	ver := vvector.New().Increment("aaaa").String()
	newer := vvector.New().Increment("aaaa").Increment("aaaa").String()
	local := []ManifestEntry{
		{RelPath: "Old/a.txt", Size: 10, Hash: "ha", Version: ver},
		{RelPath: "Old/b.txt", Size: 20, Hash: "hb", Version: ver},
		{RelPath: "gone.txt", Size: 5, Hash: "hg", Version: ver},
		{RelPath: "Mine/c.txt", Size: 30, Hash: "hc", Version: ver, Deleted: true},
		{RelPath: "Mine2/c.txt", Size: 30, Hash: "hc", Version: ver},
	}
	remote := []ManifestEntry{
		{RelPath: "Old/a.txt", Hash: "ha", Version: newer, Deleted: true},
		{RelPath: "Old/b.txt", Hash: "hb", Version: newer, Deleted: true},
		{RelPath: "gone.txt", Hash: "hg", Version: newer, Deleted: true},
		{RelPath: "Mine/c.txt", Size: 30, Hash: "hc", Version: vvector.New().String()},
		{RelPath: "New/a.txt", Size: 10, Hash: "ha", Version: ver},
		{RelPath: "New/b.txt", Size: 20, Hash: "hb", Version: ver},
		{RelPath: "Copy/a.txt", Size: 10, Hash: "ha", Version: ver},
		{RelPath: "big.bin", Size: 1 << 20, Hash: "hz", Version: ver},
	}
	var got []string
	for _, a := range Schedule(Plan(local, remote)) {
		s := string(a.Kind) + ":" + a.RelPath
		if a.AdoptFrom != "" {
			s += "<" + a.AdoptFrom
		}
		if a.AdoptLink {
			s += "(link)"
		}
		if a.LinkedTo != "" {
			s += ">" + a.LinkedTo
		}
		got = append(got, s)
	}
	want := []string{
		"pull_create:New/a.txt<Old/a.txt(link)", "pull_create:Copy/a.txt<Old/a.txt", "pull_delete:Old/a.txt>New/a.txt",
		"pull_create:New/b.txt<Old/b.txt(link)", "pull_delete:Old/b.txt>New/b.txt",
		"push_create:Mine2/c.txt", "push_delete:Mine/c.txt",
		"pull_delete:gone.txt", "pull_create:big.bin",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("schedule\n %v\nwant\n %v", got, want)
	}
}

// A file adopted by hard link that still shares its inode with the source
// after the deletion (the deletion kept a local edit) is given a copy of
// its own.
func TestUnshareLinkSeparatesFiles(t *testing.T) {
	e := newEngineEnv(t)
	writeNested(t, e, "a/one.txt", "shared bytes\n")
	if err := os.MkdirAll(filepath.Join(e.root, "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(e.root, "a/one.txt"), filepath.Join(e.root, "b/one.txt")); err != nil {
		t.Skip("no hard links here:", err)
	}
	e.unshareLink(e.root, "a/one.txt", "b/one.txt")
	fa, _ := os.Stat(filepath.Join(e.root, "a/one.txt"))
	fb, _ := os.Stat(filepath.Join(e.root, "b/one.txt"))
	if os.SameFile(fa, fb) {
		t.Fatal("the two paths still share a file")
	}
	e.assertHolds(t, map[string]string{"a/one.txt": "shared bytes\n", "b/one.txt": "shared bytes\n"})
}

// A rename's deletion waits while the renamed content is not in place:
// here another round holds the claim on the new path (it may be adopting
// from the old file right now), so this round neither fetches it nor
// removes the old file under it. The next round adopts and deletes.
func TestRenameDeletionWaitsForItsContent(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	files := renameFiles(t, a, "old", 1, 1000)
	pa := servePeer(t, a)
	cleanRound(t, b, pa)
	renameLocally(t, a, "old", "new")
	var rel, body string
	for r, c := range files {
		rel, body = r, c
	}
	newRel := "new/" + strings.TrimPrefix(rel, "old/")
	release, ok := b.claimPull(b.root, newRel, fswatch.HashBytes([]byte(body)), "cccc")
	if !ok {
		t.Fatal("claim")
	}
	st, err := b.Round(context.Background(), []*peer.Entry{pa})
	if err != nil || len(st.Errors) != 0 || st.SkippedInFlight != 1 || st.Deleted != 0 {
		t.Fatalf("round with the claim held: %+v, %v", st, err)
	}
	b.assertHolds(t, map[string]string{rel: body})
	release()
	st, err = b.Round(context.Background(), []*peer.Entry{pa})
	if err != nil || len(st.Errors) != 0 || st.Adopted != 1 || st.Deleted != 1 {
		t.Fatalf("next round: %+v, %v", st, err)
	}
	b.assertHolds(t, map[string]string{newRel: body})
	assertGone(t, filepath.Join(b.root, "old"))
	if served, _ := a.Served(); served != 1 {
		t.Fatalf("served %d files; want only the first sync's", served)
	}
}

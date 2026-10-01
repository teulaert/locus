package sync

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	gosync "sync"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/vvector"
)

// rootContents maps the hash of every file under the root to its path, so a
// test can ask whether a version survived anywhere: at its path or as a
// sibling.
func (e *engineEnv) rootContents(t *testing.T) map[string]string {
	t.Helper()
	entries, err := fswatch.Scan(e.root, fswatch.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string, len(entries))
	for _, en := range entries {
		out[en.Hash] = en.RelPath
	}
	return out
}

// assertRecordMatchesDisk fails if the record for rel describes content
// other than the file actually at rel.
func (e *engineEnv) assertRecordMatchesDisk(t *testing.T, rel string) {
	t.Helper()
	doc, err := e.Store.GetDoc(e.root, rel)
	if err != nil {
		t.Fatalf("%s has no record: %v", rel, err)
	}
	hash, _, err := fswatch.HashFile(filepath.Join(e.root, rel))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Hash != hash {
		t.Fatalf("record for %s says %.8s, disk holds %.8s", rel, doc.Hash, hash)
	}
}

// A local edit that lands after Apply read the record but before it wrote
// (the watcher's debounce has not let the reconciler record it yet) is a
// version concurrent with the incoming one. It must be kept as a conflict,
// never renamed over or removed (LOC-7).
func TestApplyPreservesEditMadeDuringApply(t *testing.T) {
	const remote = "ffffffffffffffffffffffffffffffff"
	const incoming, edit = "incoming from peer", "edited locally just now"
	cases := []struct {
		name     string
		recorded bool // a record exists before the incoming operation
		deleted  bool // the incoming operation is a deletion
	}{
		{name: "descendant update", recorded: true},
		{name: "new path", recorded: false},
		{name: "descendant delete", recorded: true, deleted: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngineEnv(t)
			base := vvector.New().Increment(remote)
			if tc.recorded {
				e.putFile(t, "f.txt", "common ancestor", base, remote)
			}
			e.afterRead = func(root, rel string) {
				if err := os.WriteFile(filepath.Join(root, rel), []byte(edit), 0o644); err != nil {
					t.Error(err)
				}
			}
			meta := Meta{
				Hash:       fswatch.HashBytes([]byte(incoming)),
				Size:       int64(len(incoming)),
				Version:    base.Increment(remote),
				LastWriter: remote,
				Deleted:    tc.deleted,
			}
			var body io.Reader
			if !tc.deleted {
				body = strings.NewReader(incoming)
			}
			res, err := e.Apply(e.root, "f.txt", meta, body, remote)
			if err != nil || res != ResultConflicted {
				t.Fatalf("apply = %s, %v; want conflicted", res, err)
			}
			got := e.rootContents(t)
			if _, ok := got[fswatch.HashBytes([]byte(edit))]; !ok {
				t.Fatalf("the local edit was lost; files: %v", got)
			}
			if _, ok := got[meta.Hash]; !ok && !tc.deleted {
				t.Fatalf("the incoming version was lost; files: %v", got)
			}
			e.assertRecordMatchesDisk(t, "f.txt")
			if list, _ := e.Store.ListConflicts(); len(list) != 1 {
				t.Fatalf("conflicts = %d, want 1", len(list))
			}
		})
	}
}

// PullVersion must not adopt a vector for content the file no longer holds.
func TestPullVersionSkipsUnrecordedEdit(t *testing.T) {
	e := newEngineEnv(t)
	const pa = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	e.putFile(t, "f.txt", "same", vvector.Vector{pa: 1}, pa)
	remote := e.manifestEntry(t, "f.txt")
	remote.Version = vvector.Vector{pa: 2}.String()
	if err := os.WriteFile(filepath.Join(e.root, "f.txt"), []byte("edited"), 0o644); err != nil {
		t.Fatal(err)
	}
	if res, err := e.PullVersion(e.root, "f.txt", &remote); err != nil || res != ResultNoop {
		t.Fatalf("pull version = %s, %v; want noop", res, err)
	}
	if got := e.manifestEntry(t, "f.txt").Version; got != (vvector.Vector{pa: 1}).String() {
		t.Fatalf("version = %s, want it untouched", got)
	}
}

// Server handlers and the local round apply operations concurrently. Many
// divergent versions of one path arriving at once must all survive, and the
// record must describe the file at the path: without the per-path lock two
// Applies both see "no record", both install, and the last record wins.
func TestApplyConcurrentDivergentVersionsAllSurvive(t *testing.T) {
	e := newEngineEnv(t)
	const writers = 6
	for round := range 10 {
		rel := fmt.Sprintf("doc-%d.txt", round)
		bodies := make([]string, writers)
		var wg gosync.WaitGroup
		start := make(chan struct{})
		for i := range writers {
			peerID := fmt.Sprintf("%032x", i+1)
			bodies[i] = fmt.Sprintf("version from %s in round %d", peerID, round)
			meta := Meta{
				Hash:       fswatch.HashBytes([]byte(bodies[i])),
				Size:       int64(len(bodies[i])),
				Version:    vvector.New().Increment(peerID),
				LastWriter: peerID,
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := e.Apply(e.root, rel, meta, strings.NewReader(bodies[i]), peerID); err != nil {
					t.Error(err)
				}
			}()
		}
		// PullVersion reads and writes the same record; it must queue
		// behind Apply rather than overwrite what Apply recorded.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range writers {
				doc, err := e.Store.GetDoc(e.root, rel)
				if err != nil {
					continue
				}
				me := ManifestEntry{RelPath: rel, Hash: doc.Hash, Size: doc.Size, Version: doc.Version.Increment(fmt.Sprintf("%032x", 99)).String(), LastWriter: doc.LastWriter}
				if _, err := e.PullVersion(e.root, rel, &me); err != nil {
					t.Error(err)
				}
			}
		}()
		close(start)
		wg.Wait()
		got := e.rootContents(t)
		for i, b := range bodies {
			if _, ok := got[fswatch.HashBytes([]byte(b))]; !ok {
				t.Fatalf("round %d: version %d vanished; files: %v", round, i, got)
			}
		}
		e.assertRecordMatchesDisk(t, rel)
	}
}

// A symlink inside the root that points at a file outside it must never be
// read as the local side of a conflict: the sibling would otherwise carry
// the outside file's content into the root, and on to every peer (LOC-49).
// Push reads the same way and must refuse too.
func TestConflictSiblingIgnoresSymlinkOutsideRoot(t *testing.T) {
	e := newEngineEnv(t)
	const local, remote = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ffffffffffffffffffffffffffffffff"
	const secret, incoming = "outside the root", "incoming from peer"
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	e.putFile(t, "f.txt", "recorded before the link", vvector.New().Increment(local), local)
	link := filepath.Join(e.root, "f.txt")
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	// The incoming version is concurrent with the recorded one, so Apply
	// would preserve the local side as a sibling.
	meta := Meta{
		Hash:       fswatch.HashBytes([]byte(incoming)),
		Size:       int64(len(incoming)),
		Version:    vvector.New().Increment(remote),
		LastWriter: remote,
	}
	if res, err := e.Apply(e.root, "f.txt", meta, strings.NewReader(incoming), remote); err == nil {
		t.Fatalf("apply through an escaping symlink = %s, want an error", res)
	}
	// copyToSibling on its own must refuse as well, whatever its caller
	// checked first.
	if err := e.copyToSibling(e.root, "f.txt", "f (conflict).txt"); err == nil {
		t.Fatal("copyToSibling followed a symlink out of the root")
	}
	if _, err := e.Push(t.Context(), "http://127.0.0.1:1", e.root, "f.txt", &ManifestEntry{RelPath: "f.txt"}); err == nil {
		t.Fatal("push read a file through a symlink out of the root")
	}

	if _, ok := e.rootContents(t)[fswatch.HashBytes([]byte(secret))]; ok {
		t.Fatalf("the outside file was copied into the root: %v", e.rootContents(t))
	}
	if b, err := os.ReadFile(outside); err != nil || string(b) != secret {
		t.Fatalf("outside file = %q, %v; want it untouched", b, err)
	}
	if target, err := os.Readlink(link); err != nil || target != outside {
		t.Fatalf("link = %q, %v; want it left pointing where it did", target, err)
	}
}

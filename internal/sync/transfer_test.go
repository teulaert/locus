package sync

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/vvector"
)

// withInstallSync replaces the install flushes for one test. Tests using it
// must not run in parallel: the hooks are package state.
func withInstallSync(t *testing.T, d durability) {
	t.Helper()
	old := installSync
	installSync = d
	t.Cleanup(func() { installSync = old })
}

// assertNoTempFiles fails if an install left a .locus-tmp-* file behind.
func assertNoTempFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(d.Name(), fswatch.TempPrefix) {
			t.Errorf("temporary file left behind: %s", p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func readOrEmpty(t *testing.T, path string) (string, bool) {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b), true
}

// A rename is atomic for the name but not for the data: after a machine
// crash an unflushed file can be truncated while state.db records the
// verified hash. Apply must flush the file before the rename, flush the
// directory after it, and record the hash only once both have succeeded
// (LOC-16).
func TestApplyRecordsHashOnlyAfterFlush(t *testing.T) {
	const remote = "ffffffffffffffffffffffffffffffff"
	const old, incoming = "common ancestor", "incoming from peer"
	for _, recorded := range []bool{false, true} {
		name := map[bool]string{false: "new path", true: "descendant update"}[recorded]
		t.Run(name, func(t *testing.T) {
			e := newEngineEnv(t)
			base := vvector.New().Increment(remote)
			if recorded {
				e.putFile(t, "f.txt", old, base, remote)
			}
			want := fswatch.HashBytes([]byte(incoming))
			full := filepath.Join(e.root, "f.txt")
			recordedHash := func() string {
				doc, err := e.Store.GetDoc(e.root, "f.txt")
				if err != nil {
					return ""
				}
				return doc.Hash
			}
			var steps []string
			withInstallSync(t, durability{
				file: func(f *os.File) error {
					steps = append(steps, "file")
					if recordedHash() == want {
						t.Error("hash recorded before the file was flushed")
					}
					if got, _ := readOrEmpty(t, full); got == incoming {
						t.Error("file renamed into place before it was flushed")
					}
					return f.Sync()
				},
				dir: func(dir string) error {
					steps = append(steps, "dir")
					if recordedHash() == want {
						t.Error("hash recorded before the directory was flushed")
					}
					if got, _ := readOrEmpty(t, full); got != incoming {
						t.Errorf("directory flushed before the rename; path holds %q", got)
					}
					return syncDir(dir)
				},
			})
			meta := Meta{Hash: want, Size: int64(len(incoming)), Version: base.Increment(remote), LastWriter: remote}
			res, err := e.Apply(e.root, "f.txt", meta, strings.NewReader(incoming), remote)
			if err != nil || res != ResultApplied {
				t.Fatalf("apply = %s, %v; want applied", res, err)
			}
			if strings.Join(steps, ",") != "file,dir" {
				t.Fatalf("flushes = %v, want [file dir]", steps)
			}
			if recordedHash() != want {
				t.Fatal("hash not recorded after a successful install")
			}
			e.assertRecordMatchesDisk(t, "f.txt")
			assertNoTempFiles(t, e.root)
		})
	}
}

// A flush that fails means the bytes may not be on disk: Apply must not
// record the hash, must not leave a partial file at the path, and must clean
// up its temporary file (LOC-16).
func TestApplyFailedFlushRecordsNothing(t *testing.T) {
	const remote = "ffffffffffffffffffffffffffffffff"
	const old, incoming = "common ancestor", "incoming from peer"
	diskErr := errors.New("simulated I/O error")
	cases := []struct {
		name      string
		recorded  bool
		failFile  bool // fail the file flush (before rename) or the directory flush (after)
		wantAtRel string
		wantExist bool
	}{
		{name: "file flush, new path", failFile: true, wantExist: false},
		{name: "file flush, update", recorded: true, failFile: true, wantAtRel: old, wantExist: true},
		// After the rename the path holds the complete, flushed file, but
		// the directory entry may not survive a crash; still no record.
		{name: "dir flush, new path", wantAtRel: incoming, wantExist: true},
		{name: "dir flush, update", recorded: true, wantAtRel: incoming, wantExist: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngineEnv(t)
			base := vvector.New().Increment(remote)
			if tc.recorded {
				e.putFile(t, "f.txt", old, base, remote)
			}
			d := durability{file: (*os.File).Sync, dir: syncDir}
			if tc.failFile {
				d.file = func(*os.File) error { return diskErr }
			} else {
				d.dir = func(string) error { return diskErr }
			}
			withInstallSync(t, d)
			meta := Meta{
				Hash:       fswatch.HashBytes([]byte(incoming)),
				Size:       int64(len(incoming)),
				Version:    base.Increment(remote),
				LastWriter: remote,
			}
			res, err := e.Apply(e.root, "f.txt", meta, strings.NewReader(incoming), remote)
			if !errors.Is(err, diskErr) || res != ResultRejected {
				t.Fatalf("apply = %s, %v; want rejected with the flush error", res, err)
			}
			doc, gerr := e.Store.GetDoc(e.root, "f.txt")
			switch {
			case tc.recorded && (gerr != nil || doc.Hash != fswatch.HashBytes([]byte(old))):
				t.Fatalf("record changed despite the failed flush: %+v, %v", doc, gerr)
			case !tc.recorded && gerr == nil:
				t.Fatalf("record created despite the failed flush: %+v", doc)
			}
			got, exists := readOrEmpty(t, filepath.Join(e.root, "f.txt"))
			if exists != tc.wantExist || got != tc.wantAtRel {
				t.Fatalf("path holds %q (exists=%v), want %q (exists=%v)", got, exists, tc.wantAtRel, tc.wantExist)
			}
			assertNoTempFiles(t, e.root)
		})
	}
}

// Conflict siblings go through the same install: a failed flush must not
// leave a sibling or a conflict record behind.
func TestApplyConflictSiblingFlushFailure(t *testing.T) {
	const remote = "ffffffffffffffffffffffffffffffff"
	e := newEngineEnv(t)
	local := e.Identity.PeerID
	e.putFile(t, "f.txt", "local edit", vvector.New().Increment(local), local)
	withInstallSync(t, durability{
		file: func(*os.File) error { return errors.New("simulated I/O error") },
		dir:  syncDir,
	})
	incoming := "concurrent remote edit"
	meta := Meta{
		Hash:       fswatch.HashBytes([]byte(incoming)),
		Size:       int64(len(incoming)),
		Version:    vvector.New().Increment(remote),
		LastWriter: remote,
	}
	if res, err := e.Apply(e.root, "f.txt", meta, strings.NewReader(incoming), remote); err == nil {
		t.Fatalf("apply = %s, nil; want the flush error", res)
	}
	if got := e.rootContents(t); len(got) != 1 {
		t.Fatalf("files after failed conflict install: %v, want only f.txt", got)
	}
	e.assertRecordMatchesDisk(t, "f.txt")
	if list, _ := e.Store.ListConflicts(); len(list) != 0 {
		t.Fatalf("conflicts = %d, want 0", len(list))
	}
	assertNoTempFiles(t, e.root)
}

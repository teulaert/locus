package sync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/vvector"
)

// servePeer runs e's sync server and returns the peer entry other engines
// round against.
func servePeer(t *testing.T, e *engineEnv) *peer.Entry {
	t.Helper()
	s := &Server{Engine: e.Engine, Identity: e.Identity, Roots: []string{e.root}, Known: func(string) bool { return true }}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &peer.Entry{Info: peer.Info{PeerID: e.Identity.PeerID, SyncAddr: strings.TrimPrefix(srv.URL, "http://")}, Reachable: true}
}

func cleanRound(t *testing.T, e *engineEnv, to *peer.Entry) {
	t.Helper()
	if st, err := e.Round(context.Background(), []*peer.Entry{to}); err != nil || len(st.Errors) != 0 {
		t.Fatalf("round: %+v, %v", st, err)
	}
}

// writeNested writes rel (with its directories) and records it as authored
// by e at version 1.
func writeNested(t *testing.T, e *engineEnv, rel, body string) vvector.Vector {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(e.root, rel)), 0o755); err != nil {
		t.Fatal(err)
	}
	ver := vvector.New().Increment(e.Identity.PeerID)
	e.putFile(t, rel, body, ver, e.Identity.PeerID)
	return ver
}

// deleteLocally removes rel and records the tombstone the reconciler would.
func deleteLocally(t *testing.T, e *engineEnv, rel, body string, ver vvector.Vector) {
	t.Helper()
	if err := os.Remove(filepath.Join(e.root, rel)); err != nil {
		t.Fatal(err)
	}
	if err := e.record(e.root, rel, fswatch.HashBytes([]byte(body)), 0, ver.Increment(e.Identity.PeerID), e.Identity.PeerID, true); err != nil {
		t.Fatal(err)
	}
}

func assertGone(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("%s still exists (%v)", path, err)
	}
}

func assertDir(t *testing.T, path string) {
	t.Helper()
	if st, err := os.Lstat(path); err != nil || !st.IsDir() {
		t.Fatalf("%s is not a directory: %v", path, err)
	}
}

// A pulled deletion that empties a directory removes it and every parent it
// leaves empty, up to but never including the root (LOC-74).
func TestPulledDeletePrunesEmptiedDirectories(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	var logs bytes.Buffer
	b.Log = slog.New(slog.NewTextHandler(&logs, nil))
	const rel, body = "a/b/c/file.txt", "nested"
	ver := writeNested(t, a, rel, body)
	pa := servePeer(t, a)

	cleanRound(t, b, pa)
	if got, err := os.ReadFile(filepath.Join(b.root, rel)); err != nil || string(got) != body {
		t.Fatalf("b did not pull %s: %q, %v", rel, got, err)
	}

	deleteLocally(t, a, rel, body, ver)
	cleanRound(t, b, pa)
	assertGone(t, filepath.Join(b.root, "a"))
	assertDir(t, b.root)
	for _, dir := range []string{"a/b/c", "a/b", "a"} {
		if !strings.Contains(logs.String(), "path="+dir+"\n") {
			t.Fatalf("pruning %s was not logged:\n%s", dir, logs.String())
		}
	}
}

// A deletion pushed to a peer prunes on the receiving side too.
func TestPushedDeletePrunesEmptiedDirectories(t *testing.T) {
	a, b := newEngineEnv(t), newEngineEnv(t)
	const rel, body = "Postvak In/Archief/mail.eml", "message"
	ver := writeNested(t, a, rel, body)
	pb := servePeer(t, b)

	cleanRound(t, a, pb)
	if _, err := os.Stat(filepath.Join(b.root, rel)); err != nil {
		t.Fatalf("a did not push %s: %v", rel, err)
	}
	deleteLocally(t, a, rel, body, ver)
	cleanRound(t, a, pb)
	assertGone(t, filepath.Join(b.root, "Postvak In"))
	assertDir(t, b.root)
}

// Pruning stops at the first directory that still has an entry: a hidden
// file the root does not sync keeps its directory, and a sibling keeps the
// parent. No file is touched.
func TestApplyDeleteKeepsDirectoriesThatStillHaveEntries(t *testing.T) {
	e := newEngineEnv(t)
	base := vvector.New().Increment(branchRemote)
	for _, rel := range []string{"keep/hidden/doc.txt", "keep/sibling/doc.txt", "keep/sibling/other.txt"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(e.root, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		e.putFile(t, rel, "body of "+rel, base, branchRemote)
	}
	if err := os.WriteFile(filepath.Join(e.root, "keep/hidden/.DS_Store"), []byte("finder"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"keep/hidden/doc.txt", "keep/sibling/doc.txt"} {
		res, err := e.Apply(e.root, rel, Meta{Deleted: true, Version: base.Increment(branchRemote), LastWriter: branchRemote}, nil, branchRemote)
		if err != nil || res != ResultApplied {
			t.Fatalf("delete %s = %s, %v", rel, res, err)
		}
	}
	if _, err := os.Stat(filepath.Join(e.root, "keep/hidden/.DS_Store")); err != nil {
		t.Fatalf("hidden file lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(e.root, "keep/sibling/other.txt")); err != nil {
		t.Fatalf("sibling file lost: %v", err)
	}
	assertDir(t, filepath.Join(e.root, "keep/hidden"))
	assertDir(t, filepath.Join(e.root, "keep/sibling"))
}

// A deletion at the top level never removes the root, even when it was the
// root's last entry.
func TestApplyDeleteNeverRemovesRoot(t *testing.T) {
	e := newEngineEnv(t)
	base := vvector.New().Increment(branchRemote)
	e.putFile(t, "only.txt", "last file", base, branchRemote)
	res, err := e.Apply(e.root, "only.txt", Meta{Deleted: true, Version: base.Increment(branchRemote), LastWriter: branchRemote}, nil, branchRemote)
	if err != nil || res != ResultApplied {
		t.Fatalf("delete = %s, %v", res, err)
	}
	assertDir(t, e.root)
}

// A directory an install is writing into holds the install's temporary
// file, so a prune racing the transfer cannot remove it.
func TestPruneCannotRemoveDirectoryDuringInstall(t *testing.T) {
	root := t.TempDir()
	body := "arrives while a prune runs"
	r := &pruneOnRead{Reader: strings.NewReader(body), root: root, dir: "x/y"}
	if _, _, err := Install(root, "x/y/f.txt", r, fswatch.HashBytes([]byte(body)), 0); err != nil {
		t.Fatalf("install: %v", err)
	}
	if !r.tried {
		t.Fatal("the prune never ran")
	}
	if got, err := os.ReadFile(filepath.Join(root, "x/y/f.txt")); err != nil || string(got) != body {
		t.Fatalf("installed %q, %v", got, err)
	}
}

// pruneOnRead tries to prune dir while the install streams its content.
type pruneOnRead struct {
	*strings.Reader
	root, dir string
	tried     bool
}

func (p *pruneOnRead) Read(b []byte) (int, error) {
	if !p.tried {
		p.tried = true
		if ok, err := fswatch.RemoveEmptyDir(p.root, p.dir); ok || err != nil {
			return 0, fmt.Errorf("prune during install: removed=%v err=%v", ok, err)
		}
	}
	return p.Reader.Read(b)
}

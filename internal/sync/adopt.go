package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/pathguard"
	"github.com/teulaert/locus/internal/store"
)

// Adoption of content already held locally (LOC-82).
//
// A folder renamed or moved on one peer reaches every other peer as a
// deletion of each old path and a creation of each new one. Those peers
// hold every byte of the new files already, under the old paths, and used
// to fetch them all again. An incoming file (a pull, or a put another peer
// pushes) whose content hash this peer holds live under another path is
// therefore produced from that local file instead: the bytes are staged
// next to the destination from the local copy, hashed again (the copy may
// have changed since it was recorded), and handed to Apply exactly as
// received content would be, so versions, conflicts and the path lock
// behave as for any transfer. Only when no local copy verifies is the
// content transferred.

// maxAdoptCandidates bounds the local copies tried for one file. Each
// failed one costs a read of the file; more than a few copies of the same
// content that all fail to verify mean the records are stale anyway.
const maxAdoptCandidates = 4

// adoptSource is a local file that may hold the wanted content.
type adoptSource struct {
	root, relpath string
	// link allows a hard link instead of a copy (see Action.AdoptLink).
	link bool
}

// adoptSources lists the local files recorded as holding hash, other than
// root/relpath itself: from (a file the plan is about to delete) first,
// then the rest of the same root, then other configured roots.
func (e *Engine) adoptSources(root, relpath, hash, from string, link bool) ([]adoptSource, error) {
	var out []adoptSource
	if from != "" && from != relpath {
		out = append(out, adoptSource{root: root, relpath: from, link: link})
	}
	docs, err := e.Store.FindLiveByHash(hash)
	if err != nil {
		return out, err
	}
	var other []adoptSource
	for _, d := range docs {
		switch {
		case d.Root == root && (d.RelPath == relpath || d.RelPath == from):
		case d.Root == root:
			out = append(out, adoptSource{root: d.Root, relpath: d.RelPath})
		case e.hasRoot(d.Root):
			other = append(other, adoptSource{root: d.Root, relpath: d.RelPath})
		}
	}
	out = append(out, other...)
	if len(out) > maxAdoptCandidates {
		out = out[:maxAdoptCandidates]
	}
	return out, nil
}

// hasRoot reports whether root is one of the engine's configured roots: a
// record left from a root no longer configured is not a source.
func (e *Engine) hasRoot(root string) bool {
	for _, r := range e.Roots {
		if r == root {
			return true
		}
	}
	return false
}

// adoptLocal produces the content meta describes at root/relpath from a
// local copy, when one verifies, and applies it as Apply would apply the
// same bytes received from fromPeer. adopted reports whether a local copy
// was found; when it is false nothing was done and the content has to be
// transferred. from and link come from the plan (Action.AdoptFrom,
// Action.AdoptLink); from is empty when the plan names no source.
func (e *Engine) adoptLocal(root, relpath string, meta Meta, from string, link bool, fromPeer string) (res Result, conflict *store.Conflict, adopted bool, err error) {
	if meta.Deleted || meta.Hash == "" {
		return ResultNoop, nil, false, nil
	}
	if _, err := DeclaredContentHash(meta.Hash); err != nil {
		return ResultNoop, nil, false, nil
	}
	sources, err := e.adoptSources(root, relpath, meta.Hash, from, link)
	if err != nil {
		e.logger().Debug("looking up local copies failed", "relpath", relpath, "err", err)
	}
	for _, src := range sources {
		staged, release, err := e.stageLocal(src, root, relpath, meta.Hash, meta.Size)
		if err != nil {
			e.logger().Debug("local copy not adopted", "relpath", relpath, "source", src.relpath, "err", err)
			continue
		}
		res, conflict, err = e.applyOp(root, relpath, meta, staged, fromPeer)
		release()
		if err == nil && (res == ResultApplied || res == ResultConflicted) {
			e.adoptedFiles.Add(1)
			e.adoptedBytes.Add(staged.Size)
			e.logger().Debug("adopted content held locally", "relpath", relpath, "source", src.relpath, "peer", identity.Short(fromPeer))
		}
		return res, conflict, true, err
	}
	return ResultNoop, nil, false, nil
}

// stageLocal stages src's file as content for root/relpath, verified
// against hash and size: a temporary file beside the destination, so
// Install renames it into place. With src.link it is a hard link to the
// file when the filesystem allows, else a copy. On failure nothing is left
// behind, including a directory created for it.
func (e *Engine) stageLocal(src adoptSource, root, relpath, hash string, size int64) (_ *Staged, release func(), err error) {
	from, err := pathguard.Join(src.root, src.relpath)
	if err != nil {
		return nil, nil, err
	}
	dst, err := pathguard.Join(root, relpath)
	if err != nil {
		return nil, nil, err
	}
	fi, err := os.Lstat(from)
	if err != nil {
		return nil, nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("sync: %s is not a regular file", src.relpath)
	}
	if size >= 0 && fi.Size() != size {
		return nil, nil, fmt.Errorf("%w: %s holds %d bytes, want %d", ErrContentMismatch, src.relpath, fi.Size(), size)
	}
	dir := filepath.Dir(dst)
	defer func() {
		if err != nil {
			// A directory made only for this file goes again (LOC-74).
			_, _ = fswatch.PruneEmptyParents(root, relpath)
		}
	}()
	tmp, err := tempIn(dir)
	if err != nil {
		return nil, nil, err
	}
	if src.link {
		if st, err := e.linkStaged(from, tmp, hash, size); err == nil {
			return st, st.release(nil), nil
		}
		// Another filesystem, no hard links, or the content changed:
		// a copy decides.
	}
	in, err := os.Open(from)
	if err != nil {
		return nil, nil, err
	}
	defer in.Close()
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, nil, err
	}
	n, sum, err := stageFile(f, 0, size, e.MaxFile, hash, in)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, nil, err
	}
	st := &Staged{f: f, path: tmp, Hash: sum, Size: n}
	return st, st.release(nil), nil
}

// linkStaged hard-links from at tmp and verifies what it links to.
func (e *Engine) linkStaged(from, tmp, hash string, size int64) (*Staged, error) {
	if err := os.Link(from, tmp); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		os.Remove(tmp)
		return nil, err
	}
	n, sum, err := verifyContent(f, hash, size, e.MaxFile)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, err
	}
	return &Staged{f: f, path: tmp, Hash: sum, Size: n}, nil
}

// verifyContent hashes f from the start, checks it against hash, size
// (when not negative) and maxBytes (when positive), and rewinds it.
func verifyContent(f *os.File, hash string, size, maxBytes int64) (int64, string, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 62
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, maxBytes+1))
	if err != nil {
		return 0, "", err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	switch {
	case n > maxBytes:
		return 0, "", fmt.Errorf("%w %d", ErrTooLarge, maxBytes)
	case size >= 0 && n != size:
		return 0, "", fmt.Errorf("%w: expected %d bytes, have %d", ErrContentMismatch, size, n)
	case sum != hash:
		return 0, "", fmt.Errorf("%w: content hashes to %s, want %s", ErrContentMismatch, sum, hash)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	return n, sum, nil
}

// tempIn returns an unused Locus temporary file name in dir, creating dir
// when needed. The name is free when tempIn returns.
func tempIn(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, fswatch.TempPrefix+"*")
	if errors.Is(err, fs.ErrNotExist) {
		// Pruned between MkdirAll and here (LOC-74).
		if err = os.MkdirAll(dir, 0o755); err == nil {
			f, err = os.CreateTemp(dir, fswatch.TempPrefix+"*")
		}
	}
	if err != nil {
		return "", err
	}
	name := f.Name()
	f.Close()
	if err := os.Remove(name); err != nil {
		return "", err
	}
	return name, nil
}

// unshareLink gives relpath a file of its own when it still shares one
// with from: a pull adopted from's content by hard-linking it, and the
// deletion of from that should have followed did not remove it (a local
// edit was preserved instead). Two documents must never share one file, or
// an edit to either would silently change both.
func (e *Engine) unshareLink(root, from, relpath string) {
	a, err := pathguard.Join(root, from)
	if err != nil {
		return
	}
	b, err := pathguard.Join(root, relpath)
	if err != nil {
		return
	}
	fa, err := os.Lstat(a)
	if err != nil {
		return
	}
	defer e.Store.LockPathFor(root, relpath, "sync unshare")()
	fb, err := os.Lstat(b)
	if err != nil || !os.SameFile(fa, fb) {
		return
	}
	f, err := os.Open(b)
	if err != nil {
		e.logger().Warn("separating an adopted file from its source", "relpath", relpath, "err", err)
		return
	}
	defer f.Close()
	// The same bytes under a new inode: the record stays as it is.
	if _, _, err := Install(root, relpath, f, "", e.MaxFile); err != nil {
		e.logger().Warn("separating an adopted file from its source", "relpath", relpath, "err", err)
	}
}

// Adopted reports how many incoming files, and how many bytes of content,
// this engine produced from local copies rather than transferring them,
// pulled and pushed to it together, since it started (LOC-82).
func (e *Engine) Adopted() (files, bytes int64) {
	return e.adoptedFiles.Load(), e.adoptedBytes.Load()
}

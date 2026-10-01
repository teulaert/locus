package sync

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/pathguard"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// Meta describes an incoming operation.
type Meta struct {
	Hash       string
	Size       int64
	Version    vvector.Vector
	LastWriter string
	Deleted    bool
}

// Apply applies one incoming operation to the local filesystem and state
// database, using version vectors to decide between descendant, divergence
// and no-op. Conflicts preserve both sides: one version keeps the normal
// path, the other is stored beside it.
//
// relpath comes from a peer and is validated here, before anything is
// recorded or touched, so every caller is covered: a path that would escape
// the root must not even leave a tombstone behind.
//
// Apply holds the path's lock from reading the record to writing it, so
// concurrent server handlers and the local round cannot interleave on one
// path. The record can still lag the file by the watcher's debounce, so
// before replacing or removing a file Apply hashes it, and an edit the
// record does not know about is treated as a concurrent local version.
func (e *Engine) Apply(root, relpath string, meta Meta, content io.Reader, fromPeer string) (Result, error) {
	relpath, err := pathguard.CleanRel(relpath)
	if err != nil {
		return ResultRejected, err
	}
	defer e.paths.lock(root, relpath)()
	localDoc, err := e.Store.GetDoc(root, relpath)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ResultRejected, err
	}
	if e.afterRead != nil {
		e.afterRead(root, relpath)
	}
	if meta.Version == nil {
		meta.Version = vvector.New()
	}

	if meta.Deleted {
		return e.applyDelete(root, relpath, meta, localDoc, fromPeer)
	}

	// Content operations must have content.
	if content == nil {
		return ResultRejected, errors.New("sync: missing content")
	}

	if localDoc == nil {
		// A file created locally but not yet reconciled has no record.
		if cur, drifted, err := e.onDisk(root, relpath, nil, meta.Hash); err != nil {
			return ResultRejected, err
		} else if drifted {
			return e.applyConflict(root, relpath, meta, content, cur, fromPeer)
		}
		e.notePending(root, relpath, meta)
		size, sum, err := Install(root, relpath, content, meta.Hash, e.MaxFile)
		if err != nil {
			return ResultRejected, err
		}
		if sum != meta.Hash {
			return ResultRejected, fmt.Errorf("sync: hash mismatch for %s", relpath)
		}
		if err := e.record(root, relpath, sum, size, meta.Version, meta.LastWriter, false); err != nil {
			return ResultRejected, err
		}
		e.clearPending(root, relpath)
		e.notify(root, relpath, false)
		return ResultApplied, nil
	}

	localVer := localDoc.Version
	switch vvector.Compare(localVer, meta.Version) {
	case vvector.After:
		return ResultNoop, nil
	case vvector.Before:
		// The incoming version descends from what we recorded, but not
		// from an edit made since: that edit is concurrent with it.
		if cur, drifted, err := e.onDisk(root, relpath, localDoc, meta.Hash); err != nil {
			return ResultRejected, err
		} else if drifted {
			return e.applyConflict(root, relpath, meta, content, cur, fromPeer)
		}
		e.notePending(root, relpath, meta)
		size, sum, err := Install(root, relpath, content, meta.Hash, e.MaxFile)
		if err != nil {
			return ResultRejected, err
		}
		_ = size
		if err := e.record(root, relpath, sum, meta.Size, meta.Version, meta.LastWriter, false); err != nil {
			return ResultRejected, err
		}
		e.clearPending(root, relpath)
		e.notify(root, relpath, false)
		return ResultApplied, nil
	case vvector.Equal:
		if localDoc.Hash == meta.Hash {
			return ResultNoop, nil
		}
		// Same causal history but different content: treat as divergence.
		fallthrough
	default:
		// Describe the local side by what is on disk, so the conflict
		// record and the sibling name match the content preserved.
		cur, drifted, err := e.onDisk(root, relpath, localDoc, meta.Hash)
		if err != nil {
			return ResultRejected, err
		}
		if drifted {
			localDoc = cur
		}
		return e.applyConflict(root, relpath, meta, content, localDoc, fromPeer)
	}
}

// onDisk compares the file at root/relpath with what localDoc records. When
// they differ, the file holds a local edit the reconciler has not recorded
// yet (the watcher debounces events), and onDisk returns a record describing
// it as this peer's next version, which is what reconciliation would write.
// A missing file is not drift: there is no content to lose, and an unrecorded
// local deletion yields to an incoming edit as delete-vs-edit does anyway. A
// file that already holds incomingHash (when non-empty) is not drift either.
func (e *Engine) onDisk(root, relpath string, localDoc *store.Doc, incomingHash string) (*store.Doc, bool, error) {
	full, err := pathguard.Join(root, relpath)
	if err != nil {
		return nil, false, err
	}
	hash, size, err := fswatch.HashFile(full)
	if os.IsNotExist(err) {
		return localDoc, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if incomingHash != "" && hash == incomingHash {
		return localDoc, false, nil
	}
	base := vvector.New()
	if localDoc != nil {
		if !localDoc.Deleted && localDoc.Hash == hash {
			return localDoc, false, nil
		}
		base = localDoc.Version
	}
	return &store.Doc{
		Root:       root,
		RelPath:    relpath,
		Size:       size,
		Hash:       hash,
		Version:    base.Increment(e.Identity.PeerID),
		LastWriter: e.Identity.PeerID,
	}, true, nil
}

// applyConflict preserves both sides of a genuine divergence.
func (e *Engine) applyConflict(root, relpath string, meta Meta, content io.Reader, localDoc *store.Doc, fromPeer string) (Result, error) {
	local := Version{Writer: localDoc.LastWriter, Hash: localDoc.Hash}
	incoming := Version{Writer: meta.LastWriter, Hash: meta.Hash}
	if localDoc.Deleted {
		// Delete-vs-edit: keep the edited file, record the conflict.
		merged := vvector.MergeMax(localDoc.Version, meta.Version)
		if err := e.record(root, relpath, localDoc.Hash, localDoc.Size, merged, localDoc.LastWriter, false); err != nil {
			return ResultRejected, err
		}
		e.storeConflict(root, relpath, localDoc.Hash, meta.Hash, localDoc.LastWriter, meta.LastWriter, "", fromPeer)
		return ResultConflicted, nil
	}
	hostnames := e.hostnames()
	merged := vvector.MergeMax(localDoc.Version, meta.Version)
	// The merged version must dominate both sides so the conflict does not
	// re-fire on the next round once contents agree.
	merged = merged.Increment(e.Identity.PeerID)

	if IsIncomingWinner(local, incoming) {
		// Local loses the normal path: preserve it as a sibling first.
		sibling := SiblingName(relpath, localDoc.LastWriter, localDoc.Hash, hostnames)
		if err := e.copyToSibling(root, relpath, sibling); err != nil {
			return ResultRejected, err
		}
		size, sum, err := Install(root, relpath, content, meta.Hash, e.MaxFile)
		if err != nil {
			return ResultRejected, err
		}
		if err := e.record(root, relpath, sum, size, merged, meta.LastWriter, false); err != nil {
			return ResultRejected, err
		}
		e.storeConflict(root, relpath, sum, localDoc.Hash, meta.LastWriter, localDoc.LastWriter, sibling, fromPeer)
		e.notify(root, relpath, false)
		e.notify(root, sibling, false)
		return ResultConflicted, nil
	}
	// Incoming loses the normal path: store it beside the unchanged local file.
	sibling := SiblingName(relpath, meta.LastWriter, meta.Hash, hostnames)
	e.notePending(root, sibling, meta)
	if _, _, err := Install(root, sibling, content, meta.Hash, e.MaxFile); err != nil {
		return ResultRejected, err
	}
	if err := e.record(root, relpath, localDoc.Hash, localDoc.Size, merged, localDoc.LastWriter, false); err != nil {
		return ResultRejected, err
	}
	e.clearPending(root, sibling)
	e.storeConflict(root, relpath, localDoc.Hash, meta.Hash, localDoc.LastWriter, meta.LastWriter, sibling, fromPeer)
	e.notify(root, sibling, false)
	return ResultConflicted, nil
}

func (e *Engine) applyDelete(root, relpath string, meta Meta, localDoc *store.Doc, fromPeer string) (Result, error) {
	if localDoc == nil {
		if err := e.record(root, relpath, meta.Hash, 0, meta.Version, meta.LastWriter, true); err != nil {
			return ResultRejected, err
		}
		return ResultApplied, nil
	}
	switch vvector.Compare(localDoc.Version, meta.Version) {
	case vvector.Before, vvector.Equal:
		// The incoming deletion is the newer desired state, unless the file
		// was edited since it was recorded: that edit is concurrent with the
		// delete and must survive it.
		if cur, drifted, err := e.onDisk(root, relpath, localDoc, ""); err != nil {
			return ResultRejected, err
		} else if drifted {
			return e.deleteConflict(root, relpath, meta, cur, fromPeer)
		}
		full, err := pathguard.Join(root, relpath)
		if err != nil {
			return ResultRejected, err
		}
		if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
			return ResultRejected, err
		}
		if err := e.record(root, relpath, localDoc.Hash, 0, meta.Version, meta.LastWriter, true); err != nil {
			return ResultRejected, err
		}
		e.notify(root, relpath, true)
		return ResultApplied, nil
	case vvector.After:
		// Our copy is causally newer than the delete request.
		return ResultNoop, nil
	default:
		// Concurrent delete vs edit: preserve the edit, record the conflict.
		// Nothing is removed, so an unrecorded newer edit survives too and
		// is picked up by the next reconciliation.
		return e.deleteConflict(root, relpath, meta, localDoc, fromPeer)
	}
}

// deleteConflict resolves a delete concurrent with a local edit: the edit
// is preserved and the conflict recorded.
func (e *Engine) deleteConflict(root, relpath string, meta Meta, localDoc *store.Doc, fromPeer string) (Result, error) {
	merged := vvector.MergeMax(localDoc.Version, meta.Version).Increment(e.Identity.PeerID)
	if err := e.record(root, relpath, localDoc.Hash, localDoc.Size, merged, localDoc.LastWriter, false); err != nil {
		return ResultRejected, err
	}
	e.storeConflict(root, relpath, localDoc.Hash, meta.Hash, localDoc.LastWriter, meta.LastWriter, "", fromPeer)
	return ResultConflicted, nil
}

// notePending records the provenance of an incoming file just before it lands,
// so a crash between the atomic rename and the document record cannot make a
// received file look as though this peer authored it.
func (e *Engine) notePending(root, relpath string, meta Meta) {
	if meta.Version == nil || meta.LastWriter == "" {
		return
	}
	if err := e.Store.PutPendingArrival(&store.PendingArrival{
		Root: root, RelPath: relpath, Hash: meta.Hash, Version: meta.Version.String(), Writer: meta.LastWriter,
	}); err != nil {
		e.logger().Error("record pending arrival", "err", err)
	}
}

// clearPending drops the provenance once the document record is durable.
func (e *Engine) clearPending(root, relpath string) {
	if err := e.Store.ClearPendingArrival(root, relpath); err != nil {
		e.logger().Error("clear pending arrival", "err", err)
	}
}

func (e *Engine) record(root, relpath, hash string, size int64, version vvector.Vector, writer string, deleted bool) error {
	return e.Store.UpsertDoc(&store.Doc{
		Root:       root,
		RelPath:    relpath,
		Size:       size,
		Hash:       hash,
		Version:    version,
		LastWriter: writer,
		Deleted:    deleted,
		UpdatedAt:  time.Now().Unix(),
	})
}

func (e *Engine) storeConflict(root, relpath, keepHash, loseHash, keepWriter, loseWriter, siblingPath, fromPeer string) {
	c := &store.Conflict{
		ID:             ConflictID(relpath, keepHash, loseHash),
		Root:           root,
		RelPath:        relpath,
		LocalPath:      relpath,
		ConflictPath:   siblingPath,
		LocalHash:      keepHash,
		ConflictHash:   loseHash,
		LocalWriter:    keepWriter,
		ConflictWriter: loseWriter,
		CreatedAt:      time.Now().Unix(),
	}
	if err := e.Store.PutConflict(c); err != nil {
		e.logger().Error("record conflict", "err", err)
	}
	if err := e.Store.Audit("sync", "conflict", fmt.Sprintf("%s keep=%s other=%s peer=%s", relpath, identity.Short(keepWriter), identity.Short(loseWriter), identity.Short(fromPeer))); err != nil {
		e.logger().Error("audit conflict", "err", err)
	}
}

// copyToSibling preserves the file at relpath as sibling. The source is
// resolved through pathguard.Join, so a symlink inside the root that points
// outside it is refused rather than copied in (LOC-49).
func (e *Engine) copyToSibling(root, relpath, sibling string) error {
	src, err := pathguard.Join(root, relpath)
	if err != nil {
		return err
	}
	// Streamed like any other install; the sibling holds exactly the bytes
	// read, whatever their hash.
	f, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	_, _, err = Install(root, sibling, f, "", e.MaxFile)
	return err
}

func (e *Engine) hostnames() map[string]string {
	if e.Hostnames != nil {
		return e.Hostnames()
	}
	return nil
}

func (e *Engine) notify(root, relpath string, deleted bool) {
	if e.Notifier != nil {
		e.Notifier.NotifyChange(root, relpath, deleted)
	}
}

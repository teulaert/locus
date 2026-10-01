// Package reconcile compares the actual filesystem against stored metadata
// and drives both databases and the index queue toward the current desired
// state. Watcher events are hints; this pass is the source of truth.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/jobs"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// Report summarizes one reconciliation pass.
type Report struct {
	Root      string `json:"root"`
	Created   int    `json:"created"`
	Modified  int    `json:"modified"`
	Deleted   int    `json:"deleted"`
	Unchanged int    `json:"unchanged"`
	Jobs      int    `json:"jobs_queued"`
	// Hashed counts files whose bytes were read on this pass; the rest
	// were vouched for by an unchanged stat fingerprint.
	Hashed int `json:"hashed"`
	// FullVerify is set when this pass ignored fingerprints and re-hashed
	// every file (see Reconciler.VerifyEvery).
	FullVerify bool `json:"full_verify"`
	// TempsRemoved counts crashed-transfer leftovers cleaned up on this pass.
	TempsRemoved int    `json:"temps_removed"`
	InSync       bool   `json:"in_sync"`
	Duration     string `json:"duration"`
}

// Reconciler walks roots and records desired state.
type Reconciler struct {
	Store    *store.Store
	Queue    *jobs.Queue
	Identity *identity.Identity
	Roots    []string
	Log      *slog.Logger

	// TombstoneAge prunes deletion records older than this.
	TombstoneAge time.Duration

	// TempAge is how old a transfer temporary must be before a pass removes
	// it. Zero means "leave them for an hour"; a transfer in flight is always
	// fresher than this.
	TempAge time.Duration

	// VerifyEvery is how often a pass ignores stat fingerprints and hashes
	// every file under a root, as a slow safety net for edits that keep
	// size, mtime, ctime and inode (or platforms that expose only size and
	// mtime). Zero disables it.
	VerifyEvery time.Duration
}

// Pass reconciles every root.
func (r *Reconciler) Pass(ctx context.Context) ([]*Report, error) {
	var reports []*Report
	for _, root := range r.Roots {
		rep, err := r.PassRoot(ctx, root)
		if err != nil {
			return reports, err
		}
		reports = append(reports, rep)
	}
	return reports, nil
}

// PassRoot reconciles one root.
func (r *Reconciler) PassRoot(ctx context.Context, root string) (*Report, error) {
	started := time.Now()
	rep := &Report{Root: root}
	docs, err := r.Store.ListDocs(root, true)
	if err != nil {
		return rep, err
	}
	rep.FullVerify = r.verifyDue(root, started)
	byPath := make(map[string]*store.Doc, len(docs))
	for _, d := range docs {
		byPath[d.RelPath] = d
	}
	// A live record whose fingerprint equals the file's current one still
	// describes these bytes, so its hash is reused instead of re-reading
	// the file. Anything else (no record, tombstone, unknown or different
	// fingerprint, a full verification pass) is hashed.
	known := func(relpath string, st fswatch.Stat) (string, bool) {
		if rep.FullVerify || !st.Known() {
			return "", false
		}
		d, ok := byPath[relpath]
		if !ok || d.Deleted || docStat(d) != st {
			return "", false
		}
		return d.Hash, true
	}
	entries, err := fswatch.Scan(root, known)
	if err != nil {
		return rep, fmt.Errorf("reconcile: scan %s: %w", root, err)
	}
	for _, e := range entries {
		if e.Hashed {
			rep.Hashed++
		}
	}
	// Fingerprints are only recorded for files whose mtime was already
	// settled when the scan began; see fswatch.RacyWindow.
	fp := func(e fswatch.Entry) fswatch.Stat {
		if !e.Stat.Settled(started) {
			return fswatch.Stat{}
		}
		return e.Stat
	}
	withStat := func(d *store.Doc, e fswatch.Entry) *store.Doc {
		st := fp(e)
		d.MtimeNS, d.CtimeNS, d.Inode = st.MtimeNS, st.CtimeNS, st.Inode
		return d
	}
	seen := make(map[string]fswatch.Entry, len(entries))
	for _, e := range entries {
		seen[e.RelPath] = e
	}
	now := time.Now().Unix()
	for _, d := range docs {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		e, exists := seen[d.RelPath]
		switch {
		case !exists && !d.Deleted:
			// Desired state: absent.
			ver := d.Version.Increment(r.peerID())
			if err := r.Store.UpsertDoc(&store.Doc{Root: root, RelPath: d.RelPath, Size: 0, Hash: d.Hash, Version: ver, LastWriter: r.peerID(), Deleted: true, UpdatedAt: now}); err != nil {
				return rep, err
			}
			if err := r.Queue.Enqueue(ctx, jobs.KindUnindex, root, d.RelPath, d.Hash, 5); err != nil {
				return rep, err
			}
			rep.Deleted++
			rep.Jobs++
		case !exists && d.Deleted:
			rep.Unchanged++
		case exists && d.Deleted:
			// Reappearing path: new content is a fresh version.
			pv, pw, ok := r.adoptPending(root, d.RelPath, e.Hash)
			ver := d.Version.Increment(r.peerID())
			writer := r.peerID()
			if ok {
				ver, writer = pv, pw
			}
			if err := r.Store.UpsertDoc(withStat(&store.Doc{Root: root, RelPath: d.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}, e)); err != nil {
				return rep, err
			}
			if err := r.Queue.Enqueue(ctx, jobs.KindIndex, root, d.RelPath, e.Hash, 0); err != nil {
				return rep, err
			}
			rep.Created++
			rep.Jobs++
			delete(seen, d.RelPath)
		case exists && d.Hash != e.Hash:
			// A hash that matches a pending arrival is a received file whose
			// record never got written, not a local edit.
			pv, pw, ok := r.adoptPending(root, d.RelPath, e.Hash)
			ver := d.Version.Increment(r.peerID())
			writer := r.peerID()
			if ok {
				ver, writer = pv, pw
			}
			if err := r.Store.UpsertDoc(withStat(&store.Doc{Root: root, RelPath: d.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}, e)); err != nil {
				return rep, err
			}
			if err := r.Queue.Enqueue(ctx, jobs.KindIndex, root, d.RelPath, e.Hash, 0); err != nil {
				return rep, err
			}
			rep.Modified++
			rep.Jobs++
			delete(seen, d.RelPath)
		default:
			rep.Unchanged++
			// Same bytes as recorded. When they were read on this pass,
			// remember the fingerprint (or forget a stale one) so the next
			// pass can skip them; the version is untouched.
			if st := fp(e); e.Hashed && (st != docStat(d) || e.Size != d.Size) {
				if err := r.Store.SetDocStat(root, d.RelPath, e.Hash, e.Size, st.MtimeNS, st.CtimeNS, st.Inode); err != nil {
					return rep, err
				}
			}
			// Metadata is current; make sure derived state still exists.
			needs, err := r.Store.NeedsIndex(root, d.RelPath, e.Hash)
			if err != nil {
				return rep, err
			}
			if needs {
				if err := r.Queue.Enqueue(ctx, jobs.KindIndex, root, d.RelPath, e.Hash, 0); err != nil {
					return rep, err
				}
				rep.Jobs++
			}
			delete(seen, d.RelPath)
		}
	}
	for _, e := range entries {
		if _, ok := seen[e.RelPath]; !ok {
			continue
		}
		ver, writer, ok := r.adoptPending(root, e.RelPath, e.Hash)
		if !ok {
			ver, writer = vvector.New().Increment(r.peerID()), r.peerID()
		}
		if err := r.Store.UpsertDoc(withStat(&store.Doc{Root: root, RelPath: e.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}, e)); err != nil {
			return rep, err
		}
		if err := r.Queue.Enqueue(ctx, jobs.KindIndex, root, e.RelPath, e.Hash, 0); err != nil {
			return rep, err
		}
		rep.Created++
		rep.Jobs++
	}
	// Transfer temporaries left behind by a crash are not documents and would
	// otherwise accumulate forever.
	if n, err := fswatch.SweepTemps(root, r.TempAge); err == nil && n > 0 {
		rep.TempsRemoved = n
	}
	rep.Duration = time.Since(started).String()
	rep.InSync = rep.Created == 0 && rep.Modified == 0 && rep.Deleted == 0 && rep.Jobs == 0
	r.pruneTombstones()
	if rep.FullVerify {
		if err := r.Store.MetaSet(r.Store.State, verifyKey(root), strconv.FormatInt(started.Unix(), 10)); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// docStat is the fingerprint a record was hashed under. A record without one
// yields a zero Stat, which never equals a real file's.
func docStat(d *store.Doc) fswatch.Stat {
	if d.MtimeNS == 0 {
		return fswatch.Stat{}
	}
	return fswatch.Stat{Size: d.Size, MtimeNS: d.MtimeNS, CtimeNS: d.CtimeNS, Inode: d.Inode}
}

func verifyKey(root string) string { return "reconcile.verified_at:" + root }

// verifyDue reports whether this pass over root must hash every file. The
// time of the last full verification lives in state.db so restarts do not
// postpone it indefinitely; a root never verified is due at once (its
// records carry no fingerprints yet, so that costs nothing extra).
func (r *Reconciler) verifyDue(root string, now time.Time) bool {
	if r.VerifyEvery <= 0 {
		return false
	}
	v, err := r.Store.MetaGet(r.Store.State, verifyKey(root))
	if err != nil {
		return true
	}
	last, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return true
	}
	return now.Sub(time.Unix(last, 0)) >= r.VerifyEvery
}

// adoptPending returns the provenance recorded by an incoming transfer for a
// file that landed but was never recorded. Without it, reconciliation would
// treat a received file as one authored on this peer, which corrupts both the
// version vector and the author used for deterministic conflict resolution.
func (r *Reconciler) adoptPending(root, relpath, hash string) (vvector.Vector, string, bool) {
	p, err := r.Store.GetPendingArrival(root, relpath)
	if err != nil || p == nil || p.Hash != hash || p.Writer == "" {
		return nil, "", false
	}
	ver, err := vvector.Parse(p.Version)
	if err != nil || len(ver) == 0 {
		return nil, "", false
	}
	if err := r.Store.ClearPendingArrival(root, relpath); err != nil && r.Log != nil {
		r.Log.Error("clear pending arrival", "err", err)
	}
	return ver, p.Writer, true
}

func (r *Reconciler) peerID() string {
	if r.Identity != nil {
		return r.Identity.PeerID
	}
	return "local"
}

// pruneTombstones forgets deletion records long after every reachable peer
// has had a chance to observe them.
func (r *Reconciler) pruneTombstones() {
	if r.TombstoneAge <= 0 {
		return
	}
	cutoff := time.Now().Add(-r.TombstoneAge).Unix()
	_, _ = r.Store.State.Exec(`DELETE FROM docs WHERE deleted = 1 AND updated_at < ?`, cutoff)
}

// EnsureRoots verifies roots exist and are directories.
func (r *Reconciler) EnsureRoots() error {
	if len(r.Roots) == 0 {
		return errors.New("reconcile: no roots configured")
	}
	for _, root := range r.Roots {
		st, err := os.Stat(root)
		if err != nil {
			return err
		}
		if !st.IsDir() {
			return fmt.Errorf("reconcile: %s is not a directory", root)
		}
	}
	return nil
}

// Package reconcile compares the actual filesystem against stored metadata
// and drives both databases and the index queue toward the current desired
// state. Watcher events are hints; this pass is the source of truth.
package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
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
	TempsRemoved int `json:"temps_removed"`
	// IndexErrors counts index.db operations (queueing index work, checking
	// whether a file still needs indexing) that failed on this pass. They
	// do not stop the pass: state.db is still brought up to date, and the
	// next pass re-queues whatever index work is still missing.
	IndexErrors int `json:"index_errors"`
	// IndexError is the first such failure, for diagnosis.
	IndexError string `json:"index_error,omitempty"`
	// Concurrent counts paths left alone because their record changed
	// while the pass ran (a file received from a peer, typically); the
	// next pass looks at them again against the new record.
	Concurrent int    `json:"concurrent"`
	InSync     bool   `json:"in_sync"`
	Duration   string `json:"duration"`
}

// IndexQueue is where a pass records derived-state work. *jobs.Queue
// satisfies it; the queue lives in index.db, which is disposable, so a
// failure here never stops a pass from recording state.
type IndexQueue interface {
	Enqueue(ctx context.Context, kind, root, relpath, hash string, priority int) error
}

// Reconciler walks roots and records desired state.
type Reconciler struct {
	Store    *store.Store
	Queue    IndexQueue
	Identity *identity.Identity
	Roots    []string
	Log      *slog.Logger

	// TombstoneAge is the minimum age before a deletion record may be
	// pruned; it is also kept until every known peer has seen it.
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

	// passes admits one pass at a time. A daemon starts passes from the
	// watcher callback, the periodic ticker and `sync now`/rebuild; two at
	// once would diff the same records against the same scan and queue or
	// tombstone twice.
	passesOnce sync.Once
	passes     chan struct{}

	// beforeRoot, when set by a test, runs as each root's pass begins (with
	// the pass slot held).
	beforeRoot func(root string)

	// afterList, when set by a test, runs once a root's records have been
	// read and before its files are scanned.
	afterList func(root string)
}

// acquire waits for the pass slot, or for ctx to end.
func (r *Reconciler) acquire(ctx context.Context) error {
	r.passesOnce.Do(func() { r.passes = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case r.passes <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Reconciler) release() { <-r.passes }

// Pass reconciles every root. Passes are serialised: a call made while
// another pass runs waits for it (or for ctx) before starting.
func (r *Reconciler) Pass(ctx context.Context) ([]*Report, error) {
	if err := r.acquire(ctx); err != nil {
		return nil, err
	}
	defer r.release()
	var reports []*Report
	for _, root := range r.Roots {
		rep, err := r.passRoot(ctx, root)
		if err != nil {
			return reports, err
		}
		reports = append(reports, rep)
	}
	return reports, nil
}

// PassRoot reconciles one root, serialised with every other pass.
func (r *Reconciler) PassRoot(ctx context.Context, root string) (*Report, error) {
	if err := r.acquire(ctx); err != nil {
		return &Report{Root: root}, err
	}
	defer r.release()
	return r.passRoot(ctx, root)
}

func (r *Reconciler) passRoot(ctx context.Context, root string) (*Report, error) {
	if r.beforeRoot != nil {
		r.beforeRoot(root)
	}
	started := time.Now()
	rep := &Report{Root: root}
	docs, err := r.Store.ListDocs(root, true)
	if err != nil {
		return rep, err
	}
	if r.afterList != nil {
		r.afterList(root)
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
			ok, err := r.recordFor(rep, d.RelPath, d, nil, func() error {
				ver := d.Version.Increment(r.peerID())
				return r.Store.UpsertDoc(&store.Doc{Root: root, RelPath: d.RelPath, Size: 0, Hash: d.Hash, Version: ver, LastWriter: r.peerID(), Deleted: true, UpdatedAt: now})
			})
			if err != nil {
				return rep, err
			}
			if !ok {
				continue
			}
			if err := r.enqueue(ctx, rep, jobs.KindUnindex, d.RelPath, d.Hash, 5); err != nil {
				return rep, err
			}
			rep.Deleted++
		case !exists && d.Deleted:
			rep.Unchanged++
			// An unindex job lost to an index.db failure on an earlier
			// pass would otherwise leave the deleted file searchable.
			stale, err := r.indexed(root, d.RelPath)
			if err != nil {
				r.indexFailed(rep, "check index", root, d.RelPath, err)
			} else if stale {
				if err := r.enqueue(ctx, rep, jobs.KindUnindex, d.RelPath, d.Hash, 5); err != nil {
					return rep, err
				}
			}
		case exists && d.Deleted:
			// Reappearing path: new content is a fresh version.
			delete(seen, d.RelPath)
			ok, err := r.recordFor(rep, d.RelPath, d, &e, func() error {
				pv, pw, ok := r.adoptPending(root, d.RelPath, e.Hash)
				ver := d.Version.Increment(r.peerID())
				writer := r.peerID()
				if ok {
					ver, writer = pv, pw
				}
				return r.Store.UpsertDoc(withStat(&store.Doc{Root: root, RelPath: d.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}, e))
			})
			if err != nil {
				return rep, err
			}
			if !ok {
				continue
			}
			if err := r.enqueue(ctx, rep, jobs.KindIndex, d.RelPath, e.Hash, 0); err != nil {
				return rep, err
			}
			rep.Created++
		case exists && d.Hash != e.Hash:
			delete(seen, d.RelPath)
			ok, err := r.recordFor(rep, d.RelPath, d, &e, func() error {
				// A hash that matches a pending arrival is a received file
				// whose record never got written, not a local edit.
				pv, pw, ok := r.adoptPending(root, d.RelPath, e.Hash)
				ver := d.Version.Increment(r.peerID())
				writer := r.peerID()
				if ok {
					ver, writer = pv, pw
				}
				return r.Store.UpsertDoc(withStat(&store.Doc{Root: root, RelPath: d.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}, e))
			})
			if err != nil {
				return rep, err
			}
			if !ok {
				continue
			}
			if err := r.enqueue(ctx, rep, jobs.KindIndex, d.RelPath, e.Hash, 0); err != nil {
				return rep, err
			}
			rep.Modified++
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
				r.indexFailed(rep, "check index", root, d.RelPath, err)
			} else if needs {
				if err := r.enqueue(ctx, rep, jobs.KindIndex, d.RelPath, e.Hash, 0); err != nil {
					return rep, err
				}
			}
			delete(seen, d.RelPath)
		}
	}
	for _, e := range entries {
		if _, ok := seen[e.RelPath]; !ok {
			continue
		}
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		ok, err := r.recordFor(rep, e.RelPath, nil, &e, func() error {
			ver, writer, ok := r.adoptPending(root, e.RelPath, e.Hash)
			if !ok {
				ver, writer = vvector.New().Increment(r.peerID()), r.peerID()
			}
			return r.Store.UpsertDoc(withStat(&store.Doc{Root: root, RelPath: e.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}, e))
		})
		if err != nil {
			return rep, err
		}
		if !ok {
			continue
		}
		if err := r.enqueue(ctx, rep, jobs.KindIndex, e.RelPath, e.Hash, 0); err != nil {
			return rep, err
		}
		rep.Created++
	}
	// Transfer temporaries left behind by a crash are not documents and would
	// otherwise accumulate forever.
	if n, err := fswatch.SweepTemps(root, r.TempAge); err == nil && n > 0 {
		rep.TempsRemoved = n
	}
	rep.Duration = time.Since(started).String()
	rep.InSync = rep.Created == 0 && rep.Modified == 0 && rep.Deleted == 0 && rep.Jobs == 0 && rep.IndexErrors == 0 && rep.Concurrent == 0
	r.pruneTombstones(root)
	if rep.FullVerify {
		if err := r.Store.MetaSet(r.Store.State, verifyKey(root), strconv.FormatInt(started.Unix(), 10)); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// recordFor writes a new record for relpath, through write, unless the
// record changed after the pass read it. planned is the record the pass
// decided against and scanned the file it found there (each nil when there
// was none).
//
// The check and the write hold the path's lock, the one the sync engine's
// Apply holds while it installs and records a received file. Without it a
// file installed between ListDocs and the scan looks like a local creation or
// edit, and the pass would record it again as this peer's new version: same
// bytes, a vector concurrent with the sender's, so the sender's next change
// to the path would come back as a conflict (LOC-58).
//
// It reports whether the record was written. When it was not, the path is
// counted: as unchanged if the current record already describes the scanned
// content (bytes that match their record are never re-versioned), otherwise
// as concurrent, for the next pass to look at against the new record.
func (r *Reconciler) recordFor(rep *Report, relpath string, planned *store.Doc, scanned *fswatch.Entry, write func() error) (bool, error) {
	defer r.Store.LockPath(rep.Root, relpath)()
	cur, err := r.Store.GetDoc(rep.Root, relpath)
	if errors.Is(err, sql.ErrNoRows) {
		cur, err = nil, nil
	}
	if err != nil {
		return false, err
	}
	switch {
	case cur != nil && scanned != nil && !cur.Deleted && cur.Hash == scanned.Hash,
		cur != nil && scanned == nil && cur.Deleted:
		rep.Unchanged++
		return false, nil
	case !sameRecord(planned, cur):
		rep.Concurrent++
		return false, nil
	}
	return true, write()
}

// sameRecord reports whether cur is still the record planned was read as;
// nil stands for no record.
func sameRecord(planned, cur *store.Doc) bool {
	if planned == nil || cur == nil {
		return planned == nil && cur == nil
	}
	return cur.Hash == planned.Hash && cur.Deleted == planned.Deleted &&
		cur.LastWriter == planned.LastWriter && vvector.Compare(cur.Version, planned.Version) == vvector.Equal
}

// enqueue queues index work for relpath under rep.Root. A queue failure is
// recorded on rep and logged rather than returned: index.db is derived state,
// and aborting here would leave state.db without the records sync depends on.
// Only a cancelled ctx is returned, since the pass is over either way.
func (r *Reconciler) enqueue(ctx context.Context, rep *Report, kind, relpath, hash string, priority int) error {
	if err := r.Queue.Enqueue(ctx, kind, rep.Root, relpath, hash, priority); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		r.indexFailed(rep, "queue "+kind, rep.Root, relpath, err)
		return nil
	}
	rep.Jobs++
	return nil
}

// indexFailed records a non-fatal index.db failure on rep.
func (r *Reconciler) indexFailed(rep *Report, op, root, relpath string, err error) {
	if rep.IndexErrors == 0 {
		rep.IndexError = fmt.Sprintf("%s %s: %v", op, relpath, err)
	}
	rep.IndexErrors++
	if r.Log != nil {
		r.Log.Error("reconcile: index.db", "op", op, "root", root, "path", relpath, "err", err)
	}
}

// indexed reports whether index.db still holds derived state for a path.
func (r *Reconciler) indexed(root, relpath string) (bool, error) {
	var one int
	err := r.Store.Index.QueryRow(`SELECT 1 FROM documents WHERE root = ? AND relpath = ?`, root, relpath).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
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

// pruneTombstones forgets deletion records under root once they are older
// than TombstoneAge and every peer this one has synchronized the root with
// has completed a clean round since; see store.PruneTombstones.
func (r *Reconciler) pruneTombstones(root string) {
	if r.TombstoneAge <= 0 {
		return
	}
	n, err := r.Store.PruneTombstones(root, time.Now().Add(-r.TombstoneAge))
	if r.Log == nil {
		return
	}
	if err != nil {
		r.Log.Error("prune tombstones", "root", root, "err", err)
	} else if n > 0 {
		r.Log.Info("pruned tombstones", "root", root, "count", n)
	}
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

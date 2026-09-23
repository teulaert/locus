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
	"time"

	"locus/internal/fswatch"
	"locus/internal/identity"
	"locus/internal/jobs"
	"locus/internal/store"
	"locus/internal/vvector"
)

// Report summarizes one reconciliation pass.
type Report struct {
	Root      string `json:"root"`
	Created   int    `json:"created"`
	Modified  int    `json:"modified"`
	Deleted   int    `json:"deleted"`
	Unchanged int    `json:"unchanged"`
	Jobs      int    `json:"jobs_queued"`
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
	entries, err := fswatch.Scan(root)
	if err != nil {
		return rep, fmt.Errorf("reconcile: scan %s: %w", root, err)
	}
	docs, err := r.Store.ListDocs(root, true)
	if err != nil {
		return rep, err
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
			if err := r.Store.UpsertDoc(&store.Doc{Root: root, RelPath: d.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}); err != nil {
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
			if err := r.Store.UpsertDoc(&store.Doc{Root: root, RelPath: d.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}); err != nil {
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
		if err := r.Store.UpsertDoc(&store.Doc{Root: root, RelPath: e.RelPath, Size: e.Size, Hash: e.Hash, Version: ver, LastWriter: writer, Deleted: false, UpdatedAt: now}); err != nil {
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
	return rep, nil
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

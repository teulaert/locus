package app

import (
	"context"
	"path/filepath"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/jobs"
)

// NotifyChange implements sync.ChangeNotifier: files changed by
// synchronization enter the normal index pipeline.
func (a *App) NotifyChange(root, relpath string, deleted bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if deleted {
		_ = a.Queue.Enqueue(ctx, jobs.KindUnindex, root, relpath, "", 5)
		return
	}
	hash, _, err := fswatch.HashFile(filepath.Join(root, filepath.FromSlash(relpath)))
	if err != nil {
		return
	}
	_ = a.Queue.Enqueue(ctx, jobs.KindIndex, root, relpath, hash, 0)
}

// RebuildIndex deletes all derived search state and reconstructs it from the
// local files. Synchronization state is untouched.
//
// It takes the data directory's daemon lock for its whole run, so it refuses
// (ErrDaemonRunning) while `locus serve` runs on the same data directory,
// whose index workers would be writing the tables it drops, and a daemon
// started meanwhile refuses to start until the rebuild is done.
func (a *App) RebuildIndex(ctx context.Context) error {
	if a.Cfg.Rendezvous {
		return ErrRendezvous
	}
	lock, err := lockDataDir(a.DataDir())
	if err != nil {
		return err
	}
	defer lock.Release()
	// Holding the lock means no daemon is alive, so any job still marked
	// running was left behind by one that stopped mid-job. Resume it, or the
	// Drain below would wait on it forever.
	if _, err := a.Queue.RequeueStale(ctx); err != nil {
		return err
	}
	// Reconcile first so the state database reflects what is on disk; the
	// rebuild is then a purely derived operation.
	if _, err := a.Reconciler.Pass(ctx); err != nil {
		return err
	}
	if err := a.Store.DropDerived(); err != nil {
		return err
	}
	roots := a.Reconciler.Roots
	// Queue every currently present file for re-indexing. The pass above
	// just brought the records in line with the disk, so their hashes are
	// current without reading every file again; the pipeline revalidates
	// each job against the file before doing work.
	for _, root := range roots {
		docs, err := a.Store.ListDocs(root, false)
		if err != nil {
			return err
		}
		for _, d := range docs {
			if err := a.Queue.Enqueue(ctx, jobs.KindIndex, root, d.RelPath, d.Hash, 1); err != nil {
				return err
			}
		}
	}
	if err := a.Store.MetaSet(a.Store.Index, metaEmbeddingModel, a.Embedder.ID()); err != nil {
		return err
	}
	// Drain inline so the command reports a completed rebuild.
	return a.Drain(ctx, 0)
}

// Drain processes queued work inline until the queue is empty (or timeout).
func (a *App) Drain(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	if timeout <= 0 {
		deadline = time.Now().Add(24 * time.Hour)
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := a.Queue.PendingCount(ctx)
		if err != nil {
			return err
		}
		if n == 0 || time.Now().After(deadline) {
			return nil
		}
		job, err := a.Queue.Claim(ctx)
		if err != nil {
			return err
		}
		if job == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err := a.Indexer.Process(ctx, job); err != nil {
			_ = a.Queue.Fail(ctx, job, err)
			continue
		}
		_ = a.Queue.Complete(ctx, job)
	}
}

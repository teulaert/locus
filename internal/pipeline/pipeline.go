// Package pipeline runs index work from the queue: extract → chunk → embed,
// revalidating every job against the current desired filesystem state before
// doing any work.
package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/teulaert/locus/internal/chunk"
	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/extract"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/jobs"
	"github.com/teulaert/locus/internal/store"
)

// Gate lets interactive work (search) take priority over background
// indexing: workers stop claiming while a search is running.
type Gate struct{ active atomic.Int32 }

// Enter marks interactive work in flight and returns the leave function.
func (g *Gate) Enter() func() {
	g.active.Add(1)
	return func() { g.active.Add(-1) }
}

// Busy reports whether interactive work is running.
func (g *Gate) Busy() bool { return g != nil && g.active.Load() > 0 }

// Pipeline executes queued index jobs.
type Pipeline struct {
	Store    *store.Store
	Queue    *jobs.Queue
	Embedder embed.Embedder
	Gate     *Gate
	Log      *slog.Logger

	wg      sync.WaitGroup
	embedMu sync.Mutex
}

// Run starts concurrency workers and blocks until ctx is cancelled.
func (p *Pipeline) Run(ctx context.Context, concurrency int) {
	if concurrency <= 0 {
		concurrency = 1
	}
	if n, err := p.Queue.RequeueStale(ctx); err != nil {
		p.logger().Error("requeue stale jobs", "err", err)
	} else if n > 0 {
		p.logger().Info("resumed interrupted index work", "jobs", n)
	}
	for i := 0; i < concurrency; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	p.wg.Wait()
}

func (p *Pipeline) logger() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.Default()
}

func (p *Pipeline) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		if p.Gate.Busy() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		job, err := p.Queue.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger().Error("claim job", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		if job == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(150 * time.Millisecond):
			}
			continue
		}
		if err := p.Process(ctx, job); err != nil {
			_ = p.Queue.Fail(ctx, job, err)
			p.logger().Debug("job failed", "kind", job.Kind, "path", job.RelPath, "err", err)
			continue
		}
		if err := p.Queue.Complete(ctx, job); err != nil {
			p.logger().Error("complete job", "err", err)
		}
	}
}

// Process runs one job. It is exported for tests and for the foreground
// drain. A panic anywhere in the job is recovered and returned as a permanent
// failure, so one bad document parks its job instead of crashing the daemon
// (and with it synchronization).
func (p *Pipeline) Process(ctx context.Context, job *jobs.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			p.logger().Error("index job panicked", "kind", job.Kind, "path", job.RelPath, "panic", r)
			err = jobs.Permanent(fmt.Errorf("pipeline: panic: %v", r))
		}
	}()
	switch job.Kind {
	case jobs.KindIndex:
		return p.processIndex(ctx, job)
	case jobs.KindUnindex:
		return p.Store.DeleteIndexDocument(job.Root, job.RelPath)
	default:
		return fmt.Errorf("pipeline: unknown job kind %q", job.Kind)
	}
}

func (p *Pipeline) processIndex(ctx context.Context, job *jobs.Job) error {
	// Revalidate against current desired state before doing any work.
	path := filepath.Join(job.Root, filepath.FromSlash(job.RelPath))
	st, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil // obsolete: file already disappeared, drop the work
	}
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return nil
	}
	hash, size, err := fswatch.HashFile(path)
	if err != nil {
		return err
	}
	if hash != job.Hash {
		return nil // superseded: newer work exists for the newest version
	}
	needs, err := p.Store.NeedsIndex(job.Root, job.RelPath, hash)
	if err != nil {
		return err
	}
	if !needs {
		// Already indexed at this hash, but embeddings may still be stale
		// after a model change; make sure they exist.
		return p.embedDocument(ctx, job)
	}

	docOut := &extract.Document{}
	noText := ""
	if ex := extract.Lookup(path); ex != nil {
		doc, err := extract.Run(ctx, ex, path)
		if errors.Is(err, extract.ErrPanic) {
			// The parser crashed on this file. Drop stale derived state and
			// record a permanent failure: the same bytes would panic again.
			p.logger().Warn("extraction panicked", "path", job.RelPath, "extractor", ex.Name(), "err", err)
			if derr := p.Store.DeleteIndexDocument(job.Root, job.RelPath); derr != nil {
				return derr
			}
			return jobs.Permanent(err)
		}
		if err != nil {
			p.logger().Debug("extraction failed", "path", job.RelPath, "extractor", ex.Name(), "err", err)
			docOut = nil
			noText = store.NoTextExtractFailed
		} else {
			docOut = doc
		}
	} else {
		docOut = nil
		noText = store.NoTextNoExtractor
	}
	if docOut == nil || docOut.Text == "" {
		// An interrupted extraction says nothing about the file; leave it
		// to be retried rather than recording it as text-free.
		if err := ctx.Err(); err != nil {
			return err
		}
		if noText == "" {
			noText = store.NoTextEmpty
		}
		// No extractable text: the file still synchronizes. Record it as
		// processed at this hash with zero chunks, replacing any stale
		// derived state, so reconciliation does not re-queue it every pass.
		return p.Store.IndexNoText(job.Root, job.RelPath, hash, size, noText)
	}
	chunks := chunk.Split(docOut)
	stored := make([]*store.Chunk, 0, len(chunks))
	for _, c := range chunks {
		stored = append(stored, &store.Chunk{Ordinal: c.Ordinal, Text: c.Text, Page: c.Page, HasPage: c.HasPage, Section: c.Section, Hash: c.Hash})
	}
	if _, err := p.Store.IndexDocument(job.Root, job.RelPath, hash, size, stored); err != nil {
		return err
	}
	return p.embedDocument(ctx, job)
}

func (p *Pipeline) embedDocument(ctx context.Context, job *jobs.Job) error {
	var docID int64
	err := p.Store.Index.QueryRow(`SELECT id FROM documents WHERE root = ? AND relpath = ?`, job.Root, job.RelPath).Scan(&docID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	ids, err := p.Store.ChunkIDsNeedingEmbedding(docID, p.Embedder.ID())
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	texts := make([]string, len(ids))
	for i, id := range ids {
		t, err := p.Store.ChunkText(id)
		if err != nil {
			return err
		}
		texts[i] = t
	}
	p.embedMu.Lock()
	defer p.embedMu.Unlock()
	vectors, err := p.Embedder.Embed(ctx, texts)
	if err != nil {
		return err
	}
	for i, id := range ids {
		if err := p.Store.PutEmbedding(id, p.Embedder.ID(), vectors[i]); err != nil {
			return err
		}
	}
	return nil
}

// RequeueStaleEmbeddings queues index work for documents whose embeddings
// were marked stale (e.g. the embedding model changed). Synchronization is
// not involved.
func (p *Pipeline) RequeueStaleEmbeddings(ctx context.Context) (int, error) {
	rows, err := p.Store.Index.QueryContext(ctx, `SELECT DISTINCT d.root, d.relpath, d.hash
		FROM documents d JOIN chunks c ON c.doc_id = d.id
		LEFT JOIN embeddings e ON e.chunk_id = c.id
		WHERE e.chunk_id IS NULL OR e.stale = 1 OR e.model <> ? OR c.chunkver <> ?`,
		p.Embedder.ID(), store.ChunkVersion)
	if err != nil {
		return 0, err
	}
	type row struct{ root, relpath, hash string }
	var pending []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.root, &r.relpath, &r.hash); err != nil {
			rows.Close()
			return 0, err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, r := range pending {
		if err := p.Queue.Enqueue(ctx, jobs.KindIndex, r.root, r.relpath, r.hash, 0); err != nil {
			return 0, err
		}
	}
	return len(pending), nil
}

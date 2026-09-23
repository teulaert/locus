package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/jobs"
	"github.com/teulaert/locus/internal/store"
)

type fixture struct {
	root string
	st   *store.Store
	q    *jobs.Queue
	p    *Pipeline
	ctx  context.Context
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	q := jobs.NewQueue(st.Index)
	f := &fixture{root: root, st: st, q: q, ctx: context.Background()}
	f.p = &Pipeline{Store: st, Queue: q, Embedder: &embed.Hashing{}, Gate: &Gate{}, Log: nil}
	return f
}

func (f *fixture) write(t *testing.T, rel, content string) string {
	t.Helper()
	full := filepath.Join(f.root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	hash, _, err := fswatch.HashFile(full)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func (f *fixture) runQueue(t *testing.T) {
	t.Helper()
	for {
		job, err := f.q.Claim(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			return
		}
		if err := f.p.Process(f.ctx, job); err != nil {
			if err := f.q.Fail(f.ctx, job.ID, err); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := f.q.Complete(f.ctx, job.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPipelineIndexesAndEmbeds(t *testing.T) {
	f := newFixture(t)
	hash := f.write(t, "notes.md", "# Quarterly results\n\nRevenue grew twelve percent.\n\nCosts stayed flat.\n")
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "notes.md", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)

	stats, err := f.st.GetIndexStats(embed.ModelID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 1 || stats.Chunks == 0 {
		t.Fatalf("index stats = %+v", stats)
	}
	if stats.Embeddings != stats.Chunks {
		t.Fatalf("every chunk should be embedded: %+v", stats)
	}
	// Extracted text must be searchable through FTS.
	var hits int
	if err := f.st.Index.QueryRow(`SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'Revenue'`).Scan(&hits); err != nil || hits == 0 {
		t.Fatalf("fts missing extracted text: %d (%v)", hits, err)
	}
	// Section provenance must be preserved.
	var section string
	f.st.Index.QueryRow(`SELECT section FROM chunks LIMIT 1`).Scan(&section)
	if section != "Quarterly results" {
		t.Fatalf("section provenance = %q", section)
	}
	// Re-indexing the same hash must be a no-op.
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "notes.md", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	again, err := f.st.GetIndexStats(embed.ModelID)
	if err != nil || again.Chunks != stats.Chunks {
		t.Fatalf("idempotency violated: %+v vs %+v (%v)", again, stats, err)
	}
}

func TestObsoleteWorkIsDropped(t *testing.T) {
	f := newFixture(t)
	hash := f.write(t, "temp.txt", "intermediate state that will be deleted")
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "temp.txt", hash, 0); err != nil {
		t.Fatal(err)
	}
	// The file disappears before the job runs: work must be dropped, not
	// turned into an error, and must not leave derived state behind.
	if err := os.Remove(filepath.Join(f.root, "temp.txt")); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	stats, err := f.st.GetIndexStats(embed.ModelID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 0 {
		t.Fatalf("obsolete work created derived state for a deleted file: %+v", stats)
	}
	pending, _ := f.q.PendingCount(f.ctx)
	if pending != 0 {
		t.Fatalf("obsolete job not drained: %d", pending)
	}
}

func TestSupersededWorkSkipped(t *testing.T) {
	f := newFixture(t)
	oldHash := f.write(t, "doc.txt", "version one")
	newHash := f.write(t, "doc.txt", "version two is longer and different")
	// A stale job (old hash) must be skipped silently; only the newest
	// version's content should end up indexed.
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "doc.txt", oldHash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	var stored string
	f.st.Index.QueryRow(`SELECT hash FROM documents WHERE relpath = 'doc.txt'`).Scan(&stored)
	if stored == oldHash && stored != newHash {
		t.Fatal("stale version was indexed as if current")
	}
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "doc.txt", newHash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	f.st.Index.QueryRow(`SELECT hash FROM documents WHERE relpath = 'doc.txt'`).Scan(&stored)
	if stored != newHash {
		t.Fatalf("newest version not indexed: %q != %q", stored, newHash)
	}
}

func TestBinaryFilesStillSynchronizeWithoutExtraction(t *testing.T) {
	f := newFixture(t)
	hash := f.write(t, "image.bin", string([]byte{0x00, 0x01, 0xff, 0xfe}))
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "image.bin", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	pending, _ := f.q.PendingCount(f.ctx)
	if pending != 0 {
		t.Fatal("non-extractable file must not wedge the queue")
	}
	list, _ := f.q.List(f.ctx, 10)
	for _, j := range list {
		if j.State == "failed" {
			t.Fatalf("extraction failure blocked the queue: %+v", j)
		}
	}
}

func TestUnindexRemovesDerivedState(t *testing.T) {
	f := newFixture(t)
	hash := f.write(t, "a.txt", "some text that gets indexed")
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "a.txt", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	if err := f.q.Enqueue(f.ctx, jobs.KindUnindex, f.root, "a.txt", "", 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	stats, _ := f.st.GetIndexStats(embed.ModelID)
	if stats.Documents != 0 || stats.Chunks != 0 {
		t.Fatalf("unindex left state behind: %+v", stats)
	}
}

func TestGateBlocksClaimingWhileSearchRuns(t *testing.T) {
	g := &Gate{}
	if g.Busy() {
		t.Fatal("fresh gate must be idle")
	}
	leave := g.Enter()
	if !g.Busy() {
		t.Fatal("Enter must mark interactive work as in flight")
	}
	leave()
	if g.Busy() {
		t.Fatal("Leave must release the gate")
	}
	// A nil gate must behave like an idle one (workers keep running).
	var nilGate *Gate
	if nilGate.Busy() {
		t.Fatal("nil gate must never block workers")
	}
}

func TestRequeueStaleEmbeddingsAfterModelChange(t *testing.T) {
	f := newFixture(t)
	hash := f.write(t, "doc.txt", "text about photovoltaics and storage batteries")
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "doc.txt", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	before, _ := f.q.PendingCount(f.ctx)
	if before != 0 {
		t.Fatalf("queue should be empty before the model change: %d", before)
	}
	if n, err := f.st.MarkEmbeddingsStale("other-model-v9"); err != nil || n == 0 {
		t.Fatalf("stale marking: %d (%v)", n, err)
	}
	n, err := f.p.RequeueStaleEmbeddings(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("model change must queue regeneration")
	}
	// Work must complete for the new model.
	f.p.Embedder = &embed.Hashing{}
	f.runQueue(t)
	if err := f.st.MetaSet(f.st.Index, "embedding.model", embed.ModelID); err != nil {
		t.Fatal(err)
	}
	stats, _ := f.st.GetIndexStats(embed.ModelID)
	if stats.Stale != 0 {
		t.Fatalf("embeddings not regenerated: %+v", stats)
	}
}

func TestProcessUnknownKindIsError(t *testing.T) {
	f := newFixture(t)
	err := f.p.Process(f.ctx, &jobs.Job{Kind: "nonsense", Root: f.root, RelPath: "x"})
	if err == nil || !strings.Contains(err.Error(), "unknown job kind") {
		t.Fatalf("expected unknown-kind error, got %v", err)
	}
}

package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
			if err := f.q.Fail(f.ctx, job, err); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := f.q.Complete(f.ctx, job); err != nil {
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
	// The file was processed at this version: reconciliation must not see it
	// as needing index work again, or it is re-extracted on every pass.
	needs, err := f.st.NeedsIndex(f.root, "image.bin", hash)
	if err != nil || needs {
		t.Fatalf("non-extractable file still needs indexing after processing: %v (%v)", needs, err)
	}
	stats, err := f.st.GetIndexStats(embed.ModelID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.NoText != 1 || stats.Documents != 0 || stats.Chunks != 0 {
		t.Fatalf("non-extractable file should count as no_text only: %+v", stats)
	}
	var reason string
	f.st.Index.QueryRow(`SELECT no_text FROM documents WHERE relpath = 'image.bin'`).Scan(&reason)
	if reason != store.NoTextNoExtractor {
		t.Fatalf("no_text reason = %q, want %q", reason, store.NoTextNoExtractor)
	}
	// New content is a new version and must be looked at again.
	changed := f.write(t, "image.bin", string([]byte{0x00, 0x02, 0xff, 0xfe}))
	if needs, _ := f.st.NeedsIndex(f.root, "image.bin", changed); !needs {
		t.Fatal("changed non-extractable file must need indexing again")
	}
}

func TestTextThatBecomesUnextractableDropsChunks(t *testing.T) {
	f := newFixture(t)
	hash := f.write(t, "notes.txt", "searchable words about invoices")
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "notes.txt", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	// Rewritten as non-UTF-8 bytes: the old chunks must not stay searchable.
	hash = f.write(t, "notes.txt", string([]byte{0xff, 0xfe, 0x00, 0x81}))
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "notes.txt", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	stats, err := f.st.GetIndexStats(embed.ModelID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Chunks != 0 || stats.Documents != 0 || stats.NoText != 1 {
		t.Fatalf("stale chunks survived a version without text: %+v", stats)
	}
	if needs, _ := f.st.NeedsIndex(f.root, "notes.txt", hash); needs {
		t.Fatal("text-free version still needs indexing after processing")
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

// damagedPDF returns a PDF that opens (header, xref and trailer are intact)
// but whose xref entry for the page tree points at the font object, so
// ledongthuc/pdf panics as soon as the page tree is resolved.
func damagedPDF() string {
	var b strings.Builder
	offsets := map[int]int{}
	obj := func(num int, body string) {
		offsets[num] = b.Len()
		fmt.Fprintf(&b, "%d 0 obj\n%s\nendobj\n", num, body)
	}
	b.WriteString("%PDF-1.4\n")
	obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
	obj(2, "<< /Type /Pages /Kids [] /Count 0 >>")
	obj(3, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
	xref := b.Len()
	b.WriteString("xref\n0 4\n0000000000 65535 f \n")
	for _, n := range []int{1, 3, 3} { // object 2 misdirected to object 3
		fmt.Fprintf(&b, "%010d 00000 n \n", offsets[n])
	}
	fmt.Fprintf(&b, "trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", xref)
	return b.String()
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func jobFor(t *testing.T, q *jobs.Queue, rel string) *jobs.Job {
	t.Helper()
	list, err := q.List(context.Background(), 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range list {
		if j.RelPath == rel {
			return j
		}
	}
	return nil
}

// A PDF that makes the parser panic must not take the worker (and with it
// the daemon) down: the job is parked as failed with the reason recorded,
// nothing is indexed for it, and other work keeps flowing.
func TestPanickingPDFIsParkedAndWorkerSurvives(t *testing.T) {
	f := newFixture(t)
	badHash := f.write(t, "damaged.pdf", damagedPDF())
	goodHash := f.write(t, "notes.md", "# Survivor\n\nIndexing continues after a parser crash.\n")
	// The damaged PDF runs first so the good document proves the worker lived.
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "damaged.pdf", badHash, 9); err != nil {
		t.Fatal(err)
	}
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "notes.md", goodHash, 0); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() { defer close(done); f.p.Run(ctx, 1) }()
	defer func() { cancel(); <-done }()

	waitFor(t, 10*time.Second, func() bool {
		n, _ := f.q.PendingCount(f.ctx)
		return n == 0
	}, "the queue to drain past the damaged PDF")

	bad := jobFor(t, f.q, "damaged.pdf")
	if bad == nil || bad.State != jobs.StateFailed {
		t.Fatalf("damaged PDF job should be parked as failed, got %+v", bad)
	}
	if bad.Attempts != 1 {
		t.Fatalf("a parser panic is permanent and must not be retried: attempts = %d", bad.Attempts)
	}
	if !strings.Contains(bad.LastError, "panic") {
		t.Fatalf("failure reason should record the panic: %q", bad.LastError)
	}
	var docs int
	if err := f.st.Index.QueryRow(`SELECT COUNT(*) FROM documents WHERE relpath = 'damaged.pdf'`).Scan(&docs); err != nil || docs != 0 {
		t.Fatalf("damaged PDF must stay unindexed: %d (%v)", docs, err)
	}
	var hits int
	if err := f.st.Index.QueryRow(`SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'continues'`).Scan(&hits); err != nil || hits == 0 {
		t.Fatalf("worker stopped after the panic: other document not indexed (%d, %v)", hits, err)
	}
}

type panickingEmbedder struct{ embed.Hashing }

func (*panickingEmbedder) Embed(context.Context, []string) ([][]float32, error) {
	panic("embedder bug")
}

// Panics outside extraction are recovered by Process as well, so neither the
// background worker nor the foreground drain can be crashed by one job.
func TestProcessRecoversPanicsAsPermanentFailure(t *testing.T) {
	f := newFixture(t)
	f.p.Embedder = &panickingEmbedder{}
	hash := f.write(t, "a.txt", "text that reaches the embedder")
	if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, "a.txt", hash, 0); err != nil {
		t.Fatal(err)
	}
	f.runQueue(t)
	j := jobFor(t, f.q, "a.txt")
	if j == nil || j.State != jobs.StateFailed || j.Attempts != 1 || !strings.Contains(j.LastError, "embedder bug") {
		t.Fatalf("panicking job should be parked after one attempt with the panic recorded: %+v", j)
	}
}

// A ChunkVersion change must re-chunk each text document exactly once and then
// leave the queue empty. Before documents recorded their chunk version,
// NeedsIndex saw the unchanged hash, skipped chunking, and
// RequeueStaleEmbeddings re-enqueued the same documents on every pass (LOC-40).
func TestChunkVersionChangeRechunksOnce(t *testing.T) {
	f := newFixture(t)
	textHash := f.write(t, "doc.txt", "text about photovoltaics and storage batteries")
	imgHash := f.write(t, "image.bin", string([]byte{0x00, 0x01, 0x02, 0x03}))
	for rel, h := range map[string]string{"doc.txt": textHash, "image.bin": imgHash} {
		if err := f.q.Enqueue(f.ctx, jobs.KindIndex, f.root, rel, h, 0); err != nil {
			t.Fatal(err)
		}
	}
	f.runQueue(t)

	// Pretend everything was indexed by the previous chunker.
	old := store.ChunkVersion - 1
	if _, err := f.st.Index.Exec(`UPDATE chunks SET chunkver = ?`, old); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.Index.Exec(`UPDATE documents SET chunkver = ?`, old); err != nil {
		t.Fatal(err)
	}
	chunkIDs := func() string {
		t.Helper()
		var ids string
		if err := f.st.Index.QueryRow(`SELECT COALESCE(GROUP_CONCAT(id), '') FROM chunks`).Scan(&ids); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	before := chunkIDs()
	if needs, _ := f.st.NeedsIndex(f.root, "doc.txt", textHash); !needs {
		t.Fatal("document chunked by an older ChunkVersion must need indexing")
	}
	if needs, _ := f.st.NeedsIndex(f.root, "image.bin", imgHash); needs {
		t.Fatal("a no_text document has no chunks; a chunker change must not re-extract it")
	}

	n, err := f.p.RequeueStaleEmbeddings(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("requeued %d documents after a chunker change, want 1", n)
	}
	f.runQueue(t)
	after := chunkIDs()
	if after == before {
		t.Fatal("document was not re-chunked")
	}
	var stale int
	if err := f.st.Index.QueryRow(`SELECT COUNT(*) FROM chunks WHERE chunkver <> ?`, store.ChunkVersion).Scan(&stale); err != nil || stale != 0 {
		t.Fatalf("%d chunks still carry an old chunk version (%v)", stale, err)
	}

	// Further passes find nothing to do and leave the chunks alone.
	for pass := 0; pass < 3; pass++ {
		n, err := f.p.RequeueStaleEmbeddings(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("pass %d: requeued %d documents after re-chunking", pass, n)
		}
		if pending, _ := f.q.PendingCount(f.ctx); pending != 0 {
			t.Fatalf("pass %d: queue not empty: %d", pass, pending)
		}
		for rel, h := range map[string]string{"doc.txt": textHash, "image.bin": imgHash} {
			if needs, _ := f.st.NeedsIndex(f.root, rel, h); needs {
				t.Fatalf("pass %d: %s still needs indexing", pass, rel)
			}
		}
		f.runQueue(t)
	}
	if got := chunkIDs(); got != after {
		t.Fatal("document was re-chunked more than once")
	}
	stats, err := f.st.GetIndexStats(embed.ModelID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Stale != 0 || stats.Embeddings != stats.Chunks || stats.NoText != 1 {
		t.Fatalf("index not in sync after re-chunking: %+v", stats)
	}
}

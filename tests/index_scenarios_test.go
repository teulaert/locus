package tests

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/search"
	"github.com/teulaert/locus/internal/store"
)

// standaloneApp is a peer without the network daemon: exactly what the CLI
// uses when no `locus serve` is running.
func standaloneApp(t *testing.T, mutate ...func(*config.Config)) (*app.App, string) {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Roots = config.PathRoots(root)
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.Workers = 2
	for _, m := range mutate {
		m(cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a, err := app.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	return a, root
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func indexAndDrain(t *testing.T, a *app.App) {
	t.Helper()
	ctx := context.Background()
	if _, err := a.Reconciler.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Drain(ctx, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
}

// Scenario 4 — 10,000 files are added at once without making search unusable.
func TestScenario04_TenThousandFilesStaySearchable(t *testing.T) {
	if testing.Short() {
		t.Skip("bulk scenario skipped in -short mode")
	}
	a, root := standaloneApp(t)
	const count = 10000
	added := time.Now()
	for i := 0; i < count; i++ {
		writeFile(t, root, fmt.Sprintf("bulk/%04d/note-%04d.txt", i/100, i),
			fmt.Sprintf("Document %d about quarterly budget reviews and invoices, serial zz%04dqq.\n", i, i))
	}
	ctx := context.Background()
	if _, err := a.Reconciler.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Drain(ctx, 15*time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Logf("indexed %d files in %s", count, time.Since(added))

	stats, err := a.Store.GetIndexStats(a.Embedder.ID())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents < count {
		t.Fatalf("only %d/%d documents indexed", stats.Documents, count)
	}
	if stats.Embeddings == 0 {
		t.Fatal("no embeddings generated")
	}

	// Search must stay usable: correct results, bounded latency.
	latencies := []time.Duration{}
	for _, q := range []string{"serial zz0420qq", "budget reviews", "invoice"} {
		start := time.Now()
		results, err := a.Search.Search(ctx, q, search.Options{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, time.Since(start))
		if len(results) == 0 {
			t.Fatalf("query %q returned no results over %d documents", q, count)
		}
	}
	for i, d := range latencies {
		if d > 3*time.Second {
			t.Fatalf("search latency unusable at %d documents: query %d took %s", count, i, d)
		}
	}
	t.Logf("search latency at %d docs: %v", count, latencies)
}

// Scenario 5 — hundreds of newly added files are immediately deleted;
// obsolete queue work is coalesced/cancelled.
func TestScenario05_ImmediateDeletionDropsObsoleteWork(t *testing.T) {
	a, root := standaloneApp(t)
	ctx := context.Background()

	for i := 0; i < 500; i++ {
		writeFile(t, root, fmt.Sprintf("burst/file-%03d.txt", i), "transient content that will vanish")
	}
	// Delete them again before the index pipeline ever gets a chance to run.
	for i := 0; i < 500; i++ {
		if err := os.Remove(filepath.Join(root, fmt.Sprintf("burst/file-%03d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Reconciler.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Drain(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	pending, err := a.Queue.PendingCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("queue did not drain: %d jobs still pending", pending)
	}
	stats, err := a.Store.GetIndexStats(a.Embedder.ID())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 0 {
		t.Fatalf("obsolete work created %d index documents for files that no longer exist", stats.Documents)
	}
	failed := 0
	list, err := a.Queue.List(ctx, 200)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range list {
		if j.State == "failed" {
			failed++
		}
	}
	if failed != 0 {
		t.Fatalf("%d jobs failed instead of being dropped", failed)
	}
}

// Scenario 7 — Locus is killed mid-indexing; restart safely resumes work.
func TestScenario07_KilledMidIndexingResumesSafely(t *testing.T) {
	dirRoot := t.TempDir()
	cfg := config.Default()
	root := filepath.Join(dirRoot, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg.Roots = config.PathRoots(root)
	cfg.DataDir = filepath.Join(dirRoot, "data")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	first, err := app.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	writeFile(t, root, "doc.txt", "content that will be interrupted mid indexing")
	if _, err := first.Reconciler.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	// Leave a job in the running state, exactly like a process killed while
	// it was working on it.
	job, err := first.Queue.Claim(ctx)
	if err != nil || job == nil {
		t.Fatalf("expected queued work to claim: %v", err)
	}
	first.Close()

	// "Restart": a fresh process over the same data directory.
	second, err := app.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	// The index workers recover interrupted jobs when they start, as Start
	// runs them; building the App alone must not (LOC-47).
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		second.Indexer.Run(runCtx, 1)
		close(done)
	}()
	deadline := time.Now().Add(30 * time.Second)
	var stats *store.IndexStats
	for {
		stats, err = second.Store.GetIndexStats(second.Embedder.ID())
		if err != nil {
			t.Fatal(err)
		}
		pending, _ := second.Queue.PendingCount(ctx)
		if (stats.Documents == 1 && stats.Chunks > 0 && pending == 0) || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	<-done
	if stats.Documents != 1 || stats.Chunks == 0 {
		t.Fatalf("interrupted work was not resumed after restart: %+v", stats)
	}
	pending, _ := second.Queue.PendingCount(ctx)
	if pending != 0 {
		t.Fatalf("restart left %d jobs stuck", pending)
	}
	results := searchOrFail(t, second, "interrupt")
	if len(results) == 0 {
		t.Fatal("resumed index is not searchable")
	}
}

// Scenario 8 — The entire search database is deleted; rebuild produces a
// functioning equivalent index.
func TestScenario08_IndexRebuildIsEquivalent(t *testing.T) {
	a, root := standaloneApp(t)
	writeFile(t, root, "notes/a.md", "# Solar\n\nInstallation quotes for photovoltaic modules and warranties.")
	writeFile(t, root, "notes/b.txt", "Meeting minutes about the rooftop installation and the inverter warranty.")
	writeFile(t, root, "misc/c.txt", "unrelated grocery list with bread and garlic")
	indexAndDrain(t, a)

	before := map[string][]string{}
	for _, q := range []string{"photovoltaic modules", "inverter warranty", "solar installation"} {
		results := searchOrFail(t, a, q)
		var paths []string
		for _, r := range results {
			paths = append(paths, r.Path)
		}
		before[q] = paths
		if len(results) == 0 {
			t.Fatalf("query %q had no results before rebuild", q)
		}
	}
	docsBefore, _ := a.Store.GetIndexStats(a.Embedder.ID())

	// Wipe all derived state and rebuild from the files themselves.
	if err := a.RebuildIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := a.Store.GetIndexStats(a.Embedder.ID())
	if err != nil {
		t.Fatal(err)
	}
	if after.Documents != docsBefore.Documents || after.Chunks != docsBefore.Chunks {
		t.Fatalf("rebuild produced a different index shape: %+v vs %+v", after, docsBefore)
	}
	for q, want := range before {
		got := searchOrFail(t, a, q)
		var gotPaths []string
		for _, r := range got {
			gotPaths = append(gotPaths, r.Path)
		}
		if strings.Join(gotPaths, ",") != strings.Join(want, ",") {
			t.Fatalf("query %q: rebuild changed results\n before: %v\n after:  %v", q, want, gotPaths)
		}
	}
	// Synchronization state must be untouched by the rebuild.
	if _, err := a.Store.GetDoc(root, "notes/a.md"); err != nil {
		t.Fatalf("rebuild damaged sync state: %v", err)
	}
}

// Scenario 9 — An embedding model changes; embeddings are safely regenerated
// without affecting synchronization.
func TestScenario09_EmbeddingModelChangeRegenerates(t *testing.T) {
	dirRoot := t.TempDir()
	cfg := config.Default()
	root := filepath.Join(dirRoot, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg.Roots = config.PathRoots(root)
	cfg.DataDir = filepath.Join(dirRoot, "data")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	a, err := app.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "doc.txt", "battery storage and photovoltaic self consumption")
	indexAndDrain(t, a)
	statsBefore, _ := a.Store.GetIndexStats(a.Embedder.ID())
	docBefore, err := a.Store.GetDoc(root, "doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if statsBefore.Embeddings == 0 || statsBefore.Stale != 0 {
		t.Fatalf("expected fresh embeddings: %+v", statsBefore)
	}
	// Remember sync correctness data so we can prove it is untouched.
	versionBefore := docBefore.Version.String()
	hashBefore := docBefore.Hash
	a.Close()

	// Simulate the upgrade path: stored embeddings were produced by an older
	// model, and the recorded model identifier changes.
	st, err := store.Open(filepath.Join(dirRoot, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Index.Exec(`UPDATE embeddings SET model = 'legacy-model-v1'`); err != nil {
		t.Fatal(err)
	}
	if err := st.MetaSet(st.Index, "embedding.model", "legacy-model-v1"); err != nil {
		t.Fatal(err)
	}
	st.Close()

	b, err := app.New(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	statsAfterChange, err := b.Store.GetIndexStats(b.Embedder.ID())
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterChange.Stale == 0 {
		t.Fatalf("model change must mark embeddings stale: %+v", statsAfterChange)
	}
	n, err := b.Indexer.RequeueStaleEmbeddings(context.Background())
	if err != nil || n == 0 {
		t.Fatalf("model change must queue regeneration: %d (%v)", n, err)
	}
	if err := b.Drain(context.Background(), time.Minute); err != nil {
		t.Fatal(err)
	}
	statsDone, _ := b.Store.GetIndexStats(b.Embedder.ID())
	if statsDone.Stale != 0 || statsDone.Embeddings < statsBefore.Embeddings {
		t.Fatalf("embeddings not regenerated: %+v", statsDone)
	}
	// Synchronization state must be byte-identical to before.
	doc, err := b.Store.GetDoc(root, "doc.txt")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Version.String() != versionBefore || doc.Hash != hashBefore {
		t.Fatalf("embedding change must not touch sync state: %s/%s -> %s/%s", versionBefore, hashBefore, doc.Version, doc.Hash)
	}
	if len(searchOrFail(t, b, "photovoltaic")) == 0 {
		t.Fatal("search must work after regeneration")
	}
}

// Scenario 10 — A CPU-only machine works correctly.
func TestScenario10_CPUOnlyPeerWorks(t *testing.T) {
	a, root := standaloneApp(t, func(c *config.Config) { c.Embedding.Device = "cpu" })
	writeFile(t, root, "cpu.txt", "the quick brown fox indexes over lazy dogs")
	indexAndDrain(t, a)
	if a.Device.Used != "cpu" {
		t.Fatalf("device = %+v", a.Device)
	}
	results := searchOrFail(t, a, "quick brown fox")
	if len(results) == 0 || results[0].Path != "cpu.txt" {
		t.Fatalf("cpu-only peer search failed: %+v", results)
	}
}

// Scenario 11 — A supported GPU machine accelerates embedding automatically
// but behaves identically semantically. The runtime seam is asserted here:
// whatever device is requested, the embedder behind the interface must
// produce identical vectors, so an accelerated implementation cannot change
// search semantics.
func TestScenario11_GPUSeamIsSemanticallyIdentical(t *testing.T) {
	texts := []string{
		"solar panel installation quote",
		"insurance policy exclusions",
		"quarterly budget review",
	}
	cpu, _ := embed.Resolve("cpu")
	gpu, dev := embed.Resolve("gpu")
	if cpu == nil || gpu == nil {
		t.Fatal("device resolution must always yield an embedder")
	}
	ctx := context.Background()
	cpuVecs, err := cpu.Embed(ctx, texts)
	if err != nil {
		t.Fatal(err)
	}
	gpuVecs, err := gpu.Embed(ctx, texts)
	if err != nil {
		t.Fatal(err)
	}
	if cpu.ID() == gpu.ID() {
		for i := range cpuVecs {
			for j := range cpuVecs[i] {
				if cpuVecs[i][j] != gpuVecs[i][j] {
					t.Fatal("identical model must yield identical vectors regardless of device")
				}
			}
		}
	}
	t.Logf("device resolution: requested=%s used=%s gpuFound=%v", dev.Requested, dev.Used, dev.GPUFound)
	if !dev.GPUFound && dev.Used != "cpu" {
		t.Fatalf("gpu requested without hardware must fall back cleanly to cpu: %+v", dev)
	}
	// Ordering of similarity results must be identical.
	score := func(vecs [][]float32) float64 { return embed.Cosine(vecs[0], vecs[1]) - embed.Cosine(vecs[0], vecs[2]) }
	if sign(score(cpuVecs)) != sign(score(gpuVecs)) {
		t.Fatal("relative ranking differs between device implementations")
	}
}

func sign(f float64) int {
	switch {
	case f > 0:
		return 1
	case f < 0:
		return -1
	default:
		return 0
	}
}

func searchOrFail(t *testing.T, a *app.App, q string) []*search.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := a.Search.Search(ctx, q, search.Options{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	return results
}

// Queue work must coalesce for repeated modifications of the same path.
func TestQueueCoalescingUnderReconcile(t *testing.T) {
	a, root := standaloneApp(t)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		writeFile(t, root, "changing.txt", fmt.Sprintf("revision %d", i))
	}
	if _, err := a.Reconciler.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	pending, err := a.Queue.PendingCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("expected a single coalesced job, got %d", pending)
	}
	indexAndDrain(t, a)
	results := searchOrFail(t, a, "revision")
	if len(results) == 0 {
		t.Fatal("coalesced work must index the newest revision")
	}
	if !strings.Contains(results[0].Snippet, "revision 49") {
		t.Fatalf("newest revision not indexed: %q", results[0].Snippet)
	}
}

// Unindex jobs must be created for deletions so derived state disappears.
func TestDeletionsRemoveDerivedState(t *testing.T) {
	a, root := standaloneApp(t)
	writeFile(t, root, "temp.txt", "temporary searchable content")
	indexAndDrain(t, a)
	if len(searchOrFail(t, a, "temporary")) == 0 {
		t.Fatal("precondition: document should be searchable")
	}
	os.Remove(filepath.Join(root, "temp.txt"))
	if _, err := a.Reconciler.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Drain(context.Background(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if results := searchOrFail(t, a, "temporary"); len(results) != 0 {
		t.Fatalf("deleted document still searchable: %+v", results)
	}
}

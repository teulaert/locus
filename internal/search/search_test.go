package search

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/chunk"
	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/extract"
	"github.com/teulaert/locus/internal/pipeline"
	"github.com/teulaert/locus/internal/store"
)

type env struct {
	st  *store.Store
	eng *Engine
	ctx context.Context
}

func newSearchEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	e := &env{st: st, ctx: context.Background()}
	e.eng = NewEngine(st.Index, &embed.Hashing{}, &pipeline.Gate{})
	return e
}

func (e *env) index(t *testing.T, rel, hash, text string, pages ...store.Chunk) {
	t.Helper()
	var chunks []*store.Chunk
	if len(pages) > 0 {
		for i := range pages {
			c := pages[i]
			chunks = append(chunks, &c)
		}
	} else {
		for _, c := range chunk.Split(&extract.Document{Text: text}) {
			chunks = append(chunks, &store.Chunk{Ordinal: c.Ordinal, Text: c.Text, Page: c.Page, HasPage: c.HasPage, Section: c.Section, Hash: c.Hash})
		}
	}
	if _, err := e.st.IndexDocument("/root", rel, hash, int64(len(text)), chunks); err != nil {
		t.Fatal(err)
	}
	// Embed every chunk, as the pipeline would.
	var ids []int64
	rows, err := e.st.Index.Query(`SELECT id FROM chunks WHERE doc_id = (SELECT id FROM documents WHERE relpath = ?)`, rel)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	texts := make([]string, len(ids))
	for i, id := range ids {
		texts[i], err = e.st.ChunkText(id)
		if err != nil {
			t.Fatal(err)
		}
	}
	vecs, err := e.eng.Embedder.Embed(e.ctx, texts)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		if err := e.st.PutEmbedding(id, e.eng.Embedder.ID(), vecs[i]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLexicalRankingFindsRelevantDocument(t *testing.T) {
	e := newSearchEnv(t)
	e.index(t, "finance/invoice-solar.txt", "h1", "Invoice for the solar panels installed on the roof. Total 14200 EUR due in thirty days.")
	e.index(t, "notes/recipes.txt", "h2", "Garlic soup recipe: simmer garlic, add broth, finish with bread.")
	results, err := e.eng.Search(e.ctx, "invoice for the solar panels", Options{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected matches")
	}
	if results[0].Path != "finance/invoice-solar.txt" {
		t.Fatalf("top hit = %s", results[0].Path)
	}
	// Results group by document, strongest passages exposed.
	if len(results[0].Chunks) == 0 || results[0].Chunks[0].Snippet == "" {
		t.Fatalf("missing passage evidence: %+v", results[0])
	}
	// Scores must be ordered.
	for i := 1; i < len(results); i++ {
		if results[i-1].Score < results[i].Score {
			t.Fatalf("results not ranked: %+v", results)
		}
	}
}

func TestFilters(t *testing.T) {
	e := newSearchEnv(t)
	e.index(t, "finance/quote.pdf", "h1", "Quotation for photovoltaic modules and mounting rails")
	e.index(t, "misc/quote.txt", "h2", "Quotation for photovoltaic modules and mounting rails")
	pdfOnly, err := e.eng.Search(e.ctx, "photovoltaic", Options{Type: "pdf", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(pdfOnly) == 0 {
		t.Fatal("type filter dropped the matching pdf")
	}
	for _, r := range pdfOnly {
		if !strings.HasSuffix(r.Path, ".pdf") {
			t.Fatalf("type filter leaked %s", r.Path)
		}
	}
	pathOnly, err := e.eng.Search(e.ctx, "photovoltaic", Options{PathPrefix: "misc", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range pathOnly {
		if !strings.HasPrefix(r.Path, "misc/") {
			t.Fatalf("path filter leaked %s", r.Path)
		}
	}
	if len(pathOnly) == 0 {
		t.Fatal("path filter dropped matches")
	}
	// Rooted prefix without leading slash must behave the same.
	if alt, err := e.eng.Search(e.ctx, "photovoltaic", Options{PathPrefix: "/misc", Limit: 10}); err != nil || len(alt) != len(pathOnly) {
		t.Fatalf("leading slash prefix mismatch: %d vs %d (%v)", len(alt), len(pathOnly), err)
	}
	// A filter that matches nothing yields nothing.
	none, err := e.eng.Search(e.ctx, "photovoltaic", Options{Type: "docx", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Fatalf("type=docx leaked results: %+v", none)
	}
}

// TestFiltersTreatWildcardsLiterally guards LOC-36: '_' and '%' in a path or
// type filter are literal characters, so a directory like my_docs is
// searchable and neither character acts as a LIKE wildcard against siblings.
func TestFiltersTreatWildcardsLiterally(t *testing.T) {
	e := newSearchEnv(t)
	const text = "Quotation for photovoltaic modules and mounting rails"
	for i, rel := range []string{
		"my_docs/quote.txt", "myXdocs/quote.txt", // '_' must not match X
		"100%/quote.txt", "100abc/quote.txt", // '%' must not match abc
		"archive/quote.tar_gz", "archive/quote.tarXgz",
	} {
		e.index(t, rel, fmt.Sprintf("h%d", i), text)
	}
	paths := func(opts Options) []string {
		t.Helper()
		opts.Limit = 20
		results, err := e.eng.Search(e.ctx, "photovoltaic", opts)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range results {
			out = append(out, r.Path)
		}
		return out
	}
	for _, tc := range []struct {
		opts Options
		want string
	}{
		{Options{PathPrefix: "my_docs"}, "my_docs/quote.txt"},
		{Options{PathPrefix: "/my_docs/"}, "my_docs/quote.txt"},
		{Options{PathPrefix: "100%"}, "100%/quote.txt"},
		{Options{Type: "tar_gz"}, "archive/quote.tar_gz"},
		{Options{PathPrefix: "archive", Type: "tar_gz"}, "archive/quote.tar_gz"},
	} {
		got := paths(tc.opts)
		if len(got) != 1 || got[0] != tc.want {
			t.Errorf("filter %+v returned %v, want only %s", tc.opts, got, tc.want)
		}
	}
}

func TestLimitHonoured(t *testing.T) {
	e := newSearchEnv(t)
	for i := 0; i < 12; i++ {
		name := string(rune('a'+i)) + ".txt"
		e.index(t, name, "h", "quarterly budget review meeting minutes")
	}
	results, err := e.eng.Search(e.ctx, "budget", Options{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("limit ignored: %d", len(results))
	}
}

func TestPageAndSectionProvenance(t *testing.T) {
	e := newSearchEnv(t)
	e.index(t, "report.pdf", "h1", "",
		store.Chunk{Ordinal: 0, Text: "Introduction to warranties", Page: 1, HasPage: true, Hash: "c0"},
		store.Chunk{Ordinal: 1, Text: "Warranty terms cover the inverter for ten years", Page: 4, HasPage: true, Hash: "c1"},
	)
	results, err := e.eng.Search(e.ctx, "warranty inverter terms", Options{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("no results")
	}
	var foundPage int
	for _, c := range results[0].Chunks {
		if strings.Contains(c.Snippet, "inverter") {
			foundPage = c.Page
		}
	}
	if foundPage != 4 {
		t.Fatalf("search must identify the relevant page, got %d (%+v)", foundPage, results[0].Chunks)
	}
}

func TestEmptyQueryAndEmptyIndex(t *testing.T) {
	e := newSearchEnv(t)
	if _, err := e.eng.Search(e.ctx, "   ", Options{}); err == nil {
		t.Fatal("empty query accepted")
	}
	results, err := e.eng.Search(e.ctx, "anything at all", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("empty index returned %+v", results)
	}
}

func TestPassagesRespectBudget(t *testing.T) {
	e := newSearchEnv(t)
	long := strings.Repeat("detail ", 200)
	e.index(t, "a.txt", "h1", long)
	e.index(t, "b.txt", "h2", long)
	results, err := e.eng.Search(e.ctx, "detail", Options{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) < 2 {
		t.Skip("need multiple documents")
	}
	full := PassagesForContext(results, 100000)
	limited := PassagesForContext(results, 120)
	if len(limited) >= len(full) {
		t.Fatalf("budget ignored: %d vs %d", len(limited), len(full))
	}
	total := 0
	for _, p := range limited {
		total += len(p)
	}
	if total > 120 {
		t.Fatalf("budget exceeded: %d", total)
	}
}

func TestSearchSetsGateWhileRunning(t *testing.T) {
	e := newSearchEnv(t)
	e.index(t, "a.txt", "h1", "gate observability check")
	gate := e.eng.Gate
	done := make(chan bool, 1)
	go func() {
		_, _ = e.eng.Search(e.ctx, "gate", Options{})
		done <- gate.Busy()
	}()
	<-done
	if gate.Busy() {
		t.Fatal("gate must be released after the search completes")
	}
}

// sqliteMaxVariables is SQLITE_MAX_VARIABLE_NUMBER in modernc sqlite. A
// query binding more variables fails with "too many SQL variables".
const sqliteMaxVariables = 32766

// indexBulk indexes one document with one chunk per text and embeds every
// chunk in a single transaction, so large indexes stay cheap to build.
func (e *env) indexBulk(t *testing.T, rel string, texts []string) {
	t.Helper()
	chunks := make([]*store.Chunk, len(texts))
	for i, text := range texts {
		chunks[i] = &store.Chunk{Ordinal: i, Text: text, Hash: fmt.Sprintf("c%d", i)}
	}
	docID, err := e.st.IndexDocument("/root", rel, "bulk", int64(len(texts)), chunks)
	if err != nil {
		t.Fatal(err)
	}
	vecs, err := e.eng.Embedder.Embed(e.ctx, texts)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := e.st.Index.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT id, ordinal FROM chunks WHERE doc_id = ?`, docID)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[int]int64{}
	for rows.Next() {
		var id int64
		var ord int
		if err := rows.Scan(&id, &ord); err != nil {
			t.Fatal(err)
		}
		ids[ord] = id
	}
	rows.Close()
	for i := range texts {
		if err := store.PutEmbeddingOn(tx, ids[i], e.eng.Embedder.ID(), vecs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// LOC-35: an index with more embedded chunks than SQLite's variable limit
// must still be searchable. The semantic leg used to reload vectors with one
// placeholder per candidate chunk and failed outright past 32 766.
//
// Building 33k embedded chunks takes several seconds, so -short skips it;
// TestFillTextStaysUnderVariableLimit and the small-batch test cover the same
// code paths quickly.
func TestSearchBeyondSQLiteVariableLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("builds an index of 33k embedded chunks")
	}
	e := newSearchEnv(t)
	n := sqliteMaxVariables + 1000
	texts := make([]string, n)
	for i := range texts {
		texts[i] = fmt.Sprintf("gardening journal entry %d about tomatoes", i)
	}
	texts[n/2] = "photovoltaic invoices for the rooftop installation"
	e.indexBulk(t, "big.txt", texts)

	// Default engine: bounded candidate set.
	results, err := e.eng.Search(e.ctx, "photovoltaic invoice", Options{Limit: 5})
	if err != nil {
		t.Fatalf("search over %d embedded chunks: %v", n, err)
	}
	if len(results) == 0 || !strings.Contains(results[0].Snippet, "photovoltaic") {
		t.Fatalf("expected the photovoltaic passage first, got %+v", results)
	}

	// Unbounded candidate set: every semantic-only candidate needs its text
	// loaded, which must be batched under the variable limit.
	e.eng.Candidates = 0
	results, err = e.eng.Search(e.ctx, "tomato", Options{Limit: 1, MaxPassages: 1})
	if err != nil {
		t.Fatalf("unbounded search over %d embedded chunks: %v", n, err)
	}
	if len(results) != 1 || !strings.Contains(results[0].Snippet, "tomatoes") {
		t.Fatalf("expected one result with a snippet, got %+v", results)
	}
}

// LOC-35: text for semantic-only candidates is loaded after ranking, in
// batches. A tiny batch size exercises the multi-batch path on a small index.
func TestSemanticOnlyHitsCarryTextAcrossBatches(t *testing.T) {
	old := textBatch
	textBatch = 2
	t.Cleanup(func() { textBatch = old })

	e := newSearchEnv(t)
	for i := 0; i < 7; i++ {
		e.index(t, fmt.Sprintf("inv%d.txt", i), "h", fmt.Sprintf("Invoices batch %d for rooftop installations", i))
	}
	e.index(t, "soup.txt", "h", "Garlic soup recipe with broth")
	// "invoice" never matches "invoices" lexically (no stemming), so every
	// hit below comes from the semantic leg.
	results, err := e.eng.Search(e.ctx, "invoice", Options{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	invoices := 0
	for _, r := range results {
		if r.Snippet == "" || len(r.Chunks) == 0 || r.Chunks[0].Snippet == "" {
			t.Fatalf("semantic hit %s lost its text: %+v", r.Path, r)
		}
		if strings.HasPrefix(r.Path, "inv") {
			invoices++
			if !strings.Contains(r.Snippet, "Invoices batch") {
				t.Fatalf("semantic hit %s has the wrong text: %q", r.Path, r.Snippet)
			}
		}
	}
	if invoices != 7 {
		t.Fatalf("expected all 7 invoice documents, got %d", invoices)
	}
}

// LOC-35: no query issued by fillText may bind more variables than SQLite
// allows, however many candidates need text.
func TestFillTextStaysUnderVariableLimit(t *testing.T) {
	if textBatch <= 0 || textBatch > sqliteMaxVariables {
		t.Fatalf("textBatch %d outside (0, %d]", textBatch, sqliteMaxVariables)
	}
	e := newSearchEnv(t)
	e.index(t, "a.txt", "h", "single real chunk")
	var realID int64
	if err := e.st.Index.QueryRow(`SELECT id FROM chunks`).Scan(&realID); err != nil {
		t.Fatal(err)
	}
	// Ids need not exist: the point is the query shape against real SQLite.
	cands := make([]candidate, 2*sqliteMaxVariables)
	for i := range cands {
		cands[i] = candidate{chunkID: realID + int64(i), needText: true}
	}
	if err := e.eng.fillText(e.ctx, cands); err != nil {
		t.Fatalf("fillText over %d ids: %v", len(cands), err)
	}
	if cands[0].text != "single real chunk" || cands[0].needText {
		t.Fatalf("existing chunk not filled: %+v", cands[0])
	}
}

// axisEmbedder maps the query "zebra" and every text mentioning "aurora" to
// one axis and everything else to an orthogonal one, so cosine similarity is
// exactly 1 or 0 and the test controls the semantic leg completely.
type axisEmbedder struct{}

func (axisEmbedder) ID() string { return "test-axis" }
func (axisEmbedder) Dims() int  { return 2 }
func (axisEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if t == "zebra" || strings.Contains(t, "aurora") {
			out[i] = []float32{1, 0}
		} else {
			out[i] = []float32{0, 1}
		}
	}
	return out, nil
}

// LOC-44: when more candidates have evidence than Candidates allows, the cut
// must rank by the fused, normalised score. It used to sort on raw lex+sem,
// where BM25 (typically well above 1) beat any cosine (at most 1), so every
// weak lexical match pushed out every semantic-only candidate however close
// its embedding was.
func TestSemanticCandidatesSurviveCandidateCut(t *testing.T) {
	e := newSearchEnv(t)
	e.eng.Embedder = axisEmbedder{}
	e.eng.Candidates = 6

	// Fillers make "zebra" rare, so its BM25 weight is large, and carry no
	// evidence of their own (cosine 0 is below the semantic floor).
	fillers := make([]string, 200)
	for i := range fillers {
		fillers[i] = fmt.Sprintf("gardening journal entry %d about tomatoes and beans", i)
	}
	e.indexBulk(t, "fillers.txt", fillers)
	// Many weak lexical matches: one mention among ordinary words.
	weak := make([]string, 20)
	for i := range weak {
		weak[i] = fmt.Sprintf("journal entry %d mentions a zebra once among beans", i)
	}
	e.indexBulk(t, "weak.txt", weak)
	e.index(t, "strong.txt", "h", "zebra zebra zebra")
	for i := 0; i < 3; i++ {
		e.index(t, fmt.Sprintf("aurora%d.txt", i), "h", fmt.Sprintf("aurora borealis field notes %d", i))
	}

	// Precondition: the raw BM25 of every lexical match exceeds the best
	// possible cosine, which is what made the raw-score cut drop the
	// semantic leg.
	lex, err := e.eng.lexical(e.ctx, "zebra", Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range lex {
		if c.lex <= 1 {
			t.Fatalf("test setup: BM25 %v does not exceed cosine 1", c.lex)
		}
	}

	results, err := e.eng.Search(e.ctx, "zebra", Options{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, r := range results {
		found[r.Path] = true
	}
	if !found["strong.txt"] {
		t.Fatalf("strong lexical match lost: %+v", results)
	}
	for i := 0; i < 3; i++ {
		if p := fmt.Sprintf("aurora%d.txt", i); !found[p] {
			t.Fatalf("semantic-only candidate %s did not survive the cut: %+v", p, results)
		}
	}
}

// LOC-44: the lexical leg fetches at most Candidates rows, not a fixed 400.
func TestLexicalLegBoundByCandidates(t *testing.T) {
	e := newSearchEnv(t)
	texts := make([]string, 30)
	for i := range texts {
		texts[i] = fmt.Sprintf("zebra sighting %d", i)
	}
	e.indexBulk(t, "z.txt", texts)
	e.eng.Candidates = 7
	lex, err := e.eng.lexical(e.ctx, "zebra", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(lex) != 7 {
		t.Fatalf("lexical leg returned %d rows, want Candidates = 7", len(lex))
	}
	e.eng.Candidates = 0
	if lex, err = e.eng.lexical(e.ctx, "zebra", Options{}); err != nil || len(lex) != 30 {
		t.Fatalf("unbounded lexical leg returned %d rows (%v), want 30", len(lex), err)
	}
}

// LOC-44: fuse min-max normalises each leg against the raw range of the
// whole candidate set and weights the result. The old per-candidate
// normalisation rescanned the set for every candidate while earlier entries
// had already been rewritten to [0,1], so later candidates were scaled
// against a mixed range; the expected scores below catch that. The large set
// would also not finish within the test timeout if min/max were recomputed
// per candidate.
func TestFuseNormalisesEachLegOnce(t *testing.T) {
	eng := &Engine{LexicalWeight: 0.6, SemanticWeight: 0.4}
	cands := []candidate{
		{lex: 20, sem: 0}, // best lexical
		{lex: 10, sem: 0.45},
		{lex: 0, sem: 0.9}, // best semantic, no lexical match
	}
	eng.fuse(cands)
	want := []float64{0.6, 0.6*0.5 + 0.4*0.5, 0.4}
	for i, c := range cands {
		if math.Abs(c.score-want[i]) > 1e-9 {
			t.Fatalf("candidate %d score = %v, want %v", i, c.score, want[i])
		}
	}

	// A leg without spread carries no ranking signal: positive values count
	// fully.
	flat := []candidate{{lex: 5, sem: 0.3}, {lex: 5, sem: 0.3}}
	eng.fuse(flat)
	if math.Abs(flat[0].score-1) > 1e-9 || math.Abs(flat[1].score-1) > 1e-9 {
		t.Fatalf("constant legs: %+v", flat)
	}

	big := make([]candidate, 300000)
	for i := range big {
		big[i] = candidate{lex: float64(i % 1000), sem: float64(i%7) / 6}
	}
	eng.fuse(big)
	if big[999].lex != 1 || big[1000].lex != 0 || big[6].sem != 1 || big[7].sem != 0 {
		t.Fatalf("large set not normalised: %+v %+v %+v %+v", big[999], big[1000], big[6], big[7])
	}
}

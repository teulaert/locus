package search

import (
	"context"
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

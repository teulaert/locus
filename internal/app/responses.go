package app

import (
	"context"

	"github.com/teulaert/locus/internal/search"
	"github.com/teulaert/locus/internal/store"
)

// The types in this file are the JSON responses the CLI (`--json`) and the
// HTTP API both encode. They are built here, once, so the two front ends
// cannot drift apart, and their constructors guarantee that every documented
// collection is an empty array or object rather than null.

// IndexStatus is `locus index status --json` and GET /api/v1/index.
type IndexStatus struct {
	Index *store.IndexStats `json:"index"`
	// Jobs counts queued jobs as kind -> state -> count.
	Jobs  map[string]map[string]int `json:"jobs"`
	Model string                    `json:"model"`
}

// NewIndexStatus builds an IndexStatus whose jobs map is never nil.
func NewIndexStatus(idx *store.IndexStats, jobs map[string]map[string]int, model string) *IndexStatus {
	return &IndexStatus{Index: idx, Jobs: nonNilJobs(jobs), Model: model}
}

// nonNilJobs keeps the documented jobs object as {} instead of null.
func nonNilJobs(jobs map[string]map[string]int) map[string]map[string]int {
	if jobs == nil {
		return map[string]map[string]int{}
	}
	return jobs
}

// IndexStatus reports derived index counters and job queue state.
func (a *App) IndexStatus(ctx context.Context) (*IndexStatus, error) {
	model := a.Embedder.ID()
	idx, err := a.Store.GetIndexStats(model)
	if err != nil {
		return nil, err
	}
	jobs, err := a.Queue.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return NewIndexStatus(idx, jobs, model), nil
}

// SearchResponse is `locus search --json` and GET /api/v1/search.
type SearchResponse struct {
	Query   string           `json:"query"`
	Results []*search.Result `json:"results"`
}

// NewSearchResponse builds a SearchResponse whose results are never nil.
func NewSearchResponse(query string, results []*search.Result) *SearchResponse {
	if results == nil {
		results = []*search.Result{}
	}
	return &SearchResponse{Query: query, Results: results}
}

// Provenance identifies the passage behind one context block.
type Provenance struct {
	Path    string  `json:"path"`
	Root    string  `json:"root"`
	Ordinal int     `json:"ordinal"`
	Page    int     `json:"page,omitempty"`
	Section string  `json:"section,omitempty"`
	Score   float64 `json:"score"`
	Start   *int    `json:"start,omitempty"`
}

// ContextSearchResponse is `locus search --context --json` and
// GET /api/v1/search?context=1: the plain search response plus compact
// passages for an LLM context window and the provenance of every matching
// passage.
type ContextSearchResponse struct {
	SearchResponse
	Context    []string     `json:"context"`
	Provenance []Provenance `json:"provenance"`
}

// NewContextSearchResponse builds a ContextSearchResponse from search results,
// keeping context passages within budget characters. No collection is nil.
func NewContextSearchResponse(query string, results []*search.Result, budget int) *ContextSearchResponse {
	blocks := search.PassagesForContext(results, budget)
	if blocks == nil {
		blocks = []string{}
	}
	prov := []Provenance{}
	for _, r := range results {
		for _, c := range r.Chunks {
			prov = append(prov, Provenance{
				Path: r.Path, Root: r.Root, Ordinal: c.Ordinal,
				Page: c.Page, Section: c.Section, Score: c.Score, Start: c.Start,
			})
		}
	}
	return &ContextSearchResponse{
		SearchResponse: *NewSearchResponse(query, results),
		Context:        blocks,
		Provenance:     prov,
	}
}

// DefaultContextBudget is the character budget for context passages when the
// caller does not choose one.
const DefaultContextBudget = 8000

// SearchDocuments runs a search and returns the documented response.
func (a *App) SearchDocuments(ctx context.Context, query string, opts search.Options) (*SearchResponse, error) {
	results, err := a.Search.Search(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	return NewSearchResponse(query, results), nil
}

// SearchWithContext runs a search and returns its results together with
// compact context passages (within budget characters, DefaultContextBudget
// when budget <= 0) and their provenance.
func (a *App) SearchWithContext(ctx context.Context, query string, opts search.Options, budget int) (*ContextSearchResponse, error) {
	if budget <= 0 {
		budget = DefaultContextBudget
	}
	results, err := a.Search.Search(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	return NewContextSearchResponse(query, results, budget), nil
}

// Package search implements hybrid retrieval over the local derived index:
// FTS5/BM25 lexical evidence, embedding similarity and metadata signals,
// grouped by document with the strongest passages exposed.
package search

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/pipeline"
	"github.com/teulaert/locus/internal/store"
)

// Options narrow a search.
type Options struct {
	// PathPrefix restricts results to a path prefix (slash form).
	PathPrefix string
	// Type restricts to a file extension, e.g. "pdf".
	Type  string
	Limit int
	// MaxPassages caps chunks exposed per document.
	MaxPassages int
}

// ChunkHit is one matching passage with provenance.
type ChunkHit struct {
	Ordinal int     `json:"ordinal"`
	Page    int     `json:"page,omitempty"`
	Section string  `json:"section,omitempty"`
	Score   float64 `json:"score"`
	Snippet string  `json:"snippet"`
	Start   int     `json:"start"`
}

// Result is one matching document.
type Result struct {
	Root    string     `json:"root"`
	Path    string     `json:"path"`
	Score   float64    `json:"score"`
	Chunks  []ChunkHit `json:"chunks"`
	Snippet string     `json:"snippet"`
}

// Engine runs hybrid searches against the derived index.
type Engine struct {
	DB       *sql.DB
	Embedder embed.Embedder
	Gate     *pipeline.Gate

	// LexicalWeight/SemanticWeight are internal and may evolve.
	LexicalWeight  float64
	SemanticWeight float64
	// Candidates bounds the reranked candidate set.
	Candidates int
	// SemanticFloor is the minimum cosine similarity a semantic-only
	// candidate must reach to be reported. Without such a floor an embedding
	// scan would return every document in the index for any query, which is
	// noise: the lexical leg can be empty simply because the wording differs,
	// and weak cosine scores around zero carry no evidence.
	SemanticFloor float64
}

// NewEngine returns an engine with default ranking weights.
func NewEngine(db *sql.DB, e embed.Embedder, gate *pipeline.Gate) *Engine {
	return &Engine{DB: db, Embedder: e, Gate: gate, LexicalWeight: 0.55, SemanticWeight: 0.45, Candidates: 400, SemanticFloor: 0.05}
}

type candidate struct {
	chunkID int64
	root    string
	relpath string
	ordinal int
	page    sql.NullInt64
	section string
	text    string
	lex     float64
	sem     float64
	score   float64
}

// Search runs a hybrid query. Results group by document.
func (e *Engine) Search(ctx context.Context, query string, opts Options) ([]*Result, error) {
	if e.Gate != nil {
		leave := e.Gate.Enter()
		defer leave()
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("search: empty query")
	}
	if opts.Limit <= 0 {
		opts.Limit = 10
	}
	if opts.MaxPassages <= 0 {
		opts.MaxPassages = 3
	}
	cands, err := e.lexical(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	semCandidates, err := e.semanticCandidates(ctx, cands, opts)
	if err != nil {
		return nil, err
	}
	if err := e.scoreSemantic(ctx, query, semCandidates); err != nil {
		return nil, err
	}
	seen := map[int64]bool{}
	for _, c := range cands {
		seen[c.chunkID] = true
	}
	for _, c := range semCandidates {
		if !seen[c.chunkID] {
			cands = append(cands, c)
		}
	}
	// Only candidates with actual evidence survive: a lexical match, or a
	// semantic similarity above the floor.
	kept := cands[:0]
	for _, c := range cands {
		if c.lex > 0 || c.sem >= e.SemanticFloor {
			kept = append(kept, c)
		}
	}
	cands = kept
	if len(cands) == 0 {
		return nil, nil
	}
	if e.Candidates > 0 && len(cands) > e.Candidates {
		sort.SliceStable(cands, func(i, j int) bool {
			return cands[i].lex+cands[i].sem > cands[j].lex+cands[j].sem
		})
		cands = cands[:e.Candidates]
	}
	for i := range cands {
		cands[i].lex = normalize(cands[i].lex, cands, true)
		cands[i].sem = normalize(cands[i].sem, cands, false)
		cands[i].score = e.LexicalWeight*cands[i].lex + e.SemanticWeight*cands[i].sem
	}
	return group(cands, opts.Limit, opts.MaxPassages, query), nil
}

// lexical runs the FTS5/BM25 leg.
func (e *Engine) lexical(ctx context.Context, query string, opts Options) ([]candidate, error) {
	match := buildMatch(query)
	if match == "" {
		return nil, nil
	}
	q := `SELECT c.id, d.root, d.relpath, c.ordinal, c.page, COALESCE(c.section, ''), c.text, bm25(fts_chunks) AS rank
		FROM fts_chunks
		JOIN chunks c ON c.id = fts_chunks.rowid
		JOIN documents d ON d.id = c.doc_id
		WHERE fts_chunks MATCH ?`
	args := []any{match}
	if p := opts.pathFilter(); p != "" {
		q += ` AND (d.relpath LIKE ? OR d.relpath = ?)`
		args = append(args, escapeLike(p)+`/%`, p)
	}
	if t := opts.typeFilter(); t != "" {
		q += ` AND d.relpath LIKE ?`
		args = append(args, `%`+t)
	}
	q += ` ORDER BY rank LIMIT 400`
	rows, err := e.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []candidate
	for rows.Next() {
		var c candidate
		var rank float64
		if err := rows.Scan(&c.chunkID, &c.root, &c.relpath, &c.ordinal, &c.page, &c.section, &c.text, &rank); err != nil {
			return nil, err
		}
		// bm25(): lower (more negative) is better. Invert to higher-is-better.
		c.lex = -rank
		out = append(out, c)
	}
	return out, rows.Err()
}

func buildMatch(query string) string {
	terms := strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !(r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 0x80)
	})
	var parts []string
	seen := map[string]bool{}
	for _, t := range terms {
		if len(t) < 2 || seen[t] {
			continue
		}
		seen[t] = true
		parts = append(parts, `"`+strings.ReplaceAll(t, `"`, "")+`"`)
	}
	return strings.Join(parts, " OR ")
}

// semanticCandidates widens the candidate set to documents matching the
// filters so semantic hits are not limited to lexical matches.
func (e *Engine) semanticCandidates(ctx context.Context, lexical []candidate, opts Options) ([]candidate, error) {
	seen := map[int64]bool{}
	for _, c := range lexical {
		seen[c.chunkID] = true
	}
	q := `SELECT c.id, d.root, d.relpath, c.ordinal, c.page, COALESCE(c.section, ''), c.text
		FROM chunks c JOIN documents d ON d.id = c.doc_id
		JOIN embeddings em ON em.chunk_id = c.id
		WHERE em.stale = 0 AND em.model = ?`
	args := []any{e.Embedder.ID()}
	if p := opts.pathFilter(); p != "" {
		q += ` AND (d.relpath LIKE ? OR d.relpath = ?)`
		args = append(args, escapeLike(p)+`/%`, p)
	}
	if t := opts.typeFilter(); t != "" {
		q += ` AND d.relpath LIKE ?`
		args = append(args, `%`+t)
	}
	q += ` AND em.stale = 0 AND em.model = ? LIMIT ?`
	args = append(args, e.Embedder.ID(), maxSemScan)
	rows, err := e.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.chunkID, &c.root, &c.relpath, &c.ordinal, &c.page, &c.section, &c.text); err != nil {
			return nil, err
		}
		if !seen[c.chunkID] {
			out = append(out, c)
		}
	}
	return out, rows.Err()
}

// maxSemScan bounds the exhaustive cosine scan for reranking. Profiling may
// later replace this with a vector index behind the same abstraction.
const maxSemScan = 50000

func (e *Engine) scoreSemantic(ctx context.Context, query string, cands []candidate) error {
	if len(cands) == 0 {
		return nil
	}
	qv, err := e.Embedder.Embed(ctx, []string{query})
	if err != nil || len(qv) == 0 {
		return err
	}
	ids := make([]int64, len(cands))
	for i, c := range cands {
		ids[i] = c.chunkID
	}
	vecs, err := loadEmbeddings(ctx, e.DB, ids)
	if err != nil {
		return err
	}
	for i, c := range cands {
		if v, ok := vecs[c.chunkID]; ok {
			cands[i].sem = embed.Cosine(qv[0], v)
		}
	}
	return nil
}

func loadEmbeddings(ctx context.Context, db *sql.DB, ids []int64) (map[int64][]float32, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := db.QueryContext(ctx, `SELECT chunk_id, vector FROM embeddings WHERE chunk_id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][]float32{}
	for rows.Next() {
		var id int64
		var buf []byte
		if err := rows.Scan(&id, &buf); err != nil {
			return nil, err
		}
		out[id] = store.DecodeVector(buf)
	}
	return out, rows.Err()
}

func normalize(v float64, all []candidate, lexical bool) float64 {
	var lo, hi float64
	first := true
	for _, c := range all {
		x := c.sem
		if lexical {
			x = c.lex
		}
		if first || x < lo {
			lo = x
		}
		if first || x > hi {
			hi = x
		}
		first = false
	}
	if hi-lo < 1e-9 {
		if v > 0 {
			return 1
		}
		return 0
	}
	return (v - lo) / (hi - lo)
}

func group(cands []candidate, limit, maxPassages int, query string) []*Result {
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].score > cands[j].score })
	byDoc := map[string]*Result{}
	var order []string
	for _, c := range cands {
		r := byDoc[c.relpath]
		if r == nil {
			r = &Result{Root: c.root, Path: c.relpath, Score: c.score}
			byDoc[c.relpath] = r
			order = append(order, c.relpath)
		}
		if len(r.Chunks) < maxPassages {
			page := 0
			if c.page.Valid {
				page = int(c.page.Int64)
			}
			r.Chunks = append(r.Chunks, ChunkHit{
				Ordinal: c.ordinal,
				Page:    page,
				Section: c.section,
				Score:   c.score,
				Snippet: snippet(c.text, query, 280),
				Start:   0,
			})
		}
	}
	var out []*Result
	for _, rel := range order {
		r := byDoc[rel]
		if len(r.Chunks) > 0 {
			r.Snippet = r.Chunks[0].Snippet
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func snippet(text, query string, width int) string {
	lower := strings.ToLower(text)
	terms := strings.Fields(strings.ToLower(query))
	pos := -1
	for _, t := range terms {
		if len(t) < 3 {
			continue
		}
		if i := strings.Index(lower, t); i >= 0 && (pos < 0 || i < pos) {
			pos = i
		}
	}
	if pos < 0 {
		pos = 0
	}
	start := max(0, pos-width/2)
	end := min(len(text), start+width)
	s := strings.TrimSpace(text[start:end])
	if start > 0 {
		s = "…" + s
	}
	if end < len(text) {
		s += "…"
	}
	return s
}

func (o Options) pathFilter() string {
	p := strings.TrimSpace(strings.ReplaceAll(o.PathPrefix, "\\", "/"))
	p = strings.Trim(p, "/")
	if p == "" || p == "." {
		return ""
	}
	return filepath.ToSlash(p)
}

func (o Options) typeFilter() string {
	t := strings.ToLower(strings.TrimSpace(o.Type))
	if t == "" {
		return ""
	}
	if !strings.HasPrefix(t, ".") {
		t = "." + t
	}
	return t
}

func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

// PassagesForContext renders top passages for LLM consumption.
func PassagesForContext(results []*Result, budget int) []string {
	var out []string
	used := 0
	for _, r := range results {
		for _, c := range r.Chunks {
			src := r.Path
			if c.Page > 0 {
				src = fmt.Sprintf("%s#page=%d", src, c.Page)
			} else if c.Section != "" {
				src = fmt.Sprintf("%s#%s", src, c.Section)
			}
			block := fmt.Sprintf("[%s]\n%s", src, c.Snippet)
			n := utf8.RuneCountInString(block)
			if used+n > budget {
				return out
			}
			used += n
			out = append(out, block)
		}
	}
	return out
}

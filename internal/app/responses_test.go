package app

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/search"
	"github.com/teulaert/locus/internal/store"
)

// encodeKeys marshals v and returns its top-level keys, failing on any null.
func encodeKeys(t *testing.T, v any) []string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "null") {
		t.Fatalf("encoded response contains null: %s", b)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// The typed responses the CLI and API both encode keep every collection
// non-nil when there is nothing to report (LOC-23).
func TestResponsesWithNoResultsHaveNoNullCollections(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want []string
	}{
		{"search", NewSearchResponse("q", nil), []string{"query", "results"}},
		{"search with context", NewContextSearchResponse("q", nil, 100), []string{"context", "provenance", "query", "results"}},
		{"index status", NewIndexStatus(&store.IndexStats{}, nil, "model"), []string{"index", "jobs", "model"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := encodeKeys(t, tc.v); !slices.Equal(got, tc.want) {
				t.Fatalf("keys %v, want %v", got, tc.want)
			}
		})
	}
}

// The App methods return the same shapes from a fresh, empty peer.
func TestAppResponsesOnEmptyPeer(t *testing.T) {
	a := testApp(t)
	ctx := context.Background()

	st, err := a.IndexStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	encodeKeys(t, st)

	resp, err := a.SearchWithContext(ctx, "nothing indexed yet", search.Options{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("empty peer returned %d results", len(resp.Results))
	}
	encodeKeys(t, resp)

	status, err := a.BuildStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	encodeKeys(t, status)
}

// Provenance carries one entry per matching passage, with page, section and
// start only when known.
func TestContextSearchResponseProvenance(t *testing.T) {
	start := 42
	results := []*search.Result{{
		Root: "/r", Path: "a.pdf", Score: 1,
		Chunks: []search.ChunkHit{
			{Ordinal: 1, Page: 4, Score: 0.9, Snippet: "invoice", Start: &start},
			{Ordinal: 2, Section: "Terms", Score: 0.5, Snippet: "terms"},
		},
	}}
	resp := NewContextSearchResponse("q", results, 8000)
	if len(resp.Context) != 2 || !strings.HasPrefix(resp.Context[0], "[a.pdf#page=4]") {
		t.Fatalf("context = %q", resp.Context)
	}
	b, err := json.Marshal(resp.Provenance)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"path":"a.pdf","root":"/r","ordinal":1,"page":4,"score":0.9,"start":42},` +
		`{"path":"a.pdf","root":"/r","ordinal":2,"section":"Terms","score":0.5}]`
	if string(b) != want {
		t.Fatalf("provenance\n got %s\nwant %s", b, want)
	}
}

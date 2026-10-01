package chunk

import (
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/extract"
)

func TestSplitMarkdownSections(t *testing.T) {
	text := "# Alpha\n\nfirst body paragraph\n\nsecond paragraph of alpha\n\n# Beta\n\nbeta content here\n"
	doc := &extract.Document{Text: text, Markdown: true}
	chunks := Split(doc)
	if len(chunks) < 2 {
		t.Fatalf("expected sections to split, got %d chunks", len(chunks))
	}
	if chunks[0].Section != "Alpha" {
		t.Fatalf("first chunk section = %q, want Alpha", chunks[0].Section)
	}
	var foundBeta bool
	for _, c := range chunks {
		if c.Section == "Beta" && strings.Contains(c.Text, "beta content") {
			foundBeta = true
		}
		if c.Hash == "" {
			t.Fatal("chunk missing content hash")
		}
	}
	if !foundBeta {
		t.Fatalf("beta section not found in %+v", chunks)
	}
	for i, c := range chunks {
		if c.Ordinal != i {
			t.Fatalf("ordinals not sequential at %d: %+v", i, chunks)
		}
	}
}

func TestSmallParagraphsMergeTowardTargetSize(t *testing.T) {
	// Tiny fragments are coalesced toward the target chunk size instead of
	// becoming a storm of one-sentence chunks.
	doc := &extract.Document{Text: "one\n\ntwo\n\nthree"}
	chunks := Split(doc)
	if len(chunks) != 1 {
		t.Fatalf("expected small paragraphs merged, got %d: %+v", len(chunks), chunks)
	}
	if !strings.Contains(chunks[0].Text, "one") || !strings.Contains(chunks[0].Text, "three") {
		t.Fatalf("merged chunk lost content: %q", chunks[0].Text)
	}
}

func TestLargeParagraphsStaySeparateUpToTarget(t *testing.T) {
	para := strings.Repeat("word ", 600) // ~600 tokens
	doc := &extract.Document{Text: para + "\n\n" + para}
	chunks := Split(doc)
	if len(chunks) < 2 {
		t.Fatalf("expected distinct chunks near target size, got %d", len(chunks))
	}
	for _, c := range chunks {
		if EstimateTokens(c.Text) > MaxTokens {
			t.Fatalf("chunk exceeds target/max: %d tokens", EstimateTokens(c.Text))
		}
	}
}

func TestPageProvenance(t *testing.T) {
	doc := &extract.Document{
		Text: "page one text\n\npage two text",
		Pages: []extract.Page{
			{Num: 1, Text: "page one text"},
			{Num: 2, Text: "page two text"},
		},
	}
	chunks := Split(doc)
	if len(chunks) != 2 {
		t.Fatalf("expected per-page chunks, got %+v", chunks)
	}
	if !chunks[0].HasPage || chunks[0].Page != 1 || chunks[1].Page != 2 {
		t.Fatalf("page provenance lost: %+v", chunks)
	}
}

func TestOversizedBlockIsWindowed(t *testing.T) {
	long := strings.Repeat("filler ", 4000) // far beyond MaxTokens
	doc := &extract.Document{Text: long}
	chunks := Split(doc)
	if len(chunks) < 2 {
		t.Fatalf("expected forced split, got %d chunks", len(chunks))
	}
	for _, c := range chunks {
		if EstimateTokens(c.Text) > MaxTokens+50 {
			t.Fatalf("chunk exceeds max tokens: %d", EstimateTokens(c.Text))
		}
	}
	// Overlap must be present: consecutive windows share content.
	if len(chunks) > 1 {
		a := strings.Fields(chunks[0].Text)
		b := strings.Fields(chunks[1].Text)
		if len(a) < OverlapTokens || len(b) < OverlapTokens {
			t.Fatal("windows too small to overlap")
		}
		tailA := strings.Join(a[len(a)-OverlapTokens:], " ")
		if !strings.Contains(strings.Join(b, " "), tailA[:len(tailA)/2]) {
			t.Fatal("expected overlap between adjacent windows")
		}
	}
}

func TestIdempotentHashes(t *testing.T) {
	doc := &extract.Document{Text: "# T\n\nsome stable text here\n\nmore text"}
	a := Split(doc)
	b := Split(doc)
	if len(a) != len(b) {
		t.Fatalf("non-deterministic chunk count: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Hash != b[i].Hash || a[i].Text != b[i].Text {
			t.Fatalf("chunk %d differs between runs", i)
		}
	}
}

func TestEmptyDocument(t *testing.T) {
	if got := Split(&extract.Document{}); len(got) != 0 {
		t.Fatalf("empty document produced %d chunks", len(got))
	}
	if got := Split(nil); got != nil {
		t.Fatal("nil document must produce no chunks")
	}
}

func TestUnicodeContent(t *testing.T) {
	doc := &extract.Document{Text: "# Über\n\nGrüße, hier ist Text mit Umlauten. 漢字 も 含まれます。"}
	chunks := Split(doc)
	if len(chunks) == 0 {
		t.Fatal("unicode document produced no chunks")
	}
	if !strings.Contains(chunks[0].Text, "Grüße") {
		t.Fatal("unicode content mangled")
	}
}

func TestEstimateTokens(t *testing.T) {
	if n := EstimateTokens("one two three four five"); n < 5 {
		t.Fatalf("estimate too low: %d", n)
	}
	long := strings.Repeat("a", 400)
	if n := EstimateTokens(long); n < 90 {
		t.Fatalf("char-based estimate too low: %d", n)
	}
}

// sectionOf returns the section of the first chunk containing needle.
func sectionOf(t *testing.T, chunks []*Chunk, needle string) string {
	t.Helper()
	for _, c := range chunks {
		if strings.Contains(c.Text, needle) {
			return c.Section
		}
	}
	t.Fatalf("no chunk contains %q: %+v", needle, chunks)
	return ""
}

func TestSetextHeadingKeepsItsTitle(t *testing.T) {
	text := "intro paragraph\n\nInstallation\n============\n\ninstall body text\n\nWarranty terms\n--------------\n\nwarranty body text"
	chunks := Split(&extract.Document{Text: text, Markdown: true})
	if got := sectionOf(t, chunks, "install body"); got != "Installation" {
		t.Fatalf("setext '=' section = %q, want Installation (%+v)", got, chunks)
	}
	if got := sectionOf(t, chunks, "warranty body"); got != "Warranty terms" {
		t.Fatalf("setext '-' section = %q, want Warranty terms (%+v)", got, chunks)
	}
	for _, c := range chunks {
		if strings.Contains(c.Text, "Installation") || strings.Contains(c.Text, "====") {
			t.Fatalf("heading text or underline leaked into a body chunk: %q", c.Text)
		}
	}
}

func TestATXRequiresHashesThenSpace(t *testing.T) {
	text := "# Real\n\nreal body\n\n#hashtag not a heading\n\n####### seven hashes\n\n## Closed ##\n\nclosed body"
	chunks := Split(&extract.Document{Text: text, Markdown: true})
	if got := sectionOf(t, chunks, "real body"); got != "Real" {
		t.Fatalf("section = %q, want Real", got)
	}
	if got := sectionOf(t, chunks, "#hashtag"); got != "Real" {
		t.Fatalf("'#hashtag' must stay body text under Real, got section %q", got)
	}
	if got := sectionOf(t, chunks, "seven hashes"); got != "Real" {
		t.Fatalf("seven '#' is not a heading, got section %q", got)
	}
	if got := sectionOf(t, chunks, "closed body"); got != "Closed" {
		t.Fatalf("closing hashes must be stripped, got %q", got)
	}
}

func TestMarkdownFrontMatterIsNotAHeading(t *testing.T) {
	text := "---\ntitle: Notes\n---\n\nbody before any heading"
	chunks := Split(&extract.Document{Text: text, Markdown: true})
	for _, c := range chunks {
		if c.Section != "" {
			t.Fatalf("front matter produced section %q: %+v", c.Section, chunks)
		}
	}
}

func TestNonMarkdownHashLinesAreNotSections(t *testing.T) {
	cases := map[string]string{
		"shell":  "#!/bin/sh\necho hello\n\n# configure the thing\nmake install",
		"c":      "#include <stdio.h>\n#define N 10\n\nint main(void) { return N; }",
		"css":    "#id {\n  color: red;\n}\n\n.other { color: blue; }",
		"config": "# database settings\nhost = localhost\n\nport = 5432\n=====\ntrailer",
	}
	for name, text := range cases {
		chunks := Split(&extract.Document{Text: text})
		if len(chunks) == 0 {
			t.Fatalf("%s: no chunks", name)
		}
		var all []string
		for _, c := range chunks {
			if c.Section != "" {
				t.Fatalf("%s: non-Markdown line became section %q", name, c.Section)
			}
			all = append(all, c.Text)
		}
		// Nothing is swallowed as a heading: every line stays searchable.
		joined := strings.Join(all, "\n")
		for _, line := range strings.Split(text, "\n") {
			if line = strings.TrimSpace(line); line != "" && !strings.Contains(joined, line) {
				t.Fatalf("%s: line %q dropped from chunks", name, line)
			}
		}
	}
}

package extract

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// withLimit lowers the extraction size limit for one test so oversized
// inputs need not be 64 MB on disk.
func withLimit(t *testing.T, n int64) {
	t.Helper()
	old := maxBytes
	maxBytes = n
	t.Cleanup(func() { maxBytes = old })
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// writeLargePDF writes a PDF with pages pages of a few lines each: big
// enough that extracting all of it takes far longer than the cancellation
// bound the tests assert.
func writeLargePDF(t *testing.T, path string, pages int) {
	t.Helper()
	content := make([][]string, pages)
	for i := range content {
		content[i] = []string{
			fmt.Sprintf("Page %d of the quarterly maintenance log", i+1),
			"Pump station inspected, valves greased, no leaks found.",
			"Next inspection scheduled after the spring thaw.",
		}
	}
	writePDF(t, path, content)
}

func TestPDFExtractionStopsPromptlyWhenCancelled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.pdf")
	writeLargePDF(t, path, 400)

	start := time.Now()
	if _, err := (PDF{}).Extract(testCtx(), path); err != nil {
		t.Fatal(err)
	}
	full := time.Since(start)

	ctx, cancel := context.WithTimeout(testCtx(), 5*time.Millisecond)
	defer cancel()
	start = time.Now()
	doc, err := Run(ctx, PDF{}, path)
	elapsed := time.Since(start)
	if !isCtxErr(err) {
		t.Fatalf("cancelled extraction = %v, %v; want an error wrapping the context error (full parse took %v)", doc, err, full)
	}
	if errors.Is(err, ErrPanic) {
		t.Fatalf("aborted reads must not be reported as a parser panic: %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("cancelled extraction took %v (full parse %v); want it to return promptly", elapsed, full)
	}
}

func TestExtractorsHonourCancelledContext(t *testing.T) {
	dir := t.TempDir()
	big := strings.Repeat("a line of a large log file\n", 1<<20) // ~27 MB
	txt := filepath.Join(dir, "big.log")
	if err := os.WriteFile(txt, []byte(big), 0o600); err != nil {
		t.Fatal(err)
	}
	htmlPath := filepath.Join(dir, "big.html")
	if err := os.WriteFile(htmlPath, []byte("<p>"+big[:1<<20]+"</p>"), 0o600); err != nil {
		t.Fatal(err)
	}
	paras := make([]string, 5000)
	for i := range paras {
		paras[i] = fmt.Sprintf("Paragraph %d of a long report.", i)
	}
	docx := filepath.Join(dir, "big.docx")
	writeDOCX(t, docx, paras)
	pdfPath := filepath.Join(dir, "small.pdf")
	writePDF(t, pdfPath, [][]string{{"hello"}})

	ctx, cancel := context.WithCancel(testCtx())
	cancel()
	for _, tc := range []struct {
		ex   Extractor
		path string
	}{{PlainText{}, txt}, {Markdown{}, txt}, {HTML{}, htmlPath}, {DOCX{}, docx}, {PDF{}, pdfPath}} {
		start := time.Now()
		doc, err := Run(ctx, tc.ex, tc.path)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s with cancelled ctx = %v, %v; want context.Canceled", tc.ex.Name(), doc, err)
		}
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Errorf("%s took %v with a cancelled ctx", tc.ex.Name(), d)
		}
	}
}

func TestRunReportsCancellationNotPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(testCtx())
	cancel()
	_, err := Run(ctx, panickyExtractor{}, "x.panicky")
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrPanic) {
		t.Fatalf("panic after cancellation = %v; want a retryable context error", err)
	}
}

func TestOversizedPlainTextIsTruncatedAtRuneBoundary(t *testing.T) {
	// "€" is three bytes; a 10-byte limit cuts the fourth one in half.
	content := "a€€€€€ tail"
	withLimit(t, 10)
	path := filepath.Join(t.TempDir(), "big.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, ex := range []Extractor{PlainText{}, HTML{}} {
		doc, err := ex.Extract(testCtx(), path)
		if err != nil {
			t.Fatalf("%s: oversized text must be indexed to its cap, got %v", ex.Name(), err)
		}
		if !doc.Truncated {
			t.Errorf("%s: truncation not flagged", ex.Name())
		}
		if !utf8.ValidString(doc.Text) || doc.Text != "a€€€" {
			t.Errorf("%s: text = %q; want %q cut at a rune boundary", ex.Name(), doc.Text, "a€€€")
		}
	}

	exact := filepath.Join(t.TempDir(), "exact.txt")
	if err := os.WriteFile(exact, []byte("a€€€"), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := (PlainText{}).Extract(testCtx(), exact)
	if err != nil || doc.Truncated || doc.Text != "a€€€" {
		t.Fatalf("file exactly at the limit = %+v, %v; want untruncated", doc, err)
	}
}

func TestOversizedDOCXKeepsLeadingParagraphs(t *testing.T) {
	paras := make([]string, 2000)
	for i := range paras {
		paras[i] = fmt.Sprintf("Paragraph number %d.", i)
	}
	path := filepath.Join(t.TempDir(), "big.docx")
	writeDOCX(t, path, paras)
	withLimit(t, 4096)
	doc, err := (DOCX{}).Extract(testCtx(), path)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.Truncated || !strings.HasPrefix(doc.Text, "Paragraph number 0.") || strings.Contains(doc.Text, "Paragraph number 1999.") {
		t.Fatalf("truncated docx = truncated %v, text %.80q...", doc.Truncated, doc.Text)
	}
	if !utf8.ValidString(doc.Text) {
		t.Fatal("truncated docx text is not valid UTF-8")
	}
}

func TestTrimPartialRune(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"abc", "abc"},
		{"ab€", "ab€"},
		{"ab\xe2\x82", "ab"},
		{"ab\xe2", "ab"},
		{"ab\xf0\x9f\x98", "ab"},
		{"ab\xf0\x9f\x98\x80", "ab\xf0\x9f\x98\x80"},
		{"ab\xff", "ab\xff"}, // never valid: left for the caller to reject
	} {
		if got := string(trimPartialRune([]byte(tc.in))); got != tc.want {
			t.Errorf("trimPartialRune(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

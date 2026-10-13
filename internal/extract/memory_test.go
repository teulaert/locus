package extract

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/testutil/synthpdf"
)

// allocated returns the bytes allocated on the heap while f ran.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// LOC-84: pdf.Reader.Page(i) searches the page tree from its root and
// parses every kid before page i, so extracting page by page through it was
// quadratic in the page count: a 1500-page report allocated 3.7 GB. The
// single tree walk keeps allocation linear: four times the pages allocate
// about four times as much, where the quadratic walk allocated sixteen
// times as much.
func TestPDFExtractionAllocatesLinearlyInPages(t *testing.T) {
	dir := t.TempDir()
	cost := func(pages int) uint64 {
		path := filepath.Join(dir, fmt.Sprintf("p%d.pdf", pages))
		if _, err := synthpdf.WriteFile(path, synthpdf.Spec{Pages: pages, LinesPerPage: 20}); err != nil {
			t.Fatal(err)
		}
		var doc *Document
		var err error
		n := allocated(func() { doc, err = (PDF{}).Extract(testCtx(), path) })
		if err != nil || len(doc.Pages) != pages {
			t.Fatalf("%d pages: %v, %d pages extracted", pages, err, len(doc.Pages))
		}
		return n
	}
	small, large := cost(250), cost(1000)
	ratio := float64(large) / float64(small)
	t.Logf("250 pages allocate %d KB, 1000 pages %d KB (x%.1f)", small>>10, large>>10, ratio)
	if ratio > 6 {
		t.Fatalf("4x the pages allocated %.1fx as much; extraction is not linear in the page count", ratio)
	}
}

// Pages under intermediate Pages nodes are numbered in document order, and
// a page tree whose Kids lead back to an ancestor ends in an error rather
// than a walk that never finishes.
func TestPDFNestedAndCyclicPageTrees(t *testing.T) {
	page := func(n int) string {
		return fmt.Sprintf("<< /Type /Page /Parent %d 0 R /Resources << /Font << /F1 3 0 R >> >> /Contents %d 0 R >>", 2, n)
	}
	content := func(text string) string {
		body := fmt.Sprintf("BT /F1 12 Tf 50 750 Td (%s) Tj ET", text)
		return fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(body), body)
	}
	// 1 catalog, 2 root Pages, 3 font, 4 inner Pages, 5..10 pages and
	// contents: root kids are [page A, inner [page B, page C]].
	nested := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [5 0 R 4 0 R] /Count 3 >>",
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
		"<< /Type /Pages /Parent 2 0 R /Kids [7 0 R 9 0 R] /Count 2 >>",
		page(6), content("alpha page"),
		page(8), content("bravo page"),
		page(10), content("charlie page"),
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "nested.pdf")
	writeRawPDF(t, path, nested)
	doc, err := (PDF{}).Extract(testCtx(), path)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, p := range doc.Pages {
		got = append(got, fmt.Sprintf("%d:%s", p.Num, p.Text))
	}
	if want := "1:alpha page 2:bravo page 3:charlie page"; strings.Join(got, " ") != want {
		t.Fatalf("pages = %q; want %q", strings.Join(got, " "), want)
	}

	cyclic := append([]string(nil), nested...)
	cyclic[3] = "<< /Type /Pages /Parent 2 0 R /Kids [7 0 R 2 0 R] /Count 2 >>"
	path = filepath.Join(dir, "cyclic.pdf")
	writeRawPDF(t, path, cyclic)
	if doc, err := (PDF{}).Extract(testCtx(), path); err == nil {
		t.Fatalf("cyclic page tree = %d pages, no error; want an error", len(doc.Pages))
	}
}

// writeRawPDF writes objs as objects 1..n of a PDF with a valid xref table;
// object 1 must be the catalog.
func writeRawPDF(t *testing.T, path string, objs []string) {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objs))
	for i, o := range objs {
		offsets[i] = buf.Len()
		fmt.Fprintf(&buf, "%d 0 obj\n%s\nendobj\n", i+1, o)
	}
	xref := buf.Len()
	fmt.Fprintf(&buf, "xref\n0 %d\n0000000000 65535 f \n", len(objs)+1)
	for _, off := range offsets {
		fmt.Fprintf(&buf, "%010d 00000 n \n", off)
	}
	fmt.Fprintf(&buf, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objs)+1, xref)
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A PDF's text stops at the text limit, at a page boundary or inside the
// page that crosses it, and the document says it was cut.
func TestPDFTextIsCappedAtTheTextLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.pdf")
	if _, err := synthpdf.WriteFile(path, synthpdf.Spec{Pages: 200, LinesPerPage: 20}); err != nil {
		t.Fatal(err)
	}
	full, err := extractPDF(testCtx(), path, MaxBytes, MaxTextBytes)
	if err != nil || full.Truncated {
		t.Fatalf("whole document: truncated %v, %v", full.Truncated, err)
	}
	const limit = 10_000
	doc, err := extractPDF(testCtx(), path, MaxBytes, limit)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.Truncated || len(doc.Text) > limit || len(doc.Text) < limit/2 || !strings.HasPrefix(full.Text, doc.Text) {
		t.Fatalf("capped document: truncated %v, %d bytes of text (limit %d), prefix of the full text %v",
			doc.Truncated, len(doc.Text), limit, strings.HasPrefix(full.Text, doc.Text))
	}
	if n := len(doc.Pages); n == 0 || n >= len(full.Pages) {
		t.Fatalf("capped document kept %d of %d pages", n, len(full.Pages))
	}
}

// LOC-84: a text file is read into the string it is indexed as, sized up
// front: about its size is allocated, where io.ReadAll's doubling buffer
// and the conversion to string allocated about three times its size.
func TestPlainTextIsReadWithoutExtraCopies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readings.csv")
	const size = 8 << 20
	line := "2026-10-13,station-7,valve-3,pressure,4.71,ok\n"
	if err := os.WriteFile(path, []byte(strings.Repeat(line, size/len(line))), 0o600); err != nil {
		t.Fatal(err)
	}
	var doc *Document
	var err error
	n := allocated(func() { doc, err = (PlainText{}).Extract(testCtx(), path) })
	if err != nil || doc.Truncated {
		t.Fatalf("extract: truncated %v, %v", doc != nil && doc.Truncated, err)
	}
	t.Logf("a %d MB file allocated %.2f MB", size>>20, float64(n)/(1<<20))
	if n > size+size/4 {
		t.Fatalf("reading a %d-byte file allocated %d bytes; want about its size", size, n)
	}
}

// Text formats are read up to MaxTextBytes, not MaxBytes.
func TestTextLimitIsBelowTheFileLimit(t *testing.T) {
	if MaxTextBytes > MaxBytes {
		t.Fatalf("MaxTextBytes %d exceeds MaxBytes %d", MaxTextBytes, MaxBytes)
	}
	if got := (sizeLimit(0)).text(); got != MaxTextBytes {
		t.Fatalf("default text limit = %d; want %d", got, MaxTextBytes)
	}
	if got := (sizeLimit(10)).text(); got != 10 {
		t.Fatalf("a test's lower limit = %d; want 10", got)
	}
}

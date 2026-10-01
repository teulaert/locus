package extract

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testCtx() context.Context { return context.Background() }

// writePDF emits a minimal, valid multi-page PDF containing the given text
// per page so page provenance can be asserted in tests.
func writePDF(t *testing.T, path string, pages [][]string) {
	t.Helper()
	var buf bytes.Buffer
	offsets := map[int]int{}
	obj := func(num int, body string) {
		offsets[num] = buf.Len()
		fmt.Fprintf(&buf, "%d 0 obj\n%s\nendobj\n", num, body)
	}
	n := len(pages)
	kids := make([]string, n)
	for i := 0; i < n; i++ {
		kids[i] = fmt.Sprintf("%d 0 R", 4+2*i)
	}
	buf.WriteString("%PDF-1.4\n")
	obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
	obj(2, fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(kids, " "), n))
	obj(3, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
	for i, lines := range pages {
		pageObj, contentObj := 4+2*i, 5+2*i
		obj(pageObj, fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents %d 0 R >>", contentObj))
		var text strings.Builder
		text.WriteString("BT\n/F1 12 Tf\n50 750 Td\n")
		for j, l := range lines {
			if j > 0 {
				text.WriteString("0 -18 Td\n")
			}
			fmt.Fprintf(&text, "(%s) Tj\n", escapePDF(l))
		}
		text.WriteString("ET")
		body := text.String()
		obj(contentObj, fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(body), body))
	}
	xrefPos := buf.Len()
	total := 3 + 2*n
	fmt.Fprintf(&buf, "xref\n0 %d\n", total+1)
	buf.WriteString("0000000000 65535 f \n")
	for i := 1; i <= total; i++ {
		fmt.Fprintf(&buf, "%010d 00000 n \n", offsets[i])
	}
	fmt.Fprintf(&buf, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", total+1, xrefPos)
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func escapePDF(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `(`, `\(`, `)`, `\)`)
	return r.Replace(s)
}

func writeDOCX(t *testing.T, path string, paragraphs []string) {
	t.Helper()
	var body strings.Builder
	for _, p := range paragraphs {
		body.WriteString("<w:p><w:r><w:t>" + p + "</w:t></w:r></w:p>")
	}
	doc := `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>` + body.String() + `</w:body></w:document>`
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zw := zip.NewWriter(f)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(doc)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupByExtension(t *testing.T) {
	for path, want := range map[string]string{
		"/x/notes.md":    "markdown",
		"/x/notes.TXT":   "plaintext",
		"/x/report.pdf":  "pdf",
		"/x/thesis.docx": "docx",
		"/x/page.html":   "html",
		"/x/binary.bin":  "",
	} {
		ex := Lookup(path)
		if want == "" {
			if ex != nil {
				t.Errorf("Lookup(%s) = %s, want nil", path, ex.Name())
			}
			continue
		}
		if ex == nil {
			t.Errorf("Lookup(%s) = nil, want %s", path, want)
			continue
		}
		if ex.Name() != want {
			t.Errorf("Lookup(%s) = %s, want %s", path, ex.Name(), want)
		}
	}
}

func TestPlainTextAndMarkdown(t *testing.T) {
	dir := t.TempDir()
	txt := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(txt, []byte("hello\nworld\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := (PlainText{}).Extract(testCtx(), txt)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Text != "hello\nworld" {
		t.Fatalf("text = %q", doc.Text)
	}
	if doc.Markdown {
		t.Fatal("plain text must not be flagged as Markdown")
	}

	md := filepath.Join(dir, "b.md")
	if err := os.WriteFile(md, []byte("# Title\n\nbody\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err = (Markdown{}).Extract(testCtx(), md)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(doc.Text, "# Title") {
		t.Fatalf("markdown headings must survive for section provenance: %q", doc.Text)
	}
	if !doc.Markdown {
		t.Fatal("markdown source must be flagged so the chunker detects headings")
	}
}

func TestEmptyAndBinaryFiles(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := (PlainText{}).Extract(testCtx(), empty)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Text != "" {
		t.Fatalf("empty file extracted %q", doc.Text)
	}
	binary := filepath.Join(dir, "bin.txt")
	if err := os.WriteFile(binary, []byte{0xff, 0xfe, 0x00, 0x01}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (PlainText{}).Extract(testCtx(), binary); err == nil {
		t.Fatal("expected error for invalid UTF-8")
	}
}

func TestLongSingleLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "long.txt")
	if err := os.WriteFile(path, []byte(strings.Repeat("word ", 50000)), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := (PlainText{}).Extract(testCtx(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Text) < 1000 {
		t.Fatal("long line truncated unexpectedly")
	}
}

func TestHTMLExtraction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "page.html")
	html := `<html><head><style>body{color:red}</style><script>alert(1)</script></head>
<body><h1>Insurance terms</h1><p>Coverage&nbsp;details for the roof.</p><div>footer</div></body></html>`
	if err := os.WriteFile(path, []byte(html), 0o600); err != nil {
		t.Fatal(err)
	}
	doc, err := (HTML{}).Extract(testCtx(), path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Text, "Insurance terms") || !strings.Contains(doc.Text, "Coverage details") {
		t.Fatalf("html extraction = %q", doc.Text)
	}
	if strings.Contains(doc.Text, "alert(1)") || strings.Contains(doc.Text, "color:red") {
		t.Fatalf("script/style leaked into text: %q", doc.Text)
	}
}

func TestDOCXExtraction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "memo.docx")
	writeDOCX(t, path, []string{"First paragraph.", "Second paragraph."})
	doc, err := (DOCX{}).Extract(testCtx(), path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Text, "First paragraph.") || !strings.Contains(doc.Text, "Second paragraph.") {
		t.Fatalf("docx extraction = %q", doc.Text)
	}
	if strings.Contains(doc.Text, "w:p") {
		t.Fatalf("markup leaked: %q", doc.Text)
	}
}

func TestDOCXCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.docx")
	if err := os.WriteFile(path, []byte("not a zip at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (DOCX{}).Extract(testCtx(), path); err == nil {
		t.Fatal("expected error for corrupt docx")
	}
}

func TestPDFPagesProvenance(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.pdf")
	writePDF(t, path, [][]string{
		{"Solar panel installation quote", "Total cost 14200 EUR"},
		{"Second page talks about warranties"},
	})
	doc, err := (PDF{}).Extract(testCtx(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Pages) < 2 {
		t.Fatalf("expected 2 pages with provenance, got %d (text=%q)", len(doc.Pages), doc.Text)
	}
	if doc.Pages[0].Num != 1 || !strings.Contains(doc.Pages[0].Text, "Solar panel") {
		t.Fatalf("page 1 = %+v", doc.Pages[0])
	}
	if doc.Pages[1].Num != 2 || !strings.Contains(doc.Pages[1].Text, "warranties") {
		t.Fatalf("page 2 = %+v", doc.Pages[1])
	}
}

func TestPDFEncryptedOrInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.pdf")
	if err := os.WriteFile(path, []byte("%PDF-1.4 garbage, no xref"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (PDF{}).Extract(testCtx(), path); err == nil {
		t.Fatal("expected error for unreadable pdf")
	}
}

// writeDamagedPageTreePDF writes a PDF that opens cleanly (header, xref and
// trailer are intact) but whose xref entry for the page tree points at the
// font object. ledongthuc/pdf only notices when NumPage resolves the page
// tree, and reports it by panicking.
func writeDamagedPageTreePDF(t *testing.T, path string) {
	t.Helper()
	writePDF(t, path, [][]string{{"This text is never reached"}})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	start := strings.Index(s, "\nxref\n") + 1
	lines := strings.SplitAfter(s[start:], "\n")
	// lines: "xref", "0 N", obj 0, obj 1, obj 2 (the page tree), obj 3 (font).
	lines[4] = lines[5]
	if err := os.WriteFile(path, []byte(s[:start]+strings.Join(lines, "")), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPDFDamagedPageTreeIsAnErrorNotAPanic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "damaged.pdf")
	writeDamagedPageTreePDF(t, path)
	doc, err := (PDF{}).Extract(testCtx(), path)
	if err == nil {
		t.Fatalf("expected an error for a damaged page tree, got %+v", doc)
	}
	if !errors.Is(err, ErrPanic) {
		t.Fatalf("library panic should surface as ErrPanic, got %v", err)
	}
}

type panickyExtractor struct{}

func (panickyExtractor) Name() string         { return "panicky" }
func (panickyExtractor) Extensions() []string { return []string{".panicky"} }
func (panickyExtractor) Extract(context.Context, string) (*Document, error) {
	panic("parser bug")
}

func TestRunRecoversExtractorPanics(t *testing.T) {
	doc, err := Run(testCtx(), panickyExtractor{}, "x.panicky")
	if doc != nil || !errors.Is(err, ErrPanic) {
		t.Fatalf("Run = %+v, %v; want ErrPanic", doc, err)
	}
	if !strings.Contains(err.Error(), "parser bug") {
		t.Fatalf("panic value should be kept in the error: %v", err)
	}
}

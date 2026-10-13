package extract

import (
	"archive/zip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

func init() {
	Register(PlainText{})
	Register(Markdown{})
	Register(HTML{})
	Register(DOCX{})
	Register(PDF{})
}

// PlainText handles .txt and friends.
type PlainText struct{ limit sizeLimit }

func (PlainText) Name() string { return "plaintext" }
func (PlainText) Extensions() []string {
	return []string{".txt", ".text", ".log", ".csv", ".tsv", ".json", ".yaml", ".yml", ".toml", ".ini", ".env"}
}

func (e PlainText) Extract(ctx context.Context, path string) (*Document, error) {
	s, truncated, err := readCapped(ctx, path, e.limit.text())
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(s) {
		return nil, fmt.Errorf("extract: %s is not valid UTF-8 text", path)
	}
	return &Document{Text: strings.TrimRight(s, "\n"), Truncated: truncated}, nil
}

// Markdown handles .md files; headings are preserved in the text so the
// chunker can recover section provenance.
type Markdown struct{ limit sizeLimit }

func (Markdown) Name() string         { return "markdown" }
func (Markdown) Extensions() []string { return []string{".md", ".markdown", ".mdown", ".rst", ".adoc"} }

func (e Markdown) Extract(ctx context.Context, path string) (*Document, error) {
	doc, err := PlainText(e).Extract(ctx, path)
	if err != nil {
		return nil, err
	}
	doc.Markdown = true
	return doc, nil
}

var (
	scriptStyle  = regexp.MustCompile(`(?is)<(script|style)[^>]*>.*?</(script|style)\s*>`)
	blockTags    = regexp.MustCompile(`(?i)<(p|br|div|li|h[1-6]|tr)[^>]*>`)
	tagsOpen     = regexp.MustCompile(`(?s)<[^>]*>`)
	spaceSqueeze = regexp.MustCompile(`[ \t]+`)
	nlSqueeze    = regexp.MustCompile(`\n{3,}`)
)

// HTML strips markup to readable text.
type HTML struct{ limit sizeLimit }

func (HTML) Name() string         { return "html" }
func (HTML) Extensions() []string { return []string{".html", ".htm", ".xhtml"} }

func (e HTML) Extract(ctx context.Context, path string) (*Document, error) {
	s, truncated, err := readCapped(ctx, path, e.limit.text())
	if err != nil {
		return nil, err
	}
	// Each pass below runs over the whole input and cannot be interrupted
	// itself, so check for cancellation between them.
	for _, step := range []func(string) string{
		func(s string) string { return scriptStyle.ReplaceAllString(s, " ") },
		func(s string) string { return blockTags.ReplaceAllString(s, "\n") },
		func(s string) string { return tagsOpen.ReplaceAllString(s, "") },
		html.UnescapeString,
		strings.NewReplacer("\u00a0", " ", "\u2009", " ", "\u200b", "").Replace,
		func(s string) string { return spaceSqueeze.ReplaceAllString(s, " ") },
		func(s string) string { return nlSqueeze.ReplaceAllString(s, "\n\n") },
	} {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("extract: html: %w", err)
		}
		s = step(s)
	}
	return &Document{Text: strings.TrimSpace(s), Truncated: truncated}, nil
}

// DOCX extracts text from Office Open XML word documents.
type DOCX struct{ limit sizeLimit }

func (DOCX) Name() string         { return "docx" }
func (DOCX) Extensions() []string { return []string{".docx"} }

func (e DOCX) Extract(ctx context.Context, path string) (*Document, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("extract: open docx: %w", err)
	}
	defer zr.Close()
	var target *zip.File
	for _, f := range zr.File {
		if f.Name == "word/document.xml" {
			target = f
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("extract: %s has no word/document.xml", path)
	}
	rc, err := target.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	// The XML is decoded as it is decompressed, never held whole.
	body := &countingReader{r: io.LimitReader(ctxReader{ctx, rc}, e.limit.bytes()+1)}
	text, full, err := docxText(ctx, body, e.limit.text())
	if err != nil {
		return nil, err
	}
	if body.err != nil && !errors.Is(body.err, io.EOF) {
		// The archive itself is damaged, not just the XML in it.
		return nil, fmt.Errorf("extract: read docx: %w", body.err)
	}
	return &Document{Text: text, Truncated: full || body.n > e.limit.bytes()}, nil
}

// countingReader counts the bytes read through it and keeps the read error
// that ended them.
type countingReader struct {
	r   io.Reader
	n   int64
	err error
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if err != nil {
		c.err = err
	}
	return n, err
}

// docxText collects paragraph text from document.xml, stopping once it holds
// maxText bytes (full reports that it did). A document cut short by the size
// limit ends in a syntax error; the paragraphs decoded up to that point are
// kept.
func docxText(ctx context.Context, r io.Reader, maxText int64) (s string, full bool, err error) {
	dec := xml.NewDecoder(r)
	var sb strings.Builder
	var inP bool
	flush := func() {
		if sb.Len() > 0 && !strings.HasSuffix(sb.String(), "\n\n") {
			sb.WriteString("\n\n")
		}
		inP = false
	}
	for n := 0; ; n++ {
		if n%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return "", false, fmt.Errorf("extract: docx: %w", err)
			}
		}
		if int64(sb.Len()) >= maxText {
			full = true
			break
		}
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch se := tok.(type) {
		case xml.StartElement:
			if se.Name.Local == "p" {
				flush()
				inP = true
			}
		case xml.EndElement:
			if se.Name.Local == "p" {
				flush()
			}
		case xml.CharData:
			if inP {
				sb.Write([]byte(se))
			}
		}
	}
	if err := ctx.Err(); err != nil {
		// The reads were aborted, so the text is incomplete.
		return "", false, fmt.Errorf("extract: docx: %w", err)
	}
	s = strings.TrimSpace(sb.String())
	if full && int64(len(s)) > maxText {
		s = cutText(s, int(maxText))
	}
	s = nlSqueeze.ReplaceAllString(s, "\n\n")
	return s, full, nil
}

// readCapped reads at most limit bytes of path as text, aborting as soon as
// ctx is done. truncated reports that the file was longer than the limit;
// the returned text then ends at a UTF-8 rune boundary, so a cap landing
// inside a multi-byte character does not make otherwise valid text look like
// binary.
func readCapped(ctx context.Context, path string, limit int64) (s string, truncated bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	var hint int64
	if st, err := f.Stat(); err == nil {
		hint = st.Size()
	}
	return readLimited(ctx, f, limit, hint)
}

// readLimited is readCapped over an open reader whose length is expected to
// be about hint bytes (0 when unknown).
//
// The text is read straight into the string it is returned as, sized from
// hint up front: io.ReadAll followed by a conversion to string held up to
// three times the file's size at once (the doubling buffer and the copy),
// where this holds it once (LOC-84).
func readLimited(ctx context.Context, r io.Reader, limit, hint int64) (s string, truncated bool, err error) {
	var sb strings.Builder
	// One byte past the limit tells a file of exactly limit bytes from a
	// longer one.
	sb.Grow(int(min(max(hint, 0), limit) + 1))
	if _, err := io.Copy(&sb, io.LimitReader(ctxReader{ctx, r}, limit+1)); err != nil {
		return "", false, err
	}
	s = sb.String()
	if int64(len(s)) > limit {
		return cutText(s, int(limit)), true, nil
	}
	return s, false, nil
}

// cutText returns the longest prefix of s of at most n bytes that does not
// end in an incomplete UTF-8 sequence. It slices s rather than copying it.
func cutText(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if n >= len(s) {
		return s
	}
	s = s[:n]
	for i := len(s) - 1; i >= 0 && i >= len(s)-utf8.UTFMax; i-- {
		if utf8.RuneStart(s[i]) {
			if !utf8.FullRuneInString(s[i:]) {
				return s[:i]
			}
			break
		}
	}
	return s
}

// ctxReader fails reads once ctx is done, so a large read is abandoned
// between chunks instead of running to completion after cancellation.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, fmt.Errorf("extract: read: %w", err)
	}
	return c.r.Read(p)
}

// ctxReaderAt is ctxReader for random access, used by the PDF parser.
type ctxReaderAt struct {
	ctx context.Context
	r   io.ReaderAt
}

func (c ctxReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, fmt.Errorf("extract: read: %w", err)
	}
	return c.r.ReadAt(p, off)
}

package extract

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
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
type PlainText struct{}

func (PlainText) Name() string { return "plaintext" }
func (PlainText) Extensions() []string {
	return []string{".txt", ".text", ".log", ".csv", ".tsv", ".json", ".yaml", ".yml", ".toml", ".ini", ".env"}
}

func (PlainText) Extract(ctx context.Context, path string) (*Document, error) {
	b, truncated, err := readCapped(ctx, path)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(b) {
		return nil, fmt.Errorf("extract: %s is not valid UTF-8 text", path)
	}
	return &Document{Text: strings.TrimRight(string(b), "\n"), Truncated: truncated}, nil
}

// Markdown handles .md files; headings are preserved in the text so the
// chunker can recover section provenance.
type Markdown struct{}

func (Markdown) Name() string         { return "markdown" }
func (Markdown) Extensions() []string { return []string{".md", ".markdown", ".mdown", ".rst", ".adoc"} }

func (Markdown) Extract(ctx context.Context, path string) (*Document, error) {
	doc, err := PlainText{}.Extract(ctx, path)
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
type HTML struct{}

func (HTML) Name() string         { return "html" }
func (HTML) Extensions() []string { return []string{".html", ".htm", ".xhtml"} }

func (HTML) Extract(ctx context.Context, path string) (*Document, error) {
	b, truncated, err := readCapped(ctx, path)
	if err != nil {
		return nil, err
	}
	// Each pass below runs over the whole input and cannot be interrupted
	// itself, so check for cancellation between them.
	s := string(b)
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
type DOCX struct{}

func (DOCX) Name() string         { return "docx" }
func (DOCX) Extensions() []string { return []string{".docx"} }

func (DOCX) Extract(ctx context.Context, path string) (*Document, error) {
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
	xml, truncated, err := readLimited(ctx, rc)
	if err != nil {
		return nil, err
	}
	text, err := docxText(ctx, xml)
	if err != nil {
		return nil, err
	}
	return &Document{Text: text, Truncated: truncated}, nil
}

// docxText collects paragraph text from document.xml. A document cut short
// by the size limit ends in a syntax error; the paragraphs decoded up to that
// point are kept.
func docxText(ctx context.Context, b []byte) (string, error) {
	dec := xml.NewDecoder(bytes.NewReader(b))
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
				return "", fmt.Errorf("extract: docx: %w", err)
			}
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
	s := strings.TrimSpace(sb.String())
	s = nlSqueeze.ReplaceAllString(s, "\n\n")
	return s, nil
}

// readCapped reads at most maxBytes of path, aborting as soon as ctx is
// done. truncated reports that the file was longer than the limit; the
// returned bytes then end at a UTF-8 rune boundary, so a cap landing inside a
// multi-byte character does not make otherwise valid text look like binary.
func readCapped(ctx context.Context, path string) (b []byte, truncated bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	return readLimited(ctx, f)
}

// readLimited is readCapped over an open reader.
func readLimited(ctx context.Context, r io.Reader) (b []byte, truncated bool, err error) {
	b, err = io.ReadAll(io.LimitReader(ctxReader{ctx, r}, maxBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(b)) > maxBytes {
		return trimPartialRune(b[:maxBytes]), true, nil
	}
	return b, false, nil
}

// trimPartialRune drops an incomplete UTF-8 sequence at the end of b, as left
// by cutting valid text at an arbitrary byte. Bytes that were never valid
// UTF-8 are left alone for the caller to judge.
func trimPartialRune(b []byte) []byte {
	for i := len(b) - 1; i >= 0 && i >= len(b)-utf8.UTFMax; i-- {
		if utf8.RuneStart(b[i]) {
			if !utf8.FullRune(b[i:]) {
				return b[:i]
			}
			break
		}
	}
	return b
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

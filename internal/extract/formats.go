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

func (PlainText) Extract(_ context.Context, path string) (*Document, error) {
	b, err := readCapped(path)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(b) {
		return nil, fmt.Errorf("extract: %s is not valid UTF-8 text", path)
	}
	return &Document{Text: strings.TrimRight(string(b), "\n")}, nil
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
	tagsOpen     = regexp.MustCompile(`(?s)<[^>]*>`)
	spaceSqueeze = regexp.MustCompile(`[ \t]+`)
	nlSqueeze    = regexp.MustCompile(`\n{3,}`)
)

// HTML strips markup to readable text.
type HTML struct{}

func (HTML) Name() string         { return "html" }
func (HTML) Extensions() []string { return []string{".html", ".htm", ".xhtml"} }

func (HTML) Extract(_ context.Context, path string) (*Document, error) {
	b, err := readCapped(path)
	if err != nil {
		return nil, err
	}
	s := scriptStyle.ReplaceAllString(string(b), " ")
	s = regexp.MustCompile(`(?i)<(p|br|div|li|h[1-6]|tr)[^>]*>`).ReplaceAllString(s, "\n")
	s = tagsOpen.ReplaceAllString(s, "")
	s = html.UnescapeString(s)
	s = strings.NewReplacer("\u00a0", " ", "\u2009", " ", "\u200b", "").Replace(s)
	s = spaceSqueeze.ReplaceAllString(s, " ")
	s = nlSqueeze.ReplaceAllString(s, "\n\n")
	return &Document{Text: strings.TrimSpace(s)}, nil
}

// DOCX extracts text from Office Open XML word documents.
type DOCX struct{}

func (DOCX) Name() string         { return "docx" }
func (DOCX) Extensions() []string { return []string{".docx"} }

func (DOCX) Extract(_ context.Context, path string) (*Document, error) {
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
	xml, err := io.ReadAll(io.LimitReader(rc, MaxBytes))
	if err != nil {
		return nil, err
	}
	return &Document{Text: docxText(xml)}, nil
}

func docxText(b []byte) string {
	dec := xml.NewDecoder(bytes.NewReader(b))
	var sb strings.Builder
	var inP bool
	flush := func() {
		if sb.Len() > 0 && !strings.HasSuffix(sb.String(), "\n\n") {
			sb.WriteString("\n\n")
		}
		inP = false
	}
	for {
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
	return s
}

func readCapped(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, MaxBytes))
}

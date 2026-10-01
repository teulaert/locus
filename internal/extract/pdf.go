package extract

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ledongthuc/pdf"
)

// PDF extracts text page by page so passages keep page provenance.
type PDF struct{}

func (PDF) Name() string         { return "pdf" }
func (PDF) Extensions() []string { return []string{".pdf"} }

// Extract recovers panics: github.com/ledongthuc/pdf reports many parse
// errors (damaged page trees, font dictionaries, unsupported filters) by
// panicking from NumPage, Page, Fonts and Font, and only recovers internally
// in Open and GetPlainText.
func (PDF) Extract(_ context.Context, path string) (doc *Document, err error) {
	defer recoverPanic("pdf", &err)
	if st, err := os.Stat(path); err == nil && st.Size() > MaxBytes {
		return nil, fmt.Errorf("extract: %s exceeds extraction size limit", path)
	}
	f, r, err := pdf.Open(path)
	if err != nil {
		return nil, fmt.Errorf("extract: open pdf: %w", err)
	}
	defer f.Close()

	doc = &Document{}
	n := r.NumPage()
	var all strings.Builder
	for i := 1; i <= n; i++ {
		p := r.Page(i)
		if p.V.IsNull() {
			continue
		}
		fonts := map[string]*pdf.Font{}
		for _, name := range p.Fonts() {
			fnt := p.Font(name)
			fonts[name] = &fnt
		}
		text, err := p.GetPlainText(fonts)
		if err != nil {
			// Fall back to whole-document extraction.
			return pdfPlainText(r)
		}
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		doc.Pages = append(doc.Pages, Page{Num: i, Text: text})
		if all.Len() > 0 {
			all.WriteString("\n\n")
		}
		all.WriteString(text)
	}
	doc.Text = all.String()
	if doc.Text == "" {
		return pdfPlainText(r)
	}
	return doc, nil
}

func pdfPlainText(r *pdf.Reader) (*Document, error) {
	rd, err := r.GetPlainText()
	if err != nil {
		return nil, fmt.Errorf("extract: pdf text: %w", err)
	}
	b, err := io.ReadAll(io.LimitReader(rd, MaxBytes))
	if err != nil {
		return nil, err
	}
	return &Document{Text: strings.TrimSpace(string(b))}, nil
}

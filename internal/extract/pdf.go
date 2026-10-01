package extract

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ledongthuc/pdf"
)

// PDF extracts text page by page so passages keep page provenance.
type PDF struct{}

func (PDF) Name() string         { return "pdf" }
func (PDF) Extensions() []string { return []string{".pdf"} }

// Extract runs the parse in its own goroutine and returns as soon as ctx is
// done: github.com/ledongthuc/pdf takes no context, so a large document could
// otherwise hold up shutdown until it finished. The parser reads through a
// reader that fails once ctx is done and checks ctx between pages, so the
// abandoned goroutine stops shortly after.
//
// Panics are recovered: the library reports many parse errors (damaged page
// trees, font dictionaries, unsupported filters) by panicking from NumPage,
// Page, Fonts and Font, and only recovers internally in Open and
// GetPlainText.
func (PDF) Extract(ctx context.Context, path string) (*Document, error) {
	type result struct {
		doc *Document
		err error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		defer func() { done <- r }()
		defer recoverPanic("pdf", &r.err)
		r.doc, r.err = extractPDF(ctx, path)
	}()
	select {
	case r := <-done:
		if err := ctx.Err(); err != nil && r.err != nil {
			// The parse failed because its reads were aborted.
			return nil, fmt.Errorf("extract: pdf: %w", err)
		}
		return r.doc, r.err
	case <-ctx.Done():
		return nil, fmt.Errorf("extract: pdf: %w", ctx.Err())
	}
}

func extractPDF(ctx context.Context, path string) (*Document, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("extract: open pdf: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() > maxBytes {
		return nil, fmt.Errorf("extract: %s exceeds extraction size limit", path)
	}
	r, err := pdf.NewReader(ctxReaderAt{ctx, f}, st.Size())
	if err != nil {
		return nil, fmt.Errorf("extract: open pdf: %w", err)
	}

	doc := &Document{}
	n := r.NumPage()
	var all strings.Builder
	for i := 1; i <= n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("extract: pdf: %w", err)
		}
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
			return pdfPlainText(ctx, r)
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
		return pdfPlainText(ctx, r)
	}
	return doc, nil
}

func pdfPlainText(ctx context.Context, r *pdf.Reader) (*Document, error) {
	rd, err := r.GetPlainText()
	if err != nil {
		return nil, fmt.Errorf("extract: pdf text: %w", err)
	}
	b, truncated, err := readLimited(ctx, rd)
	if err != nil {
		return nil, err
	}
	return &Document{Text: strings.TrimSpace(string(b)), Truncated: truncated}, nil
}

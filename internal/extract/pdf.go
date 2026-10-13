package extract

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ledongthuc/pdf"
)

// PDF extracts text page by page so passages keep page provenance.
type PDF struct{ limit sizeLimit }

func (PDF) Name() string         { return "pdf" }
func (PDF) Extensions() []string { return []string{".pdf"} }

// pdfCancelGrace bounds how long Extract waits, once ctx is done, for the
// parse goroutine to notice and exit. Its reads fail and it checks ctx between
// pages, so it normally stops within microseconds; the bound only matters for
// a parser stuck in CPU-bound work on bytes it has already read.
const pdfCancelGrace = 250 * time.Millisecond

// Extract runs the parse in its own goroutine so it can return promptly when
// ctx is done: github.com/ledongthuc/pdf takes no context, so a large
// document could otherwise hold up shutdown until it finished. The parser
// reads through a reader that fails once ctx is done and checks ctx between
// pages; on cancellation Extract waits up to pdfCancelGrace for the goroutine
// to stop, so it does not normally outlive the call.
//
// Panics are recovered: the library reports many parse errors (damaged page
// trees, font dictionaries, unsupported filters) by panicking from NumPage,
// Page, Fonts and Font, and only recovers internally in Open and
// GetPlainText.
func (e PDF) Extract(ctx context.Context, path string) (*Document, error) {
	type result struct {
		doc *Document
		err error
	}
	limit, maxText := e.limit.bytes(), e.limit.text()
	done := make(chan result, 1)
	go func() {
		var r result
		defer func() { done <- r }()
		defer recoverPanic("pdf", &r.err)
		r.doc, r.err = extractPDF(ctx, path, limit, maxText)
	}()
	select {
	case r := <-done:
		if err := ctx.Err(); err != nil && r.err != nil {
			// The parse failed because its reads were aborted.
			return nil, fmt.Errorf("extract: pdf: %w", err)
		}
		return r.doc, r.err
	case <-ctx.Done():
		grace := time.NewTimer(pdfCancelGrace)
		defer grace.Stop()
		select {
		case <-done:
		case <-grace.C:
		}
		return nil, fmt.Errorf("extract: pdf: %w", ctx.Err())
	}
}

// extractPDF extracts the text of the PDF at path, which must be at most
// limit bytes, keeping at most maxText bytes of it.
func extractPDF(ctx context.Context, path string, limit, maxText int64) (*Document, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("extract: open pdf: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() > limit {
		return nil, fmt.Errorf("extract: %s exceeds extraction size limit", path)
	}
	r, err := pdf.NewReader(ctxReaderAt{ctx, f}, st.Size())
	if err != nil {
		return nil, fmt.Errorf("extract: open pdf: %w", err)
	}

	doc := &Document{}
	var all strings.Builder
	textFull := false
	err = walkPages(ctx, r, func(num int, p pdf.Page) error {
		fonts := map[string]*pdf.Font{}
		for _, name := range p.Fonts() {
			fnt := p.Font(name)
			fonts[name] = &fnt
		}
		text, err := p.GetPlainText(fonts)
		if err != nil {
			return errPageText
		}
		text = strings.TrimSpace(text)
		if text == "" {
			return nil
		}
		// Text past the limit is not indexed (as for every other
		// format), so it is not kept either: one page more than the
		// limit at most is ever held.
		sep := ""
		if all.Len() > 0 {
			sep = "\n\n"
		}
		if room := int(maxText) - all.Len() - len(sep); len(text) > room {
			text = strings.TrimSpace(cutText(text, room))
			textFull = true
		}
		if text != "" {
			doc.Pages = append(doc.Pages, Page{Num: num, Text: text})
			all.WriteString(sep)
			all.WriteString(text)
		}
		if textFull {
			return errTextFull
		}
		return nil
	})
	switch {
	case errors.Is(err, errPageText):
		// Fall back to whole-document extraction.
		return pdfPlainText(ctx, r, maxText)
	case errors.Is(err, errTextFull):
		doc.Truncated = true
	case err != nil:
		return nil, err
	}
	doc.Text = all.String()
	return doc, nil
}

var (
	// errPageText stops the page walk when a page's text cannot be
	// decoded.
	errPageText = errors.New("extract: pdf: page text")
	// errTextFull stops the page walk once the text limit is reached.
	errTextFull = errors.New("extract: pdf: text limit reached")
)

// maxPageTreeDepth bounds the nesting of the page tree walkPages follows;
// real documents nest a few levels at most, and a damaged or hostile file
// whose Kids refer back to an ancestor must not recurse forever.
const maxPageTreeDepth = 64

// walkPages calls fn for every page of r in document order, numbered from 1,
// until fn returns an error or ctx is done.
//
// It walks the page tree once. pdf.Reader.Page(i) instead searches the tree
// from its root for every page, resolving (reading and parsing from the
// file, as the library caches nothing) each kid before the one it wants: for
// a flat tree of n pages that is n²/2 object reads, which made a 1500-page
// report allocate about 3.7 GB and take seconds of CPU to yield 9 MB of text
// (LOC-84). A node whose Type is neither Pages nor Page is skipped, as
// Reader.Page does; a tree deeper than maxPageTreeDepth or holding more
// nodes than its root's Count claims pages (plus slack) is treated as
// damaged and ends the walk.
func walkPages(ctx context.Context, r *pdf.Reader, fn func(num int, p pdf.Page) error) error {
	root := r.Trailer().Key("Root").Key("Pages")
	count := root.Key("Count").Int64()
	budget := max(count, 0)*2 + 1024 // nodes visited, Pages nodes included
	num := 0
	var walk func(node pdf.Value, depth int) error
	walk = func(node pdf.Value, depth int) error {
		if depth > maxPageTreeDepth {
			return fmt.Errorf("extract: pdf: page tree nested deeper than %d levels", maxPageTreeDepth)
		}
		kids := node.Key("Kids")
		for i := 0; i < kids.Len(); i++ {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("extract: pdf: %w", err)
			}
			if budget--; budget < 0 {
				return errors.New("extract: pdf: page tree holds more nodes than its page count")
			}
			kid := kids.Index(i)
			switch kid.Key("Type").Name() {
			case "Pages":
				if err := walk(kid, depth+1); err != nil {
					return err
				}
			case "Page":
				num++
				if err := fn(num, pdf.Page{V: kid}); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if root.Key("Type").Name() != "Pages" {
		return nil
	}
	return walk(root, 0)
}

// pdfPlainText is the library's whole-document extraction, the fallback for a
// document whose pages could not be decoded one by one.
func pdfPlainText(ctx context.Context, r *pdf.Reader, maxText int64) (*Document, error) {
	rd, err := r.GetPlainText()
	if err != nil {
		return nil, fmt.Errorf("extract: pdf text: %w", err)
	}
	s, truncated, err := readLimited(ctx, rd, maxText, 0)
	if err != nil {
		return nil, err
	}
	return &Document{Text: strings.TrimSpace(s), Truncated: truncated}, nil
}

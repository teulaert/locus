// Package extract turns files on disk into searchable text. Extraction is
// intentionally behind an interface so new formats are additive; a file
// without extractable text is still a perfectly normal synchronized file.
package extract

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Page is one page of extracted text when pagination is known.
type Page struct {
	Num  int
	Text string
}

// Document is the extracted view of a file.
type Document struct {
	Text  string
	Pages []Page
	// Markdown is set by extractors for Markdown-family sources whose
	// headings carry section structure. The chunker only treats '#' lines
	// and Setext underlines as headings when it is set, so shell shebangs,
	// C preprocessor lines or config comments in plain text never become
	// sections.
	Markdown bool
	// Truncated is set when the source exceeded the extraction size limit
	// and only its leading part was extracted. The text is cut at a UTF-8
	// rune boundary, so it is always valid and still worth indexing.
	Truncated bool
}

// Extractor produces text for one family of file formats.
type Extractor interface {
	Name() string
	Extensions() []string
	Extract(ctx context.Context, path string) (*Document, error)
}

// ErrPanic marks an extraction that panicked inside a parser (typically a
// third-party format library choking on a malformed file). The panic is
// recovered so it cannot take the daemon down; retrying the same bytes would
// only panic again, so callers treat it as a permanent failure.
var ErrPanic = errors.New("extractor panicked")

// recoverPanic converts a panic in the calling function into an error
// wrapping ErrPanic. Use it as `defer recoverPanic(name, &err)`.
func recoverPanic(name string, err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("extract: %s: %w: %v", name, ErrPanic, r)
	}
}

// Run calls e.Extract, converting any panic into an error wrapping ErrPanic.
// When ctx is done by the time extraction fails, the error wraps ctx.Err()
// instead: a parser that panicked or failed because its reads were aborted
// says nothing about the file, so the work must stay retryable.
func Run(ctx context.Context, e Extractor, path string) (doc *Document, err error) {
	defer func() {
		if err != nil && ctx.Err() != nil && !errors.Is(err, ctx.Err()) {
			doc, err = nil, fmt.Errorf("extract: %s: %w", e.Name(), ctx.Err())
		}
	}()
	defer recoverPanic(e.Name(), &err)
	return e.Extract(ctx, path)
}

var (
	registryMu sync.RWMutex
	byExt      = map[string]Extractor{}
	ordered    []Extractor
)

// Register adds an extractor. Extensions are matched case-insensitively.
func Register(e Extractor) {
	registryMu.Lock()
	defer registryMu.Unlock()
	ordered = append(ordered, e)
	for _, ext := range e.Extensions() {
		byExt[normalizeExt(ext)] = e
	}
}

// Lookup returns the extractor for a path, or nil when the format is not
// extractable (the file still synchronizes normally).
func Lookup(path string) Extractor {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return byExt[normalizeExt(filepath.Ext(path))]
}

// Names lists registered extractor names, sorted.
func Names() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	seen := map[string]bool{}
	var out []string
	for _, e := range ordered {
		if !seen[e.Name()] {
			seen[e.Name()] = true
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func normalizeExt(ext string) string {
	ext = strings.ToLower(strings.TrimSpace(ext))
	if ext != "" && !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	return ext
}

// ErrUnsupported is returned when no extractor handles a file.
var ErrUnsupported = fmt.Errorf("extract: unsupported format")

// MaxBytes bounds the size of a file whose format must be parsed as a whole
// to find its text (PDF, DOCX): a larger file is not extracted.
const MaxBytes = 64 << 20

// MaxTextBytes bounds the text extracted from one file, and so what an
// index worker holds for it (LOC-84): the text, the chunks cut from it and
// their vectors are each about this size at most. A plain-text, Markdown or
// HTML file is read up to this many bytes, and text a document yields past
// it is not indexed; the document is marked Truncated and its leading part
// is indexed. 16 MB is a book of several thousand pages.
const MaxTextBytes = 16 << 20

// sizeLimit is the extraction size limit an extractor applies: MaxBytes
// unless a test set a smaller one on the extractor value, so oversized inputs
// need not be 64 MB on disk. It is a per-value field rather than a package
// variable so a test lowering it cannot race an extraction still running
// elsewhere.
type sizeLimit int64

// bytes is the largest file the extractor parses.
func (l sizeLimit) bytes() int64 {
	if l > 0 {
		return int64(l)
	}
	return MaxBytes
}

// text is the most text the extractor reads or keeps: MaxTextBytes, or a
// test's smaller limit.
func (l sizeLimit) text() int64 {
	if l > 0 {
		return min(int64(l), MaxTextBytes)
	}
	return MaxTextBytes
}

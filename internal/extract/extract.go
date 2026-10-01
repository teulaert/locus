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
func Run(ctx context.Context, e Extractor, path string) (doc *Document, err error) {
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

// MaxBytes bounds extraction of a single file.
const MaxBytes = 64 << 20

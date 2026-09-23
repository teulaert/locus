// Package chunk splits extracted text into semantic chunks with provenance.
package chunk

import (
	"strings"
	"unicode/utf8"

	"github.com/teulaert/locus/internal/extract"
	"github.com/teulaert/locus/internal/fswatch"
)

// Chunk is one searchable passage with provenance.
type Chunk struct {
	Ordinal int
	Text    string
	Page    int
	HasPage bool
	Section string
	Hash    string
}

const (
	// TargetTokens is the desired chunk size in approximate tokens.
	TargetTokens = 700
	// MaxTokens is the hard upper bound before a forced split.
	MaxTokens = 1000
	// MinTokens merges tiny fragments into neighbours when possible.
	MinTokens = 80
	// OverlapTokens is repeated between adjacent forced splits.
	OverlapTokens = 60
)

// Split converts an extracted document into chunks, preferring natural
// boundaries (sections, paragraphs, PDF pages) before falling back to a
// token-size window with overlap.
func Split(doc *extract.Document) []*Chunk {
	if doc == nil {
		return nil
	}
	var units []unit
	switch {
	case len(doc.Pages) > 1:
		for _, p := range doc.Pages {
			units = append(units, paragraphUnits(p.Text, unit{hasPage: true, page: p.Num})...)
		}
	case len(doc.Pages) == 1:
		units = append(units, paragraphUnits(doc.Pages[0].Text, unit{hasPage: true, page: doc.Pages[0].Num})...)
	default:
		units = sectionUnits(doc.Text)
	}
	return pack(units)
}

type unit struct {
	text    string
	section string
	page    int
	hasPage bool
}

// sectionUnits splits markdown-ish text at headings, then paragraphs.
func sectionUnits(text string) []unit {
	var (
		out     []unit
		section string
		buf     []string
		size    int
	)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		para := strings.TrimSpace(strings.Join(buf, "\n"))
		buf = buf[:0]
		size = 0
		if para == "" {
			return
		}
		out = append(out, unit{text: para, section: section})
	}
	for _, line := range strings.Split(text, "\n") {
		if isHeading(line) {
			flush()
			section = strings.TrimSpace(strings.TrimLeft(line, "#*=-> "))
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			flush()
			continue
		}
		buf = append(buf, line)
		size += EstimateTokens(line)
		if size >= TargetTokens {
			flush()
		}
	}
	flush()
	if len(out) == 0 && strings.TrimSpace(text) != "" {
		out = append(out, unit{text: strings.TrimSpace(text)})
	}
	return out
}

func isHeading(line string) bool {
	t := strings.TrimSpace(line)
	if strings.HasPrefix(t, "#") {
		return true
	}
	// Setext heading underline.
	if t == strings.Repeat("=", len(t)) && len(t) > 0 {
		return true
	}
	return false
}

// paragraphUnits splits text into paragraphs; base carries provenance.
func paragraphUnits(text string, base unit) []unit {
	var out []unit
	for _, para := range strings.Split(text, "\n\n") {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		if EstimateTokens(para) > MaxTokens {
			for _, w := range window(para) {
				u := base
				u.text = w
				out = append(out, u)
			}
			continue
		}
		u := base
		u.text = para
		out = append(out, u)
	}
	return out
}

// pack merges small units toward TargetTokens and hard-splits oversized ones.
// pack merges small units toward TargetTokens and hard-splits oversized ones.
func pack(units []unit) []*Chunk {
	var out []*Chunk
	ordinal := 0
	appendChunk := func(u unit) {
		t := strings.TrimSpace(u.text)
		if t == "" {
			return
		}
		out = append(out, &Chunk{
			Ordinal: ordinal,
			Text:    t,
			Page:    u.page,
			HasPage: u.hasPage,
			Section: u.section,
			Hash:    fswatch.HashBytes([]byte(t)),
		})
		ordinal++
	}
	var (
		buf     []string
		size    int
		section string
		page    int
		hasPage bool
	)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		appendChunk(unit{text: strings.Join(buf, "\n\n"), section: section, page: page, hasPage: hasPage})
		buf = buf[:0]
		size = 0
	}
	for _, u := range units {
		if EstimateTokens(u.text) > MaxTokens {
			// Oversized unit: window it with overlap rather than emitting a
			// chunk far beyond the size budget.
			flush()
			for _, w := range window(u.text) {
				wu := u
				wu.text = w
				appendChunk(wu)
			}
			continue
		}
		if size+EstimateTokens(u.text) > MaxTokens && size > 0 {
			flush()
		}
		// Sections/pages never merge across provenance boundaries.
		if size > 0 && (u.section != section || u.hasPage != hasPage || u.page != page) {
			flush()
		}
		buf = append(buf, u.text)
		size += EstimateTokens(u.text)
		section, page, hasPage = u.section, u.page, u.hasPage
		if size >= TargetTokens {
			flush()
		}
	}
	flush()
	return out
}

// window splits a large block into token-budgeted windows with overlap.
func window(text string) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}
	var out []string
	var cur []string
	size := 0
	for _, w := range words {
		wt := EstimateTokens(w) + 1
		if size+wt > MaxTokens && len(cur) > 0 {
			out = append(out, strings.Join(cur, " "))
			keep := overlapCount(cur)
			tail := make([]string, keep)
			copy(tail, cur[len(cur)-keep:])
			cur = append(tail, w)
			size = tokensOf(cur)
			continue
		}
		cur = append(cur, w)
		size += wt
	}
	if len(cur) > 0 {
		out = append(out, strings.Join(cur, " "))
	}
	return out
}

// overlapCount returns how many trailing words fit the overlap budget.
func overlapCount(words []string) int {
	total := 0
	for i := len(words) - 1; i >= 0; i-- {
		total += EstimateTokens(words[i]) + 1
		if total > OverlapTokens {
			return len(words) - 1 - i
		}
	}
	return len(words)
}

func tokensOf(words []string) int {
	total := 0
	for _, w := range words {
		total += EstimateTokens(w) + 1
	}
	return total
}

// EstimateTokens approximates LLM tokens from text (roughly 4 chars each,
// floored by word count for symbol-dense text).
func EstimateTokens(s string) int {
	words := len(strings.Fields(s))
	chars := utf8.RuneCountInString(s)
	punct := strings.Count(s, ".") + strings.Count(s, ",") + strings.Count(s, ";") + strings.Count(s, ":")
	byChar := chars / 4
	byWord := words + punct/4
	if byWord > byChar {
		return byWord
	}
	return byChar
}

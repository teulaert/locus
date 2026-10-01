// Package chunk splits extracted text into semantic chunks with provenance.
package chunk

import (
	"strings"
	"unicode"
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
	// Start is the rune offset of the chunk's first character in the
	// extracted text it was cut from: the page's text when HasPage is set,
	// otherwise the document's text. Windows of an oversized block overlap,
	// so consecutive starts can be closer than the previous chunk's length.
	Start int
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
	if len(doc.Pages) > 0 {
		for _, p := range doc.Pages {
			units = append(units, paragraphUnits(p.Text, unit{hasPage: true, page: p.Num})...)
		}
	} else {
		units = sectionUnits(doc.Text, doc.Markdown)
	}
	return pack(units)
}

type unit struct {
	text    string
	section string
	page    int
	hasPage bool
	// start is the rune offset of text in its source (see Chunk.Start).
	start int
}

// sectionUnits splits text into paragraphs. For Markdown-family sources it
// also splits at ATX and Setext headings and records them as the section;
// other text has no reliable heading syntax and is split by paragraph only.
func sectionUnits(text string, markdown bool) []unit {
	var (
		out     []unit
		section string
		buf     []string
		size    int
		// bufStart is the rune offset of buf[0] in text.
		bufStart int
	)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		joined := strings.Join(buf, "\n")
		para := strings.TrimSpace(joined)
		buf = buf[:0]
		size = 0
		if para == "" {
			return
		}
		out = append(out, unit{text: para, section: section, start: bufStart + leadingRunes(joined)})
	}
	lines := strings.Split(text, "\n")
	// lineStart[i] is the rune offset of lines[i] in text.
	lineStart := make([]int, len(lines))
	off := 0
	for i, l := range lines {
		lineStart[i] = off
		off += utf8.RuneCountInString(l) + 1
	}
	start := 0
	if markdown {
		// YAML front matter is metadata, not headings: its closing '---'
		// would otherwise read as a Setext underline.
		start = frontMatterEnd(lines)
		buf = append(buf, lines[:start]...)
		bufStart = 0
		flush()
	}
	for i := start; i < len(lines); i++ {
		line := lines[i]
		if markdown {
			if title, ok := atxHeading(line); ok {
				flush()
				section = title
				continue
			}
			// A Setext underline promotes the line directly above it (still
			// buffered as paragraph text) to a heading.
			if len(buf) > 0 && isSetextUnderline(line) {
				title := strings.TrimSpace(buf[len(buf)-1])
				buf = buf[:len(buf)-1]
				flush()
				section = title
				continue
			}
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			flush()
			continue
		}
		if len(buf) == 0 {
			bufStart = lineStart[i]
		}
		buf = append(buf, line)
		size += EstimateTokens(line)
		if size >= TargetTokens {
			flush()
		}
	}
	flush()
	if len(out) == 0 && strings.TrimSpace(text) != "" {
		out = append(out, unit{text: strings.TrimSpace(text), start: leadingRunes(text)})
	}
	return out
}

// atxHeading reports whether line is an ATX heading ("#" to "######"
// followed by a space or end of line, at most three spaces of indent) and
// returns its title without the markers or an optional closing sequence.
func atxHeading(line string) (string, bool) {
	indent := len(line) - len(strings.TrimLeft(line, " "))
	if indent > 3 {
		return "", false
	}
	t := strings.TrimRight(line[indent:], " \t\r")
	level := len(t) - len(strings.TrimLeft(t, "#"))
	if level < 1 || level > 6 {
		return "", false
	}
	rest := t[level:]
	if rest != "" && rest[0] != ' ' && rest[0] != '\t' {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	// Optional closing sequence: "## Title ##".
	if closed := strings.TrimRight(rest, "#"); closed != rest && (closed == "" || strings.HasSuffix(closed, " ") || strings.HasSuffix(closed, "\t")) {
		rest = strings.TrimSpace(closed)
	}
	return rest, true
}

// isSetextUnderline reports whether line is a run of '=' or '-' (at most
// three spaces of indent), which underlines the preceding paragraph line.
func isSetextUnderline(line string) bool {
	indent := len(line) - len(strings.TrimLeft(line, " "))
	if indent > 3 {
		return false
	}
	t := strings.TrimRight(line[indent:], " \t\r")
	if t == "" {
		return false
	}
	return strings.Trim(t, "=") == "" || strings.Trim(t, "-") == ""
}

// frontMatterEnd returns the number of leading lines that form a YAML front
// matter block ("---" ... "---" or "..."), or 0 when there is none.
func frontMatterEnd(lines []string) int {
	if len(lines) == 0 || strings.TrimRight(lines[0], " \t\r") != "---" {
		return 0
	}
	for i := 1; i < len(lines); i++ {
		if t := strings.TrimRight(lines[i], " \t\r"); t == "---" || t == "..." {
			return i + 1
		}
	}
	return 0
}

// paragraphUnits splits text into paragraphs; base carries provenance.
func paragraphUnits(text string, base unit) []unit {
	var out []unit
	off := 0 // rune offset of the current segment in text
	for _, seg := range strings.Split(text, "\n\n") {
		segStart := off
		off += utf8.RuneCountInString(seg) + 2
		para := strings.TrimSpace(seg)
		if para == "" {
			continue
		}
		paraStart := segStart + leadingRunes(seg)
		if EstimateTokens(para) > MaxTokens {
			for _, w := range window(para) {
				u := base
				u.text = w.text
				u.start = paraStart + w.start
				out = append(out, u)
			}
			continue
		}
		u := base
		u.text = para
		u.start = paraStart
		out = append(out, u)
	}
	return out
}

// leadingRunes counts the runes of leading white space in s, the amount
// strings.TrimSpace removes from its front.
func leadingRunes(s string) int {
	return utf8.RuneCountInString(s[:len(s)-len(strings.TrimLeftFunc(s, unicode.IsSpace))])
}

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
			Start:   u.start,
		})
		ordinal++
	}
	var (
		buf     []string
		size    int
		section string
		page    int
		hasPage bool
		start   int
	)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		appendChunk(unit{text: strings.Join(buf, "\n\n"), section: section, page: page, hasPage: hasPage, start: start})
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
				wu.text = w.text
				wu.start = u.start + w.start
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
		if len(buf) == 0 {
			start = u.start
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

// span is a window of a block and the rune offset of its first word in it.
type span struct {
	text  string
	start int
}

// window splits a large block into token-budgeted windows with overlap.
func window(text string) []span {
	words, starts := fields(text)
	if len(words) == 0 {
		return nil
	}
	var out []span
	var cur []string
	first := 0 // index into words of cur[0]
	size := 0
	for i, w := range words {
		wt := EstimateTokens(w) + 1
		if size+wt > MaxTokens && len(cur) > 0 {
			out = append(out, span{strings.Join(cur, " "), starts[first]})
			keep := overlapCount(cur)
			tail := make([]string, keep)
			copy(tail, cur[len(cur)-keep:])
			cur = append(tail, w)
			first = i - keep
			size = tokensOf(cur)
			continue
		}
		cur = append(cur, w)
		size += wt
	}
	if len(cur) > 0 {
		out = append(out, span{strings.Join(cur, " "), starts[first]})
	}
	return out
}

// fields is strings.Fields that also returns each field's rune offset.
func fields(s string) ([]string, []int) {
	var (
		words  []string
		starts []int
	)
	from, fromRune, n := -1, 0, 0
	for i, r := range s {
		if unicode.IsSpace(r) {
			if from >= 0 {
				words = append(words, s[from:i])
				starts = append(starts, fromRune)
				from = -1
			}
		} else if from < 0 {
			from, fromRune = i, n
		}
		n++
	}
	if from >= 0 {
		words = append(words, s[from:])
		starts = append(starts, fromRune)
	}
	return words, starts
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

// Package agent exposes the documentation that tells an AI agent how to drive
// Locus. It is embedded in the binary so `locus skill` works on any machine,
// and the same text is checked out at the repository root for humans.
package agent

import (
	_ "embed"
	"strings"
)

//go:embed SKILL.md
var skillDoc string

// Skill returns the agent skill documentation.
func Skill() string { return skillDoc }

// Section returns the markdown section under the given heading, e.g.
// "Exit codes". An empty string means the heading was not found.
func Section(heading string) string { return section(skillDoc, heading) }

// section extracts the section of doc whose heading starts with heading
// (case-insensitive). Lines inside fenced code blocks are never treated as
// headings, so a "# comment" in a shell example does not end a section.
func section(doc, heading string) string {
	want := strings.ToLower(strings.TrimSpace(heading))
	lines := strings.Split(doc, "\n")
	code := fencedLines(lines)
	var out []string
	inSection := false
	for i, line := range lines {
		if !code[i] && strings.HasPrefix(line, "#") {
			title := strings.ToLower(strings.TrimSpace(strings.TrimLeft(line, "#")))
			if inSection {
				if !strings.HasPrefix(title, want) {
					break
				}
			}
			inSection = strings.HasPrefix(title, want)
			continue
		}
		if inSection {
			out = append(out, line)
		}
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// fencedLines reports, per line, whether it belongs to a fenced code block
// (``` or ~~~ fences, including the fence lines themselves). A block closes on
// a fence of the same character that is at least as long as the opener and
// carries no info string.
//
// An unterminated fence is not treated as a code block: its opening line and
// everything after it are scanned as ordinary markdown. CommonMark would run
// such a block to the end of the document, but for section lookup that would
// silently merge every later section into the current one; ignoring the stray
// fence keeps the remaining headings addressable.
func fencedLines(lines []string) []bool {
	code := make([]bool, len(lines))
	for i := 0; i < len(lines); i++ {
		ch, n, ok := fenceOpen(lines[i])
		if !ok {
			continue
		}
		for j := i + 1; j < len(lines); j++ {
			if fenceCloses(lines[j], ch, n) {
				for k := i; k <= j; k++ {
					code[k] = true
				}
				i = j
				break
			}
		}
	}
	return code
}

// fenceRun returns the fence character and run length at the start of line
// after up to three spaces of indentation, and the remainder of the line.
func fenceRun(line string) (ch byte, n int, rest string, ok bool) {
	trimmed := strings.TrimLeft(line, " ")
	if len(line)-len(trimmed) > 3 || trimmed == "" {
		return 0, 0, "", false
	}
	ch = trimmed[0]
	if ch != '`' && ch != '~' {
		return 0, 0, "", false
	}
	for n < len(trimmed) && trimmed[n] == ch {
		n++
	}
	if n < 3 {
		return 0, 0, "", false
	}
	return ch, n, trimmed[n:], true
}

// fenceOpen reports whether line opens a fenced code block. A backtick
// fence's info string may not contain backticks.
func fenceOpen(line string) (byte, int, bool) {
	ch, n, rest, ok := fenceRun(line)
	if !ok || (ch == '`' && strings.ContainsRune(rest, '`')) {
		return 0, 0, false
	}
	return ch, n, true
}

// fenceCloses reports whether line closes a block opened by n fence chars ch.
func fenceCloses(line string, ch byte, n int) bool {
	c, m, rest, ok := fenceRun(line)
	return ok && c == ch && m >= n && strings.TrimSpace(rest) == ""
}

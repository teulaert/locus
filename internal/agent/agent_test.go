package agent

import (
	"strings"
	"testing"
)

const fencedDoc = "# Title\n\n" +
	"## Examples\n\n" +
	"```bash\n# find a passage\nlocus search x\n\n# check status\nlocus status\n```\n\n" +
	"after the block\n\n" +
	"~~~\n# tilde comment\n~~~\n\n" +
	"tail of examples\n\n" +
	"## Next\n\nnext body\n"

func TestSectionIgnoresHeadingsInFences(t *testing.T) {
	got := section(fencedDoc, "examples")
	for _, want := range []string{"# find a passage", "# check status", "locus status", "after the block", "# tilde comment", "tail of examples"} {
		if !strings.Contains(got, want) {
			t.Errorf("examples section missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "next body") {
		t.Errorf("examples section ran into the next section:\n%s", got)
	}
}

func TestSectionHeadingsOutsideFencesSplit(t *testing.T) {
	if got := section(fencedDoc, "next"); got != "next body" {
		t.Errorf("next section = %q", got)
	}
	if got := section(fencedDoc, "find a passage"); got != "" {
		t.Errorf("a comment inside a fence resolved as a section: %q", got)
	}
}

func TestSectionMismatchedFenceDoesNotClose(t *testing.T) {
	// A ~~~ line cannot close a ``` block, and a shorter fence cannot close a
	// longer one, so the "# inner" lines stay code.
	doc := "## A\n````\n```\n# inner\n~~~\n# inner2\n````\nend a\n## B\nb\n"
	got := section(doc, "a")
	if !strings.Contains(got, "# inner2") || !strings.Contains(got, "end a") {
		t.Errorf("section a = %q", got)
	}
	if got := section(doc, "b"); got != "b" {
		t.Errorf("section b = %q", got)
	}
}

func TestSectionUnterminatedFence(t *testing.T) {
	// An unterminated fence is ignored rather than running to the end of the
	// document, so later headings still split.
	doc := "## A\n```bash\n# not closed\n## B\nb body\n"
	if got := section(doc, "a"); got != "```bash" {
		t.Errorf("section a = %q", got)
	}
	if got := section(doc, "b"); got != "b body" {
		t.Errorf("section b = %q", got)
	}
}

func TestEmbeddedSectionsResolve(t *testing.T) {
	// The heading SKILL.md uses as its --section example.
	if Section("exit codes") == "" {
		t.Error(`Section("exit codes") is empty`)
	}
	if ex := Section("examples"); !strings.Contains(ex, "locus conflicts resolve") {
		t.Errorf("Examples section truncated:\n%s", ex)
	}
	// Every real heading in the embedded doc resolves to a non-empty body.
	lines := strings.Split(skillDoc, "\n")
	code := fencedLines(lines)
	n := 0
	for i, line := range lines {
		if code[i] || !(strings.HasPrefix(line, "## ") || strings.HasPrefix(line, "### ")) {
			continue
		}
		n++
		h := strings.TrimSpace(strings.TrimLeft(line, "#"))
		if Section(h) == "" {
			t.Errorf("Section(%q) is empty", h)
		}
	}
	if n == 0 {
		t.Fatal("no headings found in SKILL.md")
	}
}

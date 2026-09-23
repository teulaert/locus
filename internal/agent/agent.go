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
func Section(heading string) string {
	want := strings.ToLower(strings.TrimSpace(heading))
	var out []string
	inSection := false
	for _, line := range strings.Split(skillDoc, "\n") {
		if strings.HasPrefix(line, "#") {
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

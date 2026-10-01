package web

import (
	"regexp"
	"strings"
	"testing"
)

// innerHTMLAssign matches an innerHTML / outerHTML assignment (or
// insertAdjacentHTML call) whose value is a template literal, capturing the
// literal's body.
var innerHTMLAssign = regexp.MustCompile("(?s)(?:\\.(?:inner|outer)HTML\\s*\\+?=\\s*|insertAdjacentHTML\\([^,]*,\\s*)`([^`]*)`")

var interpolation = regexp.MustCompile(`\$\{([^}]*)\}`)

// TestAppJSDoesNotInterpolateIntoHTML guards LOC-4: file names, paths and
// extracted headings are untrusted (they arrive from other peers and from
// arbitrary documents), so the PWA must never splice them into HTML markup.
// The only interpolations allowed in an HTML template are ones whose value is
// produced by the client itself.
func TestAppJSDoesNotInterpolateIntoHTML(t *testing.T) {
	allowed := map[string]bool{
		`e.is_dir ? "📁" : iconFor(e.name)`: true, // fixed emoji set
		`e.is_dir ? "" : fmtSize(e.size)`:  true, // numeric
	}
	src := string(AppJS())
	matches := innerHTMLAssign.FindAllStringSubmatch(src, -1)
	if len(matches) == 0 {
		t.Fatal("expected at least one innerHTML template in app.js; did the pattern drift?")
	}
	for _, m := range matches {
		for _, in := range interpolation.FindAllStringSubmatch(m[1], -1) {
			expr := strings.TrimSpace(in[1])
			if !allowed[expr] {
				t.Errorf("app.js interpolates ${%s} into HTML; build the node with textContent/createElement instead:\n%s", expr, m[0])
			}
		}
	}
}

// TestAppJSKeepsTokenOutOfLocalStorage: the bearer token grants full API
// access, so it must not persist in localStorage beyond the session.
func TestAppJSKeepsTokenOutOfLocalStorage(t *testing.T) {
	src := string(AppJS())
	for _, bad := range []string{`localStorage.getItem("locus.token")`, `localStorage.setItem("locus.token"`} {
		if strings.Contains(src, bad) {
			t.Errorf("app.js still uses %s", bad)
		}
	}
}

// TestAppJSFetchesFileContentWithToken guards LOC-11: /api/v1/files/content
// requires the bearer header, which <iframe src>, <img src> and <a href>
// navigations never send. The PWA must fetch the bytes through api.req and
// only ever point src/href at a blob: URL it created.
func TestAppJSFetchesFileContentWithToken(t *testing.T) {
	src := string(AppJS())
	if !strings.Contains(src, "URL.createObjectURL") {
		t.Error("app.js no longer previews/downloads file content from a blob: URL")
	}
	fetchContent := regexp.MustCompile(`(?s)async function fetchContent\([^)]*\) \{(.*?)\n\}`).FindStringSubmatch(src)
	if fetchContent == nil || !strings.Contains(fetchContent[1], "api.req(") {
		t.Error("fetchContent must load file content through api.req so the bearer header is sent")
	}
	// Every src/href assignment must use an object URL, never an API URL.
	assign := regexp.MustCompile(`\.(src|href)\s*=\s*([^;\n]+)`)
	matches := assign.FindAllStringSubmatch(src, -1)
	if len(matches) == 0 {
		t.Fatal("expected src/href assignments in app.js; did the pattern drift?")
	}
	for _, m := range matches {
		rhs := strings.TrimSpace(m[2])
		if rhs != "objURL" && !strings.HasPrefix(rhs, "objectURLFor(") {
			t.Errorf("app.js sets .%s = %s; file content must be fetched with the token and shown via a blob: URL", m[1], rhs)
		}
	}
	if strings.Contains(string(Index()), "/api/v1/files/content") {
		t.Error("index.html links file content directly; navigations carry no bearer token")
	}
}

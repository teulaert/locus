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

// TestServiceWorkerCachesOnlyOKShellResponses guards LOC-33: the shell is
// refreshed from the network on every fetch, and a transient 5xx for "/" or
// "/app.js" must not overwrite the cached copy that is served offline.
func TestServiceWorkerCachesOnlyOKShellResponses(t *testing.T) {
	src := string(ServiceWorker())
	puts := strings.Count(src, ".put(")
	if puts == 0 {
		t.Fatal("expected a cache .put( in sw.js; did the pattern drift?")
	}
	guarded := regexp.MustCompile(`(?s)if \((?:res|response)\.ok\) \{[^}]*\.put\(`)
	if got := len(guarded.FindAllString(src, -1)); got != puts {
		t.Errorf("sw.js has %d cache .put( calls but only %d sit behind a res.ok check", puts, got)
	}
}

// TestAppJSEncodesQueryParameters guards LOC-33: every API query string is
// built by URLSearchParams (via apiURL), so no parameter is spliced in raw,
// and an unset root is omitted instead of being sent as "root=null".
func TestAppJSEncodesQueryParameters(t *testing.T) {
	src := string(AppJS())
	// A query parameter written into a string literal: "?x=" or "&x=".
	raw := regexp.MustCompile("[\"'`][^\"'`\\n]*[?&][A-Za-z_]+=")
	for _, m := range raw.FindAllString(src, -1) {
		t.Errorf("app.js builds a query string by hand (%s); use apiURL/URLSearchParams", m)
	}
	if strings.Contains(src, "qparam(") {
		t.Error("app.js still uses qparam(); use apiURL")
	}
	fn := regexp.MustCompile(`(?s)function apiURL\([^)]*\) \{(.*?)\n\}`).FindStringSubmatch(src)
	if fn == nil {
		t.Fatal("app.js has no apiURL helper")
	}
	if !strings.Contains(fn[1], "new URLSearchParams(") || !strings.Contains(fn[1], "if (state.root)") {
		t.Errorf("apiURL must encode with URLSearchParams and add root only when set:\n%s", fn[1])
	}
	if strings.Contains(src, "URLSearchParams({ root: state.root })") {
		t.Error("app.js passes a possibly-null root straight into URLSearchParams")
	}
}

// TestAppJSUploadCountsOnlySuccesses guards LOC-33: a failed upload must
// not be counted in the "Uploaded N file(s)" toast.
func TestAppJSUploadCountsOnlySuccesses(t *testing.T) {
	src := string(AppJS())
	fn := regexp.MustCompile(`(?s)async function uploadFiles\([^)]*\) \{(.*?)\n\}`).FindStringSubmatch(src)
	if fn == nil {
		t.Fatal("app.js has no uploadFiles function")
	}
	try := regexp.MustCompile(`(?s)try \{(.*?)\} catch`).FindStringSubmatch(fn[1])
	if try == nil || !strings.Contains(try[1], "ok++") {
		t.Error("uploadFiles must count a success inside the try block, after the upload resolves")
	}
	if strings.Contains(fn[1], "Uploaded ${done}") {
		t.Error("uploadFiles reports the attempted count (done) as uploaded")
	}
}

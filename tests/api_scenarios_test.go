package tests

import (
	"fmt"
	"github.com/teulaert/locus/internal/config"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/search"
)

// Scenario 12 — A file uploaded from the iPhone PWA appears as a normal file,
// synchronizes to other peers and becomes searchable.
func TestScenario12_PWAUploadBecomesNormalSyncedDocument(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	payload := "Signed contract for the solar roof installation, 14200 EUR, dated 2026-09-23."
	status, body := a.apiRaw(t, "POST", "/api/v1/files?path=/contracts/&name=contract.txt", a.Token, payload)
	if status != http.StatusCreated {
		t.Fatalf("upload failed: %d %s", status, body)
	}

	// It is an ordinary file on the normal filesystem.
	full := filepath.Join(a.Root, "contracts", "contract.txt")
	onDisk, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("upload did not write through to the filesystem: %v", err)
	}
	if string(onDisk) != payload {
		t.Fatal("uploaded bytes differ from what landed on disk")
	}
	info := decode(t, body)
	if info["path"] != "/contracts/contract.txt" {
		t.Fatalf("upload response path = %v", info["path"])
	}

	// It synchronizes to the other peer.
	waitFor(t, 30*time.Second, func() bool { return b.has(t, "contracts/contract.txt") }, "upload to propagate to the second peer")
	if got, _ := b.read(t, "contracts/contract.txt"); got != payload {
		t.Fatal("propagated upload differs")
	}

	// It becomes searchable on both peers.
	waitFor(t, 30*time.Second, func() bool {
		return len(a.search(t, "solar roof contract", search.Options{Limit: 10})) > 0 && len(b.search(t, "solar roof contract", search.Options{Limit: 10})) > 0
	}, "upload to become searchable")
}

// Scenario 13 — Rename/move/delete from the PWA propagates normally.
func TestScenario13_PWAFileOperationsPropagate(t *testing.T) {
	c := newCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	// Create a folder.
	status, body := a.apiCall(t, "POST", "/api/v1/directories", `{"path":"/Archive"}`)
	if status != http.StatusCreated {
		t.Fatalf("mkdir failed: %d %s", status, body)
	}

	// Upload a file into it.
	status, body = a.apiRaw(t, "POST", "/api/v1/files?path=/Archive/&name=report.txt", a.Token, "quarterly report contents")
	if status != http.StatusCreated {
		t.Fatalf("upload: %d %s", status, body)
	}
	waitFor(t, 30*time.Second, func() bool { return b.has(t, "Archive/report.txt") }, "upload to propagate")
	// Directory structure reappears on the peer together with its content.
	if !b.has(t, "Archive") {
		t.Fatal("parent directory must exist on the peer once files arrive")
	}

	// Rename it.
	status, body = a.apiCall(t, "POST", "/api/v1/files/move", `{"from":"/Archive/report.txt","to":"/Archive/report-final.txt"}`)
	if status != http.StatusOK {
		t.Fatalf("rename failed: %d %s", status, body)
	}
	waitFor(t, 30*time.Second, func() bool {
		return b.has(t, "Archive/report-final.txt") && !b.has(t, "Archive/report.txt")
	}, "rename to propagate")

	// Move it again, to a different directory.
	a.apiCall(t, "POST", "/api/v1/directories", `{"path":"/Final"}`)
	status, body = a.apiCall(t, "POST", "/api/v1/files/move", `{"from":"/Archive/report-final.txt","to":"/Final/report-final.txt"}`)
	if status != http.StatusOK {
		t.Fatalf("move failed: %d %s", status, body)
	}
	waitFor(t, 30*time.Second, func() bool { return b.has(t, "Final/report-final.txt") }, "move to propagate")

	// Delete it.
	status, body = a.apiCall(t, "DELETE", "/api/v1/files?path=/Final/report-final.txt", "")
	if status != http.StatusOK {
		t.Fatalf("delete failed: %d %s", status, body)
	}
	waitFor(t, 30*time.Second, func() bool { return !a.has(t, "Final/report-final.txt") && !b.has(t, "Final/report-final.txt") }, "delete to propagate")
}

// Scenario 14 — Attempts to escape a configured root through API paths or
// symlinks fail.
func TestScenario14_APIPathAndSymlinkEscapesFail(t *testing.T) {
	c := newCluster(t, 2)
	a := c.nodes[0]

	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}

	attacks := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"read traversal", "GET", "/api/v1/files/content?path=../../etc/passwd", ""},
		{"read deep traversal", "GET", "/api/v1/files/content?path=a/b/../../../../etc/passwd", ""},
		{"read outside absolute", "GET", "/api/v1/files/content?path=" + filepath.Join(outside, "secret.txt"), ""},
		{"list traversal", "GET", "/api/v1/files?path=..", ""},
		{"delete traversal", "DELETE", "/api/v1/files?path=../escape.txt", ""},
		{"move out of root", "POST", "/api/v1/files/move", `{"from":"/x.txt","to":"../../etc/y.txt"}`},
		{"mkdir escape", "POST", "/api/v1/directories", `{"path":"../../tmp/evil"}`},
		{"upload traversal name", "POST", "/api/v1/files?path=/&name=../../evil.txt", "payload"},
	}
	for _, at := range attacks {
		t.Run(at.name, func(t *testing.T) {
			status, body := a.apiRaw(t, at.method, at.path, a.Token, at.body)
			if status == http.StatusOK || status == http.StatusCreated {
				t.Fatalf("%s was accepted: %d %s", at.name, status, body)
			}
			if status != http.StatusBadRequest && status != http.StatusNotFound && status != http.StatusForbidden {
				t.Fatalf("%s returned unexpected %d (%s)", at.name, status, body)
			}
		})
	}

	// A symlink inside the root that points outside must not be readable.
	link := filepath.Join(a.Root, "escape-link")
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), link); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	status, _ := a.apiRaw(t, "GET", "/api/v1/files/content?path=escape-link", a.Token, "")
	if status == http.StatusOK {
		t.Fatal("symlink escape read accepted")
	}
	// Neither may it be used to overwrite outside content.
	status, _ = a.apiRaw(t, "POST", "/api/v1/files?path=/escape-link&name=x", a.Token, "overwrite")
	if status == http.StatusOK || status == http.StatusCreated {
		t.Fatalf("write through symlink accepted: %d", status)
	}
	if b, err := os.ReadFile(filepath.Join(outside, "secret.txt")); err == nil && string(b) != "secret" {
		t.Fatalf("file outside root was modified: %q", b)
	}
	// A symlinked *directory* is just as much an escape: intermediate
	// components must be checked, not only the leaf (LOC-3).
	if err := os.MkdirAll(filepath.Join(outside, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "sub", "deep.txt"), []byte("deep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(a.Root, "linkdir")); err != nil {
		t.Fatal(err)
	}
	for _, at := range []struct{ name, method, path, body string }{
		{"read through linked dir", "GET", "/api/v1/files/content?path=linkdir/secret.txt", ""},
		{"read deep through linked dir", "GET", "/api/v1/files/content?path=linkdir/sub/deep.txt", ""},
		{"list linked dir", "GET", "/api/v1/files?path=linkdir/sub", ""},
		{"upload into linked dir", "POST", "/api/v1/files?path=/linkdir/sub&name=newfile.txt", "planted"},
		{"overwrite through linked dir", "POST", "/api/v1/files?path=/linkdir&name=secret.txt", "overwrite"},
		{"mkdir in linked dir", "POST", "/api/v1/directories", `{"path":"linkdir/sub/evil"}`},
		{"delete file in linked dir", "DELETE", "/api/v1/files?path=linkdir/secret.txt", ""},
		{"delete linked subdir", "DELETE", "/api/v1/files?path=linkdir/sub", ""},
	} {
		status, body := a.apiRaw(t, at.method, at.path, a.Token, at.body)
		if status == http.StatusOK || status == http.StatusCreated || status == http.StatusNoContent {
			t.Errorf("%s was accepted: %d %s", at.name, status, body)
		}
	}
	if b, err := os.ReadFile(filepath.Join(outside, "secret.txt")); err != nil || string(b) != "secret" {
		t.Fatalf("outside secret.txt changed: %q, %v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(outside, "sub", "deep.txt")); err != nil || string(b) != "deep" {
		t.Fatalf("outside sub/deep.txt changed or removed: %q, %v", b, err)
	}
	for _, p := range []string{"sub/newfile.txt", "sub/evil"} {
		if _, err := os.Lstat(filepath.Join(outside, filepath.FromSlash(p))); err == nil {
			t.Fatalf("API created %s outside the root", p)
		}
	}
	// Unknown roots are refused.
	if status, _ := a.apiRaw(t, "GET", "/api/v1/files?path=/&root=/nonexistent-root", a.Token, ""); status != http.StatusBadRequest {
		t.Fatalf("unconfigured root returned %d", status)
	}
}

// The API requires application-level authentication: generic network access
// must not grant document access.
func TestAPIRequiresBearerToken(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	for _, path := range []string{"/api/v1/status", "/api/v1/files?path=/", "/api/v1/search?q=x", "/api/v1/conflicts", "/api/v1/peers"} {
		if status, _ := a.apiRaw(t, "GET", path, "", ""); status != http.StatusUnauthorized {
			t.Errorf("GET %s without token returned %d, want 401", path, status)
		}
		if status, _ := a.apiRaw(t, "GET", path, "wrong-token", ""); status != http.StatusUnauthorized {
			t.Errorf("GET %s with a wrong token returned %d, want 401", path, status)
		}
	}
	// The audit log records the refusals, summarized per source rather than
	// one row per request (LOC-18).
	a.App.MaintainAudit()
	entries, err := a.App.Store.ListAudit(50)
	if err != nil {
		t.Fatal(err)
	}
	var found int
	for _, e := range entries {
		if e.Action == "unauthorized" {
			found++
		}
	}
	if found == 0 {
		t.Fatal("unauthorized attempts must be auditable")
	}
	if found >= 10 {
		t.Fatalf("%d audit rows for 10 refused requests; refusals must be aggregated", found)
	}
}

// The embedded PWA is served by Locus itself (no Node.js on the peer).
func TestPWAShellServedByGoBinary(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	resp, err := http.Get(a.APIURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := readAllAndClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d", resp.StatusCode)
	}
	for _, want := range []string{"Locus", "searchInput", "/app.js", "manifest.webmanifest", "uploadInput"} {
		if !strings.Contains(body, want) {
			t.Errorf("PWA shell missing %q", want)
		}
	}
	if resp.Header.Get("Content-Type") == "" {
		t.Error("missing content type for PWA shell")
	}
	// Static assets are served and installable as a PWA.
	for path, want := range map[string]string{
		"/app.js":               "api.req",
		"/style.css":            "--accent",
		"/manifest.webmanifest": "\"name\": \"Locus\"",
		"/sw.js":                "locus-shell",
	} {
		resp, err := http.Get(a.APIURL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := readAllAndClose(resp)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d", path, resp.StatusCode)
		}
		if !strings.Contains(b, want) {
			t.Errorf("GET %s does not contain %q", path, want)
		}
	}
	// The service worker only caches the shell, never document contents.
	resp, _ = http.Get(a.APIURL + "/sw.js")
	body, _ = readAllAndClose(resp)
	if !strings.Contains(body, "never") || !strings.Contains(body, "fetch(event.request)") {
		t.Error("service worker must not cache document contents")
	}
}

func readAllAndClose(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	b := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		b = append(b, buf[:n]...)
		if err != nil {
			return string(b), nil
		}
		if len(b) > 4<<20 {
			return string(b), fmt.Errorf("response too large")
		}
	}
}

// Browsing, previews and downloads through the API.
func TestAPIBrowsingDownloadAndConflicts(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	a.write(t, "alpha.txt", "alpha content")
	a.write(t, "sub/beta.md", "# Beta\n\nbeta content")
	waitFor(t, 20*time.Second, func() bool {
		status, body := a.apiCall(t, "GET", "/api/v1/files?path=/", "")
		return status == http.StatusOK && strings.Contains(string(body), "alpha.txt")
	}, "files to be listed")

	status, body := a.apiCall(t, "GET", "/api/v1/files?path=/sub", "")
	if status != http.StatusOK || !strings.Contains(string(body), "beta.md") {
		t.Fatalf("directory listing failed: %d %s", status, body)
	}
	status, body = a.apiRaw(t, "GET", "/api/v1/files/content?path=/alpha.txt", a.Token, "")
	if status != http.StatusOK || string(body) != "alpha content" {
		t.Fatalf("download failed: %d %q", status, body)
	}
	if status, _ := a.apiRaw(t, "GET", "/api/v1/files/content?path=/missing.txt", a.Token, ""); status != http.StatusNotFound {
		t.Fatalf("missing file returned %d", status)
	}

	// Status and conflicts endpoints answer the PWA status bar needs.
	status, body = a.apiCall(t, "GET", "/api/v1/status", "")
	if status != http.StatusOK {
		t.Fatalf("status: %d %s", status, body)
	}
	st := decode(t, body)
	for _, key := range []string{"peer_id", "hostname", "roots", "index", "embedding", "conflicts"} {
		if _, ok := st[key]; !ok {
			t.Errorf("status response missing %q", key)
		}
	}
	status, body = a.apiCall(t, "GET", "/api/v1/conflicts", "")
	if status != http.StatusOK {
		t.Fatalf("conflicts: %d %s", status, body)
	}
	waitFor(t, 30*time.Second, func() bool {
		status, body := a.apiCall(t, "GET", "/api/v1/search?q=alpha", "")
		return status == http.StatusOK && strings.Contains(string(body), "alpha.txt")
	}, "api search to return the indexed file")
	status, body = a.apiCall(t, "GET", "/api/v1/search?q=alpha&context=1", "")
	if status != http.StatusOK || !strings.Contains(string(body), "context") {
		t.Fatalf("api search --context failed: %d %s", status, body)
	}
	if strings.Contains(string(body), `"results": null`) {
		t.Fatal("results must be an array, never null")
	}
}

// Uploads are size limited and configurable.
func TestUploadSizeLimit(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	original := a.Cfg.UploadMaxBytes
	defer func() { a.Cfg.UploadMaxBytes = original }()
	a.Cfg.UploadMaxBytes = 64

	body := strings.Repeat("x", 128)
	status, resp := a.apiRaw(t, "POST", "/api/v1/files?path=/&name=toolarge.txt", a.Token, body)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized upload returned %d (%s)", status, resp)
	}
	if _, err := os.Stat(filepath.Join(a.Root, "toolarge.txt")); !os.IsNotExist(err) {
		t.Fatal("oversized upload left a file behind")
	}
}

func TestSyncEndpointAndPeers(t *testing.T) {
	c := newCluster(t, 2)
	a := c.nodes[0]
	status, body := a.apiCall(t, "POST", "/api/v1/sync", "")
	if status != http.StatusOK {
		t.Fatalf("POST /api/v1/sync: %d %s", status, body)
	}
	stats := decode(t, body)
	if _, ok := stats["peers"]; !ok {
		t.Errorf("sync response missing peers: %v", stats)
	}
	status, body = a.apiCall(t, "GET", "/api/v1/peers", "")
	if status != http.StatusOK {
		t.Fatalf("GET /api/v1/peers: %d %s", status, body)
	}
	peers := decode(t, body)
	list, ok := peers["peers"].([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("peers endpoint must list the other peer: %v", peers)
	}
	entry := list[0].(map[string]any)
	if entry["state"] != "online" {
		t.Errorf("peer state = %v", entry["state"])
	}
	_ = fmt.Sprintf("%v", entry)
}

// LOC-11: file contents are served only to requests that carry the bearer
// header (so the PWA must fetch them rather than navigate to them), with a
// type an <img>/<iframe> can preview from a blob, and never as a page the
// browser would sniff or render in the API origin.
func TestFileContentPreviewAndDownloadHeaders(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	a.write(t, "doc.pdf", "%PDF-1.4 fake")
	a.write(t, "pic.png", "\x89PNG\r\n\x1a\nfake")
	a.write(t, "page.html", "<script>alert(1)</script>")
	a.write(t, "notes.txt", "plain")

	// A browser navigation (iframe src, img src, a href) sends no
	// Authorization header and is refused.
	if status, _ := a.apiRaw(t, "GET", "/api/v1/files/content?path=/doc.pdf", "", ""); status != http.StatusUnauthorized {
		t.Fatalf("content without token returned %d, want 401", status)
	}

	for path, wantType := range map[string]string{
		"/doc.pdf":   "application/pdf",
		"/pic.png":   "image/png",
		"/page.html": "text/html; charset=utf-8",
		"/notes.txt": "text/plain; charset=utf-8",
	} {
		req, err := http.NewRequest("GET", a.APIURL+"/api/v1/files/content?path="+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+a.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		readAllAndClose(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d", path, resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); got != wantType {
			t.Errorf("GET %s Content-Type = %q, want %q", path, got, wantType)
		}
		if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("GET %s X-Content-Type-Options = %q, want nosniff", path, got)
		}
		disp := resp.Header.Get("Content-Disposition")
		if !strings.HasPrefix(disp, "attachment") || !strings.Contains(disp, strings.TrimPrefix(path, "/")) {
			t.Errorf("GET %s Content-Disposition = %q, want attachment with the file name", path, disp)
		}
	}
}

// LOC-21: a root whose name contains characters that are special in a query
// string (`+`, `&`, `%`, `#`) is addressable by every file operation. Query
// callers percent-encode it; move and mkdir carry it in the JSON body, which
// the server must use as sent rather than smuggle through a query string.
func TestAPIRootWithQueryReservedCharacters(t *testing.T) {
	ports := allocatePorts(t, 1)
	a := newNode(t, "peer1", ports[0], ports, 0)
	odd := filepath.Join(a.Dir, "c++ a&b 100% #1")
	if err := os.MkdirAll(odd, 0o755); err != nil {
		t.Fatal(err)
	}
	a.Cfg.Roots = append(a.Cfg.Roots, config.Root{Path: odd})
	c := &cluster{t: t, nodes: []*node{a}}
	a.start(t)
	t.Cleanup(c.stop)

	rootQ := "&root=" + url.QueryEscape(odd)
	rootJSON := mustMarshal(t, odd)

	// Upload into the odd root, then list and read it back.
	status, body := a.apiRaw(t, "POST", "/api/v1/files?path=/&name=notes.txt"+rootQ, a.Token, "odd root contents")
	if status != http.StatusCreated {
		t.Fatalf("upload: %d %s", status, body)
	}
	if got, err := os.ReadFile(filepath.Join(odd, "notes.txt")); err != nil || string(got) != "odd root contents" {
		t.Fatalf("upload did not land in the odd root: %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(a.Root, "notes.txt")); !os.IsNotExist(err) {
		t.Fatal("upload landed in the default root instead of the requested one")
	}
	status, body = a.apiCall(t, "GET", "/api/v1/files?path=/"+rootQ, "")
	if status != http.StatusOK {
		t.Fatalf("list root: %d %s", status, body)
	}
	if listing := decode(t, body); listing["root"] != odd || !strings.Contains(string(body), `"notes.txt"`) {
		t.Fatalf("listing of the odd root = %s", body)
	}
	status, body = a.apiRaw(t, "GET", "/api/v1/files/content?path=/notes.txt"+rootQ, a.Token, "")
	if status != http.StatusOK || string(body) != "odd root contents" {
		t.Fatalf("read: %d %q", status, body)
	}

	// Create a folder and move the file into it; both take the root from JSON.
	status, body = a.apiCall(t, "POST", "/api/v1/directories", `{"path":"/Archive","root":`+rootJSON+`}`)
	if status != http.StatusCreated {
		t.Fatalf("mkdir in odd root: %d %s", status, body)
	}
	if st, err := os.Stat(filepath.Join(odd, "Archive")); err != nil || !st.IsDir() {
		t.Fatalf("mkdir did not create the folder in the odd root: %v", err)
	}
	status, body = a.apiCall(t, "POST", "/api/v1/files/move", `{"from":"/notes.txt","to":"/Archive/notes.txt","root":`+rootJSON+`}`)
	if status != http.StatusOK {
		t.Fatalf("move in odd root: %d %s", status, body)
	}
	if _, err := os.Stat(filepath.Join(odd, "Archive", "notes.txt")); err != nil {
		t.Fatalf("move did not land in the odd root: %v", err)
	}
	status, body = a.apiCall(t, "GET", "/api/v1/files?path=/Archive"+rootQ, "")
	if status != http.StatusOK || !strings.Contains(string(body), `"Archive/notes.txt"`) {
		t.Fatalf("list subfolder: %d %s", status, body)
	}

	// Delete it.
	status, body = a.apiCall(t, "DELETE", "/api/v1/files?path=/Archive/notes.txt"+rootQ, "")
	if status != http.StatusOK {
		t.Fatalf("delete in odd root: %d %s", status, body)
	}
	if _, err := os.Stat(filepath.Join(odd, "Archive", "notes.txt")); !os.IsNotExist(err) {
		t.Fatal("delete left the file in the odd root")
	}

	// A root that is not configured is still refused, from JSON and query alike.
	status, _ = a.apiCall(t, "POST", "/api/v1/directories", `{"path":"/x","root":`+mustMarshal(t, odd+"x")+`}`)
	if status != http.StatusBadRequest {
		t.Fatalf("mkdir in an unconfigured root returned %d, want 400", status)
	}
	status, _ = a.apiCall(t, "GET", "/api/v1/files?path=/&root="+url.QueryEscape(odd+"x"), "")
	if status != http.StatusBadRequest {
		t.Fatalf("listing an unconfigured root returned %d, want 400", status)
	}
}

// TestAPISearchScopedToRoot guards LOC-33: the PWA sends the selected root
// with its search, and the endpoint used to ignore it, returning hits from
// every root that the PWA then tried to open in the selected one. Without
// root the search still spans every root; an unconfigured root (including
// the literal "null" an unset JavaScript value would produce) is refused.
func TestAPISearchScopedToRoot(t *testing.T) {
	ports := allocatePorts(t, 1)
	a := newNode(t, "peer1", ports[0], ports, 0)
	second := filepath.Join(a.Dir, "second root & co")
	if err := os.MkdirAll(second, 0o755); err != nil {
		t.Fatal(err)
	}
	a.Cfg.Roots = append(a.Cfg.Roots, config.Root{Path: second})
	c := &cluster{t: t, nodes: []*node{a}}
	a.start(t)
	t.Cleanup(c.stop)

	if status, body := a.apiRaw(t, "POST", "/api/v1/files?path=/&name=main.txt", a.Token, "zebra crossing in the main root"); status != http.StatusCreated {
		t.Fatalf("upload to default root: %d %s", status, body)
	}
	if status, body := a.apiRaw(t, "POST", "/api/v1/files?path=/&name=other.txt&root="+url.QueryEscape(second), a.Token, "zebra stripes in the second root"); status != http.StatusCreated {
		t.Fatalf("upload to second root: %d %s", status, body)
	}

	// hits maps each result path to the root it was found in.
	hits := func(query string) (int, map[string]string) {
		status, body := a.apiCall(t, "GET", "/api/v1/search?"+query, "")
		got := map[string]string{}
		if status != http.StatusOK {
			return status, got
		}
		results, _ := decode(t, body)["results"].([]any)
		for _, r := range results {
			m, _ := r.(map[string]any)
			path, _ := m["path"].(string)
			root, _ := m["root"].(string)
			got[path] = root
		}
		return status, got
	}
	waitFor(t, 30*time.Second, func() bool {
		_, got := hits("q=zebra")
		return len(got) == 2
	}, "both roots to be indexed and searchable without a root filter")

	if _, got := hits("q=zebra&root=" + url.QueryEscape(second)); len(got) != 1 || got["other.txt"] != second {
		t.Fatalf("search scoped to the second root returned %v", got)
	}
	if _, got := hits("q=zebra&root=" + url.QueryEscape(a.Root)); len(got) != 1 || got["main.txt"] != a.Root {
		t.Fatalf("search scoped to the default root returned %v", got)
	}
	for _, bad := range []string{"null", second + "x"} {
		if status, _ := hits("q=zebra&root=" + url.QueryEscape(bad)); status != http.StatusBadRequest {
			t.Fatalf("search with unconfigured root %q returned %d, want 400", bad, status)
		}
	}
}

// A DELETE of a path that does not exist is a 404 and leaves no audit row,
// and file-operation errors name nothing of the host filesystem: the data
// directory and root are absolute paths the client never needs to see
// (LOC-28).
func TestAPIMissingPathsAndErrorsHideHostPaths(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	if err := os.WriteFile(filepath.Join(a.Root, "keep.txt"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	noHostPath := func(what string, body []byte) {
		t.Helper()
		for _, abs := range []string{a.Dir, a.Root, a.Cfg.DataDir} {
			if strings.Contains(string(body), abs) {
				t.Errorf("%s response leaks the absolute path %s: %s", what, abs, body)
			}
		}
		if decode(t, body)["error"] == nil {
			t.Errorf("%s response has no error field: %s", what, body)
		}
	}
	countDeletes := func() int {
		t.Helper()
		entries, err := a.App.Store.ListAudit(100)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range entries {
			if e.Action == "delete" {
				n++
			}
		}
		return n
	}

	status, body := a.apiCall(t, "DELETE", "/api/v1/files?path=/nope.txt", "")
	if status != http.StatusNotFound {
		t.Fatalf("DELETE of a missing path returned %d %s, want 404", status, body)
	}
	noHostPath("delete", body)
	if n := countDeletes(); n != 0 {
		t.Fatalf("a refused delete wrote %d audit rows, want 0", n)
	}

	status, body = a.apiCall(t, "GET", "/api/v1/files?path=/no/such/dir", "")
	if status != http.StatusNotFound {
		t.Fatalf("listing an unknown directory returned %d %s, want 404", status, body)
	}
	noHostPath("list", body)

	status, body = a.apiCall(t, "POST", "/api/v1/files/move", `{"from":"/missing.txt","to":"/Elsewhere/missing.txt"}`)
	if status != http.StatusNotFound {
		t.Fatalf("move of a missing source returned %d %s, want 404", status, body)
	}
	noHostPath("move", body)
	if _, err := os.Stat(filepath.Join(a.Root, "Elsewhere")); !os.IsNotExist(err) {
		t.Fatal("a failed move created the destination folder")
	}

	// A move whose destination parent is a file fails inside the filesystem;
	// the error still names only the operation and its cause.
	status, body = a.apiCall(t, "POST", "/api/v1/files/move", `{"from":"/keep.txt","to":"/keep.txt/inner.txt"}`)
	if status < 400 {
		t.Fatalf("move under a file returned %d %s, want an error", status, body)
	}
	noHostPath("move under a file", body)
	if _, err := os.Stat(filepath.Join(a.Root, "keep.txt")); err != nil {
		t.Fatalf("failed move disturbed the source: %v", err)
	}

	// An existing file still deletes and is audited once.
	status, body = a.apiCall(t, "DELETE", "/api/v1/files?path=/keep.txt", "")
	if status != http.StatusOK {
		t.Fatalf("delete of an existing file: %d %s", status, body)
	}
	if n := countDeletes(); n != 1 {
		t.Fatalf("%d delete audit rows after one real delete, want 1", n)
	}
}

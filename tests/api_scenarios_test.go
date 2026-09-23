package tests

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"locus/internal/search"
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
	// The audit log records the refusals.
	entries, err := a.App.Store.ListAudit(50)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if e.Action == "unauthorized" {
			found = true
		}
	}
	if !found {
		t.Fatal("unauthorized attempts must be auditable")
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

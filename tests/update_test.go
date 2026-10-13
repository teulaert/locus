//go:build unix

package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/release"
	"github.com/teulaert/locus/internal/update"
)

// buildLocusWith builds the locus binary with extra -X settings: the
// release key it trusts and the commit it claims, as CI's ldflags set them.
func buildLocusWith(t *testing.T, out string, x map[string]string) {
	t.Helper()
	var flags []string
	for k, v := range x {
		flags = append(flags, "-X", k+"="+v)
	}
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags", strings.Join(flags, " "), "-o", out, "./cmd/locus")
	cmd.Dir = ".."
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, b)
	}
}

// serveRelease is a stand-in for the GitHub releases API on loopback,
// publishing the files in dist as one main-channel build.
func serveRelease(t *testing.T, tag, dist string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("GET /repos/teulaert/locus/releases", func(w http.ResponseWriter, r *http.Request) {
		entries, _ := os.ReadDir(dist)
		assets := []map[string]any{}
		for _, e := range entries {
			fi, _ := e.Info()
			assets = append(assets, map[string]any{"name": e.Name(), "size": fi.Size(), "browser_download_url": srv.URL + "/download/" + e.Name()})
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"tag_name": tag, "prerelease": true, "created_at": time.Now().UTC(), "assets": assets}})
	})
	mux.HandleFunc("GET /download/{name}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(dist, filepath.Base(r.PathValue("name"))))
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// The whole update path through the real binary (LOC-86): a CI-style signed
// release on a fake GitHub, `locus update --check` and `locus status`, the
// running daemon installing the build and re-executing into it in the same
// process, a rollback through the daemon, and the swap-only path with no
// daemon. Nothing leaves loopback. Unix only: re-exec is.
func TestUpdateThroughTheBinary(t *testing.T) {
	pubHex, privB64, err := release.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	priv, _ := release.ParsePrivateKey(privB64)
	const (
		oldCommit = "0123456aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		newCommit = "89abcdebbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	oldTime := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	newTime := oldTime.Add(48 * time.Hour)
	dir := t.TempDir()
	ldflags := func(version, commit string, at time.Time) map[string]string {
		return map[string]string{
			"github.com/teulaert/locus/internal/update.trustedKeyHex": pubHex,
			"github.com/teulaert/locus/internal/update.BuildCommit":   commit,
			"github.com/teulaert/locus/internal/update.BuildTime":     at.Format(time.RFC3339),
			"github.com/teulaert/locus/internal/sync.BuildVersion":    version,
		}
	}
	exe := filepath.Join(dir, "bin", "locus")
	buildLocusWith(t, exe, ldflags("0123456", oldCommit, oldTime))
	oldBinary, _ := os.ReadFile(exe)
	dist := filepath.Join(dir, "dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		t.Fatal(err)
	}
	newPath := filepath.Join(dist, update.ArtifactName(runtime.GOOS, runtime.GOARCH))
	buildLocusWith(t, newPath, ldflags("89abcde", newCommit, newTime))
	newBinary, _ := os.ReadFile(newPath)
	_, manifest, err := release.BuildManifest(dist, release.Meta{Version: "89abcde", Commit: newCommit, CommitTime: newTime, Created: time.Now(), Channel: update.ChannelMain})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, update.ManifestFile), manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, update.SignatureFile), release.Sign(priv, manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	gh := serveRelease(t, "build-89abcde", dist)

	root := filepath.Join(dir, "docs")
	data := filepath.Join(dir, "data")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := fmt.Sprintf(`{"roots": [%q], "data_dir": %q,
  "peer": {"addr": "127.0.0.1", "port": %d}, "api": {"addr": "127.0.0.1", "port": %d},
  "discovery": {"port": %d, "gateways": false}, "update": {"url": %q}}`,
		root, data, freePort(t), freePort(t), freePort(t), gh.URL)
	c := &cli{t: t, binary: exe, config: filepath.Join(dir, "config.json"), dataDir: data, home: filepath.Join(dir, "home")}
	if err := os.WriteFile(c.config, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	// A build without a release key refuses to update anything, before it
	// contacts anyone. The real binary has the release key compiled in, so
	// this one is built with the key emptied.
	noKey := filepath.Join(dir, "bin", "locus-nokey")
	noKeyFlags := ldflags("0123456", oldCommit, oldTime)
	noKeyFlags["github.com/teulaert/locus/internal/update.trustedKeyHex"] = ""
	buildLocusWith(t, noKey, noKeyFlags)
	c0 := &cli{t: t, binary: noKey, config: c.config, dataDir: data, home: c.home}
	if code, _, stderr := c0.run("update", "--check"); code != 4 || !strings.Contains(stderr, "no release key") {
		t.Fatalf("update without a compiled-in key: exit %d, stderr %q; want 4", code, stderr)
	}

	version := func() (string, map[string]any) {
		st := c.mustJSON("status", "--json")
		u, _ := st["update"].(map[string]any)
		return fmt.Sprint(st["version"]), u
	}

	// No daemon: --check reports, status says it is available.
	chk := c.mustJSON("update", "--check", "--json")
	if chk["available"] != true || chk["latest"] != "build-89abcde" || chk["installed"] != false {
		t.Fatalf("update --check --json: %v", chk)
	}
	if _, stdout, _ := c.run("update", "--check"); !strings.Contains(stdout, "available: build-89abcde") {
		t.Fatalf("update --check: %q", stdout)
	}
	if _, stdout, _ := c.run("status"); !strings.Contains(stdout, "update  available: build-89abcde (auto off; run: locus update)") {
		t.Fatalf("status lacks the update line:\n%s", stdout)
	}
	_, u := version()
	requireKeys(t, u, "auto", "channel", "running", "latest", "state", "checked_at")

	// With a daemon: it installs, shuts down and re-executes itself.
	serve := exec.Command(exe, "--config", c.config, "--data-dir", data, "serve")
	serve.Env = c.env()
	var serveLog bytes.Buffer
	serve.Stdout, serve.Stderr = &serveLog, &serveLog
	if err := serve.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var exitErr error
	go func() { exitErr = serve.Wait(); close(exited) }()
	t.Cleanup(func() {
		_ = serve.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(30 * time.Second):
			_ = serve.Process.Kill()
		}
		if t.Failed() {
			t.Logf("serve output:\n%s", serveLog.String())
		}
	})
	daemonRuns := func(want string) func() bool {
		return func() bool {
			select {
			case <-exited:
				t.Fatalf("the daemon process exited (%v) instead of re-executing", exitErr)
			default:
			}
			code, stdout, _ := c.run("status", "--json")
			var st map[string]any
			return code == 0 && json.Unmarshal([]byte(stdout), &st) == nil && st["daemon"] == true && st["version"] == want
		}
	}
	waitFor(t, 30*time.Second, daemonRuns("0123456"), "the daemon to serve the old build")

	rep := c.mustJSON("update", "--json")
	if rep["installed"] != true || rep["restarting"] != true || rep["binary"] != exe {
		t.Fatalf("update through the daemon: %v", rep)
	}
	waitFor(t, 30*time.Second, daemonRuns("89abcde"), "the same daemon process to serve the new build")
	if b, _ := os.ReadFile(exe); !bytes.Equal(b, newBinary) {
		t.Fatal("installed binary differs from the release artifact")
	}
	if b, _ := os.ReadFile(update.PreviousPath(exe)); !bytes.Equal(b, oldBinary) {
		t.Fatal("previous binary not kept")
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(data, "update-health.json"))
		return os.IsNotExist(err)
	}, "the new build to clear the health marker once serving")
	if _, u := version(); u["state"] != update.StateUpToDate || u["running"] != "89abcde" {
		t.Fatalf("status update after the restart: %v", u)
	}

	rep = c.mustJSON("update", "--rollback", "--json")
	if rep["rolled_back"] != true || rep["restarting"] != true {
		t.Fatalf("rollback through the daemon: %v", rep)
	}
	waitFor(t, 30*time.Second, daemonRuns("0123456"), "the daemon to run the previous build again")
	if _, u := version(); u["state"] != update.StateRolledBack {
		t.Fatalf("status update after the rollback: %v", u)
	}

	_ = serve.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
		if exitErr != nil {
			t.Fatalf("daemon exit after SIGTERM: %v", exitErr)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("daemon did not stop")
	}

	// No daemon: the swap alone, picked up by the next start.
	code, stdout, stderr := c.run("update")
	if code != 0 || !strings.Contains(stdout, "installed build-89abcde") || !strings.Contains(stdout, "no daemon is running here") {
		t.Fatalf("update without a daemon: exit %d\n%s%s", code, stdout, stderr)
	}
	if b, _ := os.ReadFile(exe); !bytes.Equal(b, newBinary) {
		t.Fatal("update without a daemon did not install the release")
	}
	if code, stdout, stderr := c.run("update", "--rollback"); code != 0 || !strings.Contains(stdout, "rolled back") {
		t.Fatalf("rollback without a daemon: exit %d\n%s%s", code, stdout, stderr)
	}
	if b, _ := os.ReadFile(exe); !bytes.Equal(b, oldBinary) {
		t.Fatal("rollback without a daemon did not restore the previous binary")
	}
}

package tests

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/config"
)

// A read-only command must not create daemon.lock: run as root against a
// service's data directory, it left a root-owned lock that the service user
// could not open, and the daemon restart-looped (LOC-64).
func TestCLIStatusDoesNotCreateDaemonLock(t *testing.T) {
	c := newAgentCLI(t, nil)
	c.mustJSON("status", "--json")
	c.mustJSON("peers", "--json")
	if _, err := os.Stat(filepath.Join(c.dataDir, "daemon.lock")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("status created daemon.lock (stat: %v)", err)
	}
}

// `locus init --system` writes a service configuration and a data directory
// owned by the service user, and prints the full peer id other peers need for
// authorized_peers (LOC-68). The test uses the current user as the service
// user, which needs no privileges; --config and --data-dir stand in for
// /etc/locus/config.json and /var/lib/locus.
func TestCLIInitSystem(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("init --system is Unix-only")
	}
	me, err := user.Current()
	if err != nil {
		t.Skip("no current user:", err)
	}
	dir := t.TempDir()
	c := &cli{
		t:       t,
		binary:  locusBinary(t),
		config:  filepath.Join(dir, "etc", "locus", "config.json"),
		dataDir: filepath.Join(dir, "data"),
		home:    filepath.Join(dir, "home"),
	}
	t.Cleanup(c.requireDefaultDirsUntouched)

	// A server peer names its roots; the service user's ~/Documents is not one.
	if code, _, stderr := c.run("init", "--system", "--user", me.Username); code != 4 || !strings.Contains(stderr, "--root") {
		t.Fatalf("init --system without --root: exit %d, stderr %q; want 4 asking for --root", code, stderr)
	}
	if code, _, stderr := c.run("init", "--system", "--user", "no-such-locus-user", "--root", filepath.Join(dir, "docs")); code != 4 || !strings.Contains(stderr, "useradd") {
		t.Fatalf("init --system for a missing user: exit %d, stderr %q; want 4 explaining how to create it", code, stderr)
	}
	if _, err := os.Stat(c.config); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("refused init wrote %s", c.config)
	}

	root := filepath.Join(dir, "backup", "Documents")
	out := c.mustJSON("init", "--system", "--user", me.Username, "--root", root, "--json")
	requireKeys(t, out, "config", "data_dir", "peer_id", "roots", "user")
	if out["user"] != me.Username || out["config"] != c.config || out["data_dir"] != c.dataDir {
		t.Fatalf("init --system reported %v", out)
	}
	if _, err := os.Stat(filepath.Join(c.dataDir, "identity.json")); err != nil {
		t.Fatalf("no identity in the data directory: %v", err)
	}
	if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
		t.Fatalf("root not created: %v", err)
	}
	// The service user must be able to reach its configuration.
	if fi, err := os.Stat(filepath.Dir(c.config)); err != nil || fi.Mode().Perm()&0o055 != 0o055 {
		t.Fatalf("config directory is not traversable by the service user: %v %v", fi.Mode(), err)
	}
	b, err := os.ReadFile(c.config)
	if err != nil {
		t.Fatal(err)
	}
	var cfg config.Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != c.dataDir {
		t.Fatalf("config data_dir %q, want %q", cfg.DataDir, c.dataDir)
	}

	// A rendezvous peer (LOC-76) needs no --root.
	rv := &cli{t: t, binary: c.binary, config: filepath.Join(dir, "etc", "locus-rv", "config.json"), dataDir: filepath.Join(dir, "rv-data"), home: c.home}
	code, stdout, stderr := rv.run("init", "--system", "--rendezvous", "--user", me.Username)
	if code != 0 || !strings.Contains(stdout, "rendezvous") || !strings.Contains(stdout, "allow UDP") {
		t.Fatalf("init --system --rendezvous: exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
	if rcfg, err := config.Load(rv.config); err != nil || !rcfg.Rendezvous || len(rcfg.Roots) != 0 {
		t.Fatalf("rendezvous system config: %+v, %v", rcfg, err)
	}

	// The human output carries the full peer id, not the short form.
	if err := os.Remove(c.config); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = c.run("init", "--system", "--user", me.Username, "--root", root)
	if code != 0 || !strings.Contains(stdout, out["peer_id"].(string)) {
		t.Fatalf("init --system exit %d, stdout %q, stderr %q; want the full peer id %v", code, stdout, stderr, out["peer_id"])
	}
}

// The configuration template `make install-system` ships must load as is.
func TestContribConfigTemplateLoads(t *testing.T) {
	cfg, err := config.Load(filepath.Join("..", "contrib", "config.system.json"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "/var/lib/locus" || len(cfg.Roots) != 1 {
		t.Fatalf("template loaded as data_dir %q, roots %v", cfg.DataDir, cfg.Roots)
	}
}

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultValid(t *testing.T) {
	c := Default()
	c.Roots = PathRoots(t.TempDir())
	if err := c.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	if c.Embedding.Device != "auto" {
		t.Fatalf("embedding.device = %q, want auto", c.Embedding.Device)
	}
}

func TestLoadMissingFileGivesDefaults(t *testing.T) {
	c, err := Load(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Peer.Port != DefaultPeerPort || c.API.Port != DefaultAPIPort {
		t.Fatalf("ports not defaulted: %+v", c)
	}
}

func TestLoadRoundTripAndDefaults(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "docs")
	cfg := Default()
	cfg.Roots = PathRoots(root)
	cfg.Sync.Interval = Duration(3 * time.Second)
	cfg.Workers = 7
	cfg.Embedding.Device = "cpu"
	path := filepath.Join(dir, "config.json")
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"roots"`) {
		t.Fatalf("saved config missing roots:\n%s", raw)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Workers != 7 || got.Sync.Interval.D() != 3*time.Second || got.Embedding.Device != "cpu" {
		t.Fatalf("round trip lost values: %+v", got)
	}
	if got.Discovery.Port != DefaultDiscoveryPort {
		t.Fatalf("unset field not defaulted: %+v", got.Discovery)
	}
}

func TestValidateRejects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no roots", func(c *Config) { c.Roots = nil }, "at least one sync root"},
		{"root is /", func(c *Config) { c.Roots = PathRoots("/") }, "resolves to /"},
		{"duplicate roots", func(c *Config) { c.Roots = PathRoots("/tmp/a", "/tmp/a/") }, "duplicate root"},
		{"roots share a basename", func(c *Config) { c.Roots = PathRoots("/tmp/work/Notes", "/tmp/home/Notes") }, "share the name"},
		{"roots share a configured name", func(c *Config) {
			c.Roots = []Root{{Path: "/tmp/a", Name: "docs"}, {Path: "/tmp/b", Name: "docs"}}
		}, "share the name"},
		{"configured name collides with a basename", func(c *Config) {
			c.Roots = []Root{{Path: "/tmp/docs"}, {Path: "/tmp/b", Name: "docs"}}
		}, "share the name"},
		{"name with a separator", func(c *Config) { c.Roots = []Root{{Path: "/tmp/a", Name: "x/y"}} }, "path separator"},
		{"empty root path", func(c *Config) { c.Roots = []Root{{Name: "docs"}} }, "empty path"},
		{"bad device", func(c *Config) { c.Embedding.Device = "tpu" }, "embedding.device"},
		{"bad port", func(c *Config) { c.Peer.Port = 99999 }, "peer.port"},
		{"nested root", func(c *Config) { c.Roots = PathRoots("/tmp/a", "/tmp/a/docs") }, "nested inside"},
		{"nested root listed first", func(c *Config) { c.Roots = PathRoots("/tmp/a/docs/x", "/tmp/a") }, "nested inside"},
		{"negative sync interval", func(c *Config) { c.Sync.Interval = Duration(-5 * time.Second) }, "sync.interval"},
		{"zero sync interval", func(c *Config) { c.Sync.Interval = 0 }, "sync.interval"},
		{"negative reconcile interval", func(c *Config) { c.ReconcileInterval = Duration(-time.Second) }, "reconcile_interval"},
		{"negative discovery interval", func(c *Config) { c.Discovery.Interval = Duration(-time.Second) }, "discovery.interval"},
		{"zero workers", func(c *Config) { c.Workers = 0 }, "workers"},
		{"zero upload limit", func(c *Config) { c.UploadMaxBytes = 0 }, "upload_max_bytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			c.Roots = PathRoots("/tmp/locus-a")
			tc.mutate(c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

// Roots that merely share a name prefix are siblings, not nested.
func TestValidateAcceptsSiblingRoots(t *testing.T) {
	c := Default()
	c.Roots = PathRoots("/tmp/a", "/tmp/ab", "/tmp/b/c")
	if err := c.Validate(); err != nil {
		t.Fatalf("sibling roots rejected: %v", err)
	}
}

// A negative interval used to pass Load and panic time.NewTicker in Serve.
func TestLoadRejectsNegativeInterval(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	raw := `{"roots": ["` + filepath.Join(dir, "docs") + `"], "sync": {"interval": "-5s"}}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "sync.interval") {
		t.Fatalf("Load() error = %v, want sync.interval rejection", err)
	}
}

func TestApplyDefaultsFillsZeroConfig(t *testing.T) {
	c := &Config{Roots: PathRoots("/tmp/locus-a")}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		t.Fatalf("zero config with defaults applied is invalid: %v", err)
	}
	d := Default()
	if c.UploadMaxBytes != d.UploadMaxBytes || c.Workers != d.Workers || c.Sync.Interval != d.Sync.Interval {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestDurationJSON(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"45s"`), &d); err != nil {
		t.Fatal(err)
	}
	if d.D() != 45*time.Second {
		t.Fatalf("parsed %v", d.D())
	}
	if err := json.Unmarshal([]byte(`"nonsense"`), &d); err == nil {
		t.Fatal("expected error for invalid duration")
	}
	b, err := json.Marshal(Duration(90 * time.Second))
	if err != nil || string(b) != `"1m30s"` {
		t.Fatalf("marshal = %s, %v", b, err)
	}
}

func TestResolvedRootsAreAbsolute(t *testing.T) {
	dir := t.TempDir()
	cwd, _ := os.Getwd()
	defer func() { _ = os.Chdir(cwd) }()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	c := Default()
	c.Roots = PathRoots("relative/docs")
	got, err := c.ResolvedRoots()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(got[0]) {
		t.Fatalf("root not absolute: %v", got)
	}
}

func TestResolveDataDirOverrides(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LOCUS_DATA_DIR", dir)
	c := Default()
	c.Roots = PathRoots("/tmp/whatever")
	got, err := c.ResolveDataDir()
	if err != nil {
		t.Fatal(err)
	}
	if got != dir {
		t.Fatalf("env override ignored: %s != %s", got, dir)
	}
	c.DataDir = filepath.Join(dir, "explicit")
	got, _ = c.ResolveDataDir()
	if got != filepath.Join(dir, "explicit") {
		t.Fatalf("config override ignored: %s", got)
	}
}

// Bare-string roots (every config written before named roots) still parse,
// next to the object form, and keep their basename as the name.
func TestRootsParseBareStringsAndObjects(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	docs := filepath.Join(dir, "Documents")
	work := filepath.Join(dir, "Sync", "Work")
	raw := `{"roots": ["` + docs + `", {"path": "` + work + `", "name": "work"}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []Root{{Path: docs}, {Path: work, Name: "work"}}
	if len(c.Roots) != 2 || c.Roots[0] != want[0] || c.Roots[1] != want[1] {
		t.Fatalf("roots = %+v, want %+v", c.Roots, want)
	}
	names, err := c.RootNames()
	if err != nil {
		t.Fatal(err)
	}
	if names[docs] != "Documents" || names[work] != "work" {
		t.Fatalf("root names = %v", names)
	}

	// Saving keeps an unnamed root a bare string, so older builds can still
	// read the file, and the named one an object.
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var shape struct {
		Roots []json.RawMessage `json:"roots"`
	}
	if err := json.Unmarshal(saved, &shape); err != nil {
		t.Fatal(err)
	}
	if len(shape.Roots) != 2 || shape.Roots[0][0] != '"' || shape.Roots[1][0] != '{' {
		t.Fatalf("saved roots have the wrong shape:\n%s", saved)
	}
}

func TestRootRejectsOtherJSON(t *testing.T) {
	var r Root
	if err := json.Unmarshal([]byte(`42`), &r); err == nil {
		t.Fatal("a number parsed as a root")
	}
}

func TestParseRoot(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Root
	}{
		{"/home/me/Documents", Root{Path: "/home/me/Documents"}},
		{"docs=/home/me/Documents", Root{Name: "docs", Path: "/home/me/Documents"}},
		{"docs=relative/dir", Root{Name: "docs", Path: "relative/dir"}},
		{"/home/me/a=b", Root{Path: "/home/me/a=b"}},
	} {
		if got := ParseRoot(tc.in); got != tc.want {
			t.Errorf("ParseRoot(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
	}
}

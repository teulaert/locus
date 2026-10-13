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
		{"negative tombstone age", func(c *Config) { c.TombstoneAge = Duration(-time.Hour) }, "tombstone_age"},
		{"negative discovery interval", func(c *Config) { c.Discovery.Interval = Duration(-time.Second) }, "discovery.interval"},
		{"zero workers", func(c *Config) { c.Workers = 0 }, "workers"},
		{"zero upload limit", func(c *Config) { c.UploadMaxBytes = 0 }, "upload_max_bytes"},
		{"negative audit entries", func(c *Config) { c.Audit.MaxEntries = -1 }, "audit.max_entries"},
		{"negative audit age", func(c *Config) { c.Audit.MaxAge = Duration(-time.Hour) }, "audit.max_age"},
		{"zero audit interval", func(c *Config) { c.Audit.Interval = 0 }, "audit.interval"},
		{"rendezvous with roots", func(c *Config) { c.Rendezvous = true }, "rendezvous peer has no roots"},
		{"unreadable memory limit", func(c *Config) { c.MemoryLimit = "lots" }, "memory_limit"},
		{"memory limit over 100%", func(c *Config) { c.MemoryLimit = "150%" }, "memory_limit"},
		{"tiny memory limit", func(c *Config) { c.MemoryLimit = "16MiB" }, "memory_limit"},
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
// memory_limit is optional and accepts "off", a percentage or a size
// (LOC-84).
func TestMemoryLimitSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"roots": ["/tmp/locus-a"], "memory_limit": "1GiB"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil || c.MemoryLimit != "1GiB" {
		t.Fatalf("Load = %+v, %v; want memory_limit 1GiB", c, err)
	}
	for _, v := range []string{"", "off", "25%", "512MiB", "2G"} {
		c := Default()
		c.Roots = PathRoots("/tmp/locus-a")
		c.MemoryLimit = v
		if err := c.Validate(); err != nil {
			t.Errorf("memory_limit %q: %v", v, err)
		}
	}
}

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
	if c.UploadMaxBytes != d.UploadMaxBytes || c.Workers != d.Workers || c.Sync.Interval != d.Sync.Interval || c.Audit != d.Audit || c.TombstoneAge != d.TombstoneAge {
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

func TestRootSyncHidden(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	plain := filepath.Join(dir, "Documents")
	dots := filepath.Join(dir, "Dotfiles")
	unnamed := filepath.Join(dir, "Unnamed")
	raw := `{"roots": ["` + plain + `", {"path": "` + dots + `", "name": "dots", "sync_hidden": true}, {"path": "` + unnamed + `", "sync_hidden": true}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Roots[0].SyncHidden || !c.Roots[1].SyncHidden || !c.Roots[2].SyncHidden {
		t.Fatalf("sync_hidden not parsed: %+v", c.Roots)
	}
	hidden, err := c.HiddenRoots()
	if err != nil {
		t.Fatal(err)
	}
	if len(hidden) != 2 || !hidden[dots] || !hidden[unnamed] {
		t.Fatalf("HiddenRoots = %v, want %s and %s", hidden, dots, unnamed)
	}

	// An unnamed root that syncs hidden paths cannot be a bare string:
	// the setting must survive a save.
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	again, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range c.Roots {
		if again.Roots[i] != c.Roots[i] {
			t.Fatalf("root %d after save = %+v, want %+v", i, again.Roots[i], c.Roots[i])
		}
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
	if shape.Roots[0][0] != '"' || shape.Roots[2][0] != '{' {
		t.Fatalf("saved roots have the wrong shape:\n%s", saved)
	}

	// The command-line form keeps the default.
	if ParseRoot("dots=" + dots).SyncHidden {
		t.Fatal("ParseRoot enabled sync_hidden")
	}
}

func TestValidatePeerInterface(t *testing.T) {
	for _, c := range []struct {
		addr, iface string
		ok          bool
	}{
		{"0.0.0.0", "wg0", true},
		{"0.0.0.0", "utun4", true},
		{"0.0.0.0", "10.42.0.0/16", true},
		{"0.0.0.0", "fd00::/64", true},
		{"0.0.0.0", "10.42.0.0/99", false},
		{"10.42.0.3", "wg0", false}, // a specific peer.addr is what is announced
		{"10.42.0.3", "", true},
	} {
		cfg := Default()
		cfg.Roots = PathRoots(t.TempDir())
		cfg.Peer.Addr, cfg.Peer.Interface = c.addr, c.iface
		if err := cfg.Validate(); (err == nil) != c.ok {
			t.Errorf("addr %q interface %q: err = %v, want ok=%v", c.addr, c.iface, err, c.ok)
		}
	}
}

// LOC-76: a rendezvous peer is configured with no roots at all, survives a
// save and load, and probes gateways unless told not to.
func TestRendezvousConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"rendezvous": true, "data_dir": "`+filepath.Join(dir, "data")+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load(rendezvous, no roots) = %v", err)
	}
	if !c.Rendezvous || len(c.Roots) != 0 {
		t.Fatalf("loaded %+v", c)
	}
	if !c.GatewayDiscovery() {
		t.Fatal("gateway discovery is off by default")
	}
	off := false
	c.Discovery.Gateways = &off
	if err := c.Save(path); err != nil {
		t.Fatal(err)
	}
	c2, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c2.GatewayDiscovery() || !c2.Rendezvous {
		t.Fatalf("round trip lost discovery.gateways=false or rendezvous: %+v", c2)
	}
}

// The update block (LOC-86): off by default, channel main every 30m, and
// nothing that would poll GitHub faster than once a minute.
func TestUpdateConfig(t *testing.T) {
	d := Default()
	if d.Update.Auto || d.Update.Channel != "main" || d.Update.Interval.D() != 30*time.Minute || d.Update.URL != "" {
		t.Fatalf("default update config %+v", d.Update)
	}
	sparse := &Config{Roots: PathRoots(t.TempDir())}
	sparse.ApplyDefaults()
	if err := sparse.Validate(); err != nil || sparse.Update.Channel != "main" {
		t.Fatalf("sparse config: %v, %+v", err, sparse.Update)
	}
	for name, tc := range map[string]struct {
		mutate func(*Update)
		ok     bool
	}{
		"stable":                  {func(u *Update) { u.Channel = "stable" }, true},
		"unknown channel":         {func(u *Update) { u.Channel = "nightly" }, false},
		"too frequent for GitHub": {func(u *Update) { u.Interval = Duration(30 * time.Second) }, false},
		"fast mirror":             {func(u *Update) { u.Interval = Duration(time.Second); u.URL = "http://127.0.0.1:9000" }, true},
		"negative interval":       {func(u *Update) { u.Interval = Duration(-time.Minute) }, false},
		"url without scheme":      {func(u *Update) { u.URL = "api.example.com" }, false},
		"ftp url":                 {func(u *Update) { u.URL = "ftp://mirror.example.com" }, false},
		"https mirror":            {func(u *Update) { u.URL = "https://mirror.example.com/api" }, true},
	} {
		c := Default()
		c.Roots = PathRoots(t.TempDir())
		tc.mutate(&c.Update)
		if err := c.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: Validate = %v, want ok %v", name, err, tc.ok)
		}
	}
}

// Package config loads Locus configuration: sync roots, ports, worker
// settings and embedding device preference.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	DefaultPeerPort       = 8909
	DefaultAPIPort        = 8910
	DefaultDiscoveryPort  = 8911
	DefaultReconcileEvery = 30 * time.Second
	DefaultSyncEvery      = 10 * time.Second
	DefaultUploadMax      = 1 << 30
)

// Config is the on-disk configuration for one Locus peer.
type Config struct {
	// Roots are the synchronized filesystem roots.
	Roots []string `json:"roots"`
	// DataDir overrides the application data directory.
	DataDir string `json:"data_dir,omitempty"`

	Peer struct {
		Addr string `json:"addr,omitempty"`
		Port int    `json:"port,omitempty"`
	} `json:"peer"`
	API struct {
		Addr  string `json:"addr,omitempty"`
		Port  int    `json:"port,omitempty"`
		Token string `json:"token,omitempty"`
	} `json:"api"`
	Discovery struct {
		// Peers are explicit host:port discovery targets. Required when
		// automatic WireGuard interface detection is unavailable.
		Peers    []string `json:"peers,omitempty"`
		Port     int      `json:"port,omitempty"`
		Interval Duration `json:"interval,omitempty"`
	} `json:"discovery"`

	Sync struct {
		Interval Duration `json:"interval,omitempty"`
	} `json:"sync"`
	ReconcileInterval Duration `json:"reconcile_interval,omitempty"`
	Workers           int      `json:"workers,omitempty"`

	Embedding struct {
		// Device is auto, cpu or gpu.
		Device string `json:"device,omitempty"`
	} `json:"embedding"`

	UploadMaxBytes int64 `json:"upload_max_bytes,omitempty"`

	// AuthorizedPeers optionally restricts which peer IDs may sync or use
	// the API. Empty means any authenticated Locus peer this one has
	// discovered (that is in its presence table).
	AuthorizedPeers []string `json:"authorized_peers,omitempty"`
}

// Duration is a JSON-friendly time.Duration accepting "30s" style strings.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Default returns the configuration used when no file exists.
func Default() *Config {
	c := &Config{}
	c.Peer.Addr = "0.0.0.0"
	c.Peer.Port = DefaultPeerPort
	c.API.Addr = "127.0.0.1"
	c.API.Port = DefaultAPIPort
	c.Discovery.Port = DefaultDiscoveryPort
	c.Discovery.Interval = Duration(10 * time.Second)
	c.Sync.Interval = Duration(DefaultSyncEvery)
	c.ReconcileInterval = Duration(DefaultReconcileEvery)
	c.Workers = 2
	c.Embedding.Device = "auto"
	c.UploadMaxBytes = DefaultUploadMax
	return c
}

// Dir returns the default configuration directory for the platform.
func Dir() (string, error) {
	if d := os.Getenv("LOCUS_CONFIG_DIR"); d != "" {
		return d, nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "locus"), nil
}

// Path returns the default configuration file path.
func Path() (string, error) {
	d, err := Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "config.json"), nil
}

// Load reads a configuration file, applying defaults to unset fields.
func Load(path string) (*Config, error) {
	cfg := Default()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	} else if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ApplyDefaults fills unset (zero) fields with their defaults, so a Config
// built in code behaves like one loaded from a sparse file. Negative values
// are left alone for Validate to reject, except Workers and UploadMaxBytes,
// where any non-positive value means "use the default".
func (c *Config) ApplyDefaults() {
	d := Default()
	if c.Peer.Addr == "" {
		c.Peer.Addr = d.Peer.Addr
	}
	if c.Peer.Port == 0 {
		c.Peer.Port = d.Peer.Port
	}
	if c.API.Addr == "" {
		c.API.Addr = d.API.Addr
	}
	if c.API.Port == 0 {
		c.API.Port = d.API.Port
	}
	if c.Discovery.Port == 0 {
		c.Discovery.Port = d.Discovery.Port
	}
	if c.Discovery.Interval == 0 {
		c.Discovery.Interval = d.Discovery.Interval
	}
	if c.Sync.Interval == 0 {
		c.Sync.Interval = d.Sync.Interval
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = d.ReconcileInterval
	}
	if c.Workers <= 0 {
		c.Workers = d.Workers
	}
	if c.Embedding.Device == "" {
		c.Embedding.Device = "auto"
	}
	if c.UploadMaxBytes <= 0 {
		c.UploadMaxBytes = d.UploadMaxBytes
	}
}

// Validate rejects nonsensical configuration with readable errors.
func (c *Config) Validate() error {
	if len(c.Roots) == 0 {
		return errors.New("config: at least one sync root is required")
	}
	roots, err := c.ResolvedRoots()
	if err != nil {
		return fmt.Errorf("config: invalid root: %w", err)
	}
	if err := ValidateRoots(roots); err != nil {
		return err
	}
	switch c.Embedding.Device {
	case "auto", "cpu", "gpu":
	default:
		return fmt.Errorf("config: embedding.device must be auto, cpu or gpu (got %q)", c.Embedding.Device)
	}
	for _, p := range []struct {
		name string
		v    int
	}{
		{"peer.port", c.Peer.Port},
		{"api.port", c.API.Port},
		{"discovery.port", c.Discovery.Port},
	} {
		if p.v < 1 || p.v > 65535 {
			return fmt.Errorf("config: %s out of range: %d", p.name, p.v)
		}
	}
	for _, d := range []struct {
		name string
		v    Duration
	}{
		{"sync.interval", c.Sync.Interval},
		{"reconcile_interval", c.ReconcileInterval},
		{"discovery.interval", c.Discovery.Interval},
	} {
		if d.v <= 0 {
			return fmt.Errorf("config: %s must be positive (got %s)", d.name, d.v.D())
		}
	}
	if c.Workers < 1 {
		return fmt.Errorf("config: workers must be at least 1 (got %d)", c.Workers)
	}
	if c.UploadMaxBytes <= 0 {
		return fmt.Errorf("config: upload_max_bytes must be positive (got %d)", c.UploadMaxBytes)
	}
	return nil
}

// ValidateRoots checks a list of resolved (absolute, clean) root paths: none
// may be /, appear twice, or lie inside another, since a file under nested
// roots would be a document of both.
func ValidateRoots(roots []string) error {
	for i, r := range roots {
		if r == string(filepath.Separator) {
			return fmt.Errorf("config: root %q resolves to /", r)
		}
		for _, prev := range roots[:i] {
			switch {
			case r == prev:
				return fmt.Errorf("config: duplicate root %q", r)
			case within(r, prev):
				return fmt.Errorf("config: root %q is nested inside root %q", r, prev)
			case within(prev, r):
				return fmt.Errorf("config: root %q is nested inside root %q", prev, r)
			}
		}
	}
	return nil
}

// within reports whether path lies strictly below dir.
func within(path, dir string) bool {
	return strings.HasPrefix(path, strings.TrimSuffix(dir, string(filepath.Separator))+string(filepath.Separator))
}

// ResolvedRoots returns absolute, clean root paths.
func (c *Config) ResolvedRoots() ([]string, error) {
	out := make([]string, 0, len(c.Roots))
	for _, r := range c.Roots {
		abs, err := filepath.Abs(r)
		if err != nil {
			return nil, err
		}
		out = append(out, filepath.Clean(abs))
	}
	return out, nil
}

// ResolveDataDir returns the application data directory for this peer.
func (c *Config) ResolveDataDir() (string, error) {
	if c.DataDir != "" {
		return filepath.Abs(c.DataDir)
	}
	if d := os.Getenv("LOCUS_DATA_DIR"); d != "" {
		return filepath.Abs(d)
	}
	base, err := dataHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "locus"), nil
}

func dataHome() (string, error) {
	if runtime.GOOS == "darwin" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, "Library", "Application Support"), nil
	}
	if d := os.Getenv("XDG_DATA_HOME"); d != "" {
		return d, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".local", "share"), nil
}

// Save writes the configuration to path, creating directories.
func (c *Config) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

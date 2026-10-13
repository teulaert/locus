// Package release is the CI side of auto-update (LOC-86): it generates the
// release signing key, writes a release's manifest.json and SHA256SUMS,
// signs the manifest and verifies a release directory the way an updating
// peer does. cmd/locus-release exposes it; the locus binary never links
// the signing path.
package release

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/teulaert/locus/internal/update"
)

// Environment variables the signing key is read from, in this order.
const (
	// EnvKeyFile names a file holding the base64 private key (on the
	// self-hosted release runner: /etc/locus-release/key).
	EnvKeyFile = "LOCUS_RELEASE_KEY_FILE"
	// EnvKey holds the base64 private key itself (a repository secret).
	EnvKey = "LOCUS_RELEASE_KEY"
)

// GenerateKey creates a signing key pair: the public key as hex (for
// internal/update.ReleasePublicKey) and the private key as base64 of its 64
// bytes (for the key file or the secret).
func GenerateKey() (pubHex, privB64 string, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", err
	}
	return hex.EncodeToString(pub), base64.StdEncoding.EncodeToString(priv), nil
}

// ParsePrivateKey decodes a base64 private key: the 64-byte form
// GenerateKey prints, or a 32-byte seed.
func ParsePrivateKey(s string) (ed25519.PrivateKey, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("release key is not base64: %w", err)
	}
	switch len(b) {
	case ed25519.PrivateKeySize:
		priv := ed25519.PrivateKey(b)
		// The public half is stored in the key; a key whose halves
		// disagree would sign manifests nothing verifies.
		if !ed25519.NewKeyFromSeed(priv.Seed()).Equal(priv) {
			return nil, errors.New("release key: public half does not match the seed")
		}
		return priv, nil
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(b), nil
	}
	return nil, fmt.Errorf("release key: %d bytes, want %d (or a %d-byte seed)", len(b), ed25519.PrivateKeySize, ed25519.SeedSize)
}

// LoadPrivateKey reads the signing key: from the file named by
// EnvKeyFile when that file exists, else from EnvKey. It says where the key
// came from. A key file others can read is refused, as ssh does.
func LoadPrivateKey(getenv func(string) string) (ed25519.PrivateKey, string, error) {
	if path := getenv(EnvKeyFile); path != "" {
		fi, err := os.Stat(path)
		switch {
		case err == nil:
			if runtime.GOOS != "windows" && fi.Mode().Perm()&0o077 != 0 {
				return nil, "", fmt.Errorf("release key file %s is accessible by others (mode %v); chmod 600 it", path, fi.Mode().Perm())
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return nil, "", err
			}
			k, err := ParsePrivateKey(string(b))
			return k, path, err
		case !errors.Is(err, fs.ErrNotExist):
			return nil, "", err
		}
	}
	if s := getenv(EnvKey); strings.TrimSpace(s) != "" {
		k, err := ParsePrivateKey(s)
		return k, EnvKey, err
	}
	return nil, "", fmt.Errorf("no release signing key: set %s to a key file or %s to the base64 key", EnvKeyFile, EnvKey)
}

// Sign returns the manifest.sig contents for the exact bytes of manifest.
func Sign(priv ed25519.PrivateKey, manifest []byte) []byte {
	return update.EncodeSignature(ed25519.Sign(priv, manifest))
}

// Meta is what a manifest says about the build besides its artifacts.
type Meta struct {
	Version    string
	Commit     string
	CommitTime time.Time
	Created    time.Time
	Channel    string
}

// BuildManifest describes every binary in dir named locus-<os>-<arch> and
// returns the manifest and the bytes to publish (and sign).
func BuildManifest(dir string, meta Meta) (*update.Manifest, []byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	m := &update.Manifest{
		Version:    meta.Version,
		Commit:     meta.Commit,
		CommitTime: meta.CommitTime.UTC(),
		Created:    meta.Created.UTC(),
		Channel:    meta.Channel,
		Artifacts:  []update.Artifact{},
	}
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() || !strings.HasPrefix(name, "locus-") {
			continue
		}
		osArch := strings.SplitN(strings.TrimPrefix(name, "locus-"), "-", 2)
		if len(osArch) != 2 || osArch[0] == "" || osArch[1] == "" || strings.Contains(osArch[1], ".") {
			continue
		}
		size, sum, err := hashFile(filepath.Join(dir, name))
		if err != nil {
			return nil, nil, err
		}
		m.Artifacts = append(m.Artifacts, update.Artifact{OS: osArch[0], Arch: osArch[1], File: name, Size: size, SHA256: sum})
	}
	sort.Slice(m.Artifacts, func(i, j int) bool { return m.Artifacts[i].File < m.Artifacts[j].File })
	if err := m.Validate(); err != nil {
		return nil, nil, err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	return m, append(b, '\n'), nil
}

// Sums renders SHA256SUMS for the manifest's artifacts, in the format
// sha256sum -c reads.
func Sums(m *update.Manifest) []byte {
	var b strings.Builder
	for _, a := range m.Artifacts {
		fmt.Fprintf(&b, "%s  %s\n", a.SHA256, a.File)
	}
	return []byte(b.String())
}

// VerifyDir verifies a release directory as an updating peer would: the
// signature over manifest.json with pub, then every artifact's size and
// SHA-256.
func VerifyDir(pub ed25519.PublicKey, dir string) (*update.Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, update.ManifestFile))
	if err != nil {
		return nil, err
	}
	sig, err := os.ReadFile(filepath.Join(dir, update.SignatureFile))
	if err != nil {
		return nil, err
	}
	m, err := update.VerifyManifest(pub, data, sig)
	if err != nil {
		return nil, err
	}
	for _, a := range m.Artifacts {
		size, sum, err := hashFile(filepath.Join(dir, a.File))
		if err != nil {
			return nil, err
		}
		if size != a.Size || sum != a.SHA256 {
			return nil, fmt.Errorf("%s does not match the manifest (size %d, sha256 %s)", a.File, size, sum)
		}
	}
	return m, nil
}

func hashFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

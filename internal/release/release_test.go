package release

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/update"
)

func writeDist(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{
		"locus-linux-amd64":  "linux amd64 binary",
		"locus-linux-arm64":  "linux arm64 binary",
		"locus-darwin-arm64": "darwin arm64 binary",
		"locus-darwin-amd64": "darwin amd64 binary",
		"notes.txt":          "not an artifact",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

var meta = Meta{Version: "e8c67fb", Commit: "e8c67fb0123456789abcdef0123456789abcdef0", CommitTime: time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC), Created: time.Now(), Channel: update.ChannelMain}

// What CI publishes verifies the way a peer verifies it: keygen, manifest,
// sign, verify.
func TestKeygenManifestSignVerify(t *testing.T) {
	pubHex, privB64, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := update.ParsePublicKey(pubHex)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := ParsePrivateKey(privB64)
	if err != nil {
		t.Fatal(err)
	}
	dir := writeDist(t)
	m, data, err := BuildManifest(dir, meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Artifacts) != 4 {
		t.Fatalf("artifacts %+v", m.Artifacts)
	}
	if a, ok := m.ArtifactFor("darwin", "arm64"); !ok || a.File != "locus-darwin-arm64" || a.Size != int64(len("darwin arm64 binary")) {
		t.Fatalf("darwin/arm64 artifact %+v", a)
	}
	if err := os.WriteFile(filepath.Join(dir, update.ManifestFile), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, update.SignatureFile), Sign(priv, data), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := VerifyDir(pub, dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Commit != meta.Commit || got.Channel != update.ChannelMain || !got.CommitTime.Equal(meta.CommitTime) {
		t.Fatalf("verified manifest %+v", got)
	}
	sums := string(Sums(m))
	if !strings.Contains(sums, m.Artifacts[0].SHA256+"  "+m.Artifacts[0].File+"\n") || strings.Count(sums, "\n") != 4 {
		t.Fatalf("SHA256SUMS:\n%s", sums)
	}

	// A binary changed after signing fails verification.
	if err := os.WriteFile(filepath.Join(dir, "locus-linux-arm64"), []byte("linux arm64 binarY"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyDir(pub, dir); err == nil {
		t.Fatal("tampered artifact verified")
	}
	// Another key does not verify the signature.
	otherHex, _, _ := GenerateKey()
	other, _ := update.ParsePublicKey(otherHex)
	if _, err := VerifyDir(other, dir); !errors.Is(err, update.ErrBadSignature) {
		t.Fatalf("other key: %v", err)
	}
}

func TestBuildManifestRejectsBadMeta(t *testing.T) {
	dir := writeDist(t)
	bad := meta
	bad.Channel = "nightly"
	if _, _, err := BuildManifest(dir, bad); !errors.Is(err, update.ErrBadManifest) {
		t.Fatalf("unknown channel: %v", err)
	}
	if _, _, err := BuildManifest(t.TempDir(), meta); !errors.Is(err, update.ErrBadManifest) {
		t.Fatalf("no binaries: %v", err)
	}
}

func TestParsePrivateKey(t *testing.T) {
	_, privB64, _ := GenerateKey()
	priv, err := ParsePrivateKey(" " + privB64 + "\n")
	if err != nil {
		t.Fatal(err)
	}
	seed := base64.StdEncoding.EncodeToString(priv.Seed())
	if k, err := ParsePrivateKey(seed); err != nil || !k.Equal(priv) {
		t.Fatalf("seed form: %v", err)
	}
	// A key whose public half was swapped for another's.
	b, _ := base64.StdEncoding.DecodeString(privB64)
	_, other, _ := ed25519.GenerateKey(nil)
	copy(b[32:], other[32:])
	if _, err := ParsePrivateKey(base64.StdEncoding.EncodeToString(b)); err == nil {
		t.Fatal("inconsistent key accepted")
	}
	for _, s := range []string{"", "!!", base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, err := ParsePrivateKey(s); err == nil {
			t.Fatalf("key %q accepted", s)
		}
	}
}

// The key file comes first, then the secret; a key file others can read
// is refused.
func TestLoadPrivateKeySources(t *testing.T) {
	_, fileKey, _ := GenerateKey()
	_, envKey, _ := GenerateKey()
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, []byte(fileKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	k, src, err := LoadPrivateKey(env(map[string]string{EnvKeyFile: path, EnvKey: envKey}))
	want, _ := ParsePrivateKey(fileKey)
	if err != nil || src != path || !k.Equal(want) {
		t.Fatalf("file and secret: source %q, err %v", src, err)
	}
	k, src, err = LoadPrivateKey(env(map[string]string{EnvKeyFile: filepath.Join(t.TempDir(), "missing"), EnvKey: envKey}))
	want, _ = ParsePrivateKey(envKey)
	if err != nil || src != EnvKey || !k.Equal(want) {
		t.Fatalf("missing file, secret set: source %q, err %v", src, err)
	}
	if _, _, err := LoadPrivateKey(env(nil)); err == nil {
		t.Fatal("no key configured, yet one loaded")
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := LoadPrivateKey(env(map[string]string{EnvKeyFile: path, EnvKey: envKey})); err == nil || !strings.Contains(err.Error(), "chmod 600") {
			t.Fatalf("world-readable key file: %v", err)
		}
	}
}

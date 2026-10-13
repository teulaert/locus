package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/update"
)

func runTool(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, &out, &errb, func(k string) string { return env[k] })
	return code, out.String(), errb.String()
}

// The commands CI runs, end to end: keygen, manifest, sign (key from a
// file), verify.
func TestReleaseToolPipeline(t *testing.T) {
	code, out, _ := runTool(t, nil, "keygen")
	var pub, priv string
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) == 2 {
			switch f[0] {
			case "public":
				pub = f[1]
			case "private":
				priv = f[1]
			}
		}
	}
	if code != 0 || len(pub) != 64 || priv == "" {
		t.Fatalf("keygen: exit %d, output %q", code, out)
	}
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "key")
	if err := os.WriteFile(keyFile, []byte(priv+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dist := filepath.Join(dir, "dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"linux-amd64", "linux-arm64", "darwin-arm64", "darwin-amd64"} {
		if err := os.WriteFile(filepath.Join(dist, "locus-"+p), []byte("binary "+p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if code, _, stderr := runTool(t, nil, "manifest", "-dir", dist, "-version", "e8c67fb", "-commit", "e8c67fb0123456789abcdef0123456789abcdef0", "-commit-time", "2026-10-12T09:00:00+02:00", "-channel", "main"); code != 0 {
		t.Fatalf("manifest: exit %d: %s", code, stderr)
	}
	for _, f := range []string{update.ManifestFile, update.SumsFile} {
		if _, err := os.Stat(filepath.Join(dist, f)); err != nil {
			t.Fatalf("%s not written: %v", f, err)
		}
	}
	if code, _, stderr := runTool(t, nil, "sign", "-in", filepath.Join(dist, update.ManifestFile)); code != 1 || !strings.Contains(stderr, "LOCUS_RELEASE_KEY") {
		t.Fatalf("sign without a key: exit %d: %s", code, stderr)
	}
	code, out, stderr := runTool(t, map[string]string{"LOCUS_RELEASE_KEY_FILE": keyFile}, "sign", "-in", filepath.Join(dist, update.ManifestFile))
	if code != 0 || !strings.Contains(out, keyFile) {
		t.Fatalf("sign: exit %d: %s %s", code, out, stderr)
	}
	if code, out, stderr := runTool(t, nil, "verify", "-dir", dist, "-pubkey", pub); code != 0 || !strings.Contains(out, "4 artifacts") {
		t.Fatalf("verify: exit %d: %s %s", code, out, stderr)
	}
	// Without -pubkey it uses the key compiled into locus, which this
	// branch has not filled in, or which is not this test's key.
	if code, _, _ := runTool(t, nil, "verify", "-dir", dist); code == 0 {
		t.Fatal("verified with the compiled-in key, which did not sign this release")
	}
	if code, _, _ := runTool(t, nil, "bogus"); code != 2 {
		t.Fatalf("unknown command: exit %d", code)
	}
}

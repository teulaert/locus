package update

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func manifestBytes(t *testing.T, m Manifest) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func validManifest() Manifest {
	return Manifest{Version: "89abcde", Commit: newCommit, CommitTime: t0, Created: t0, Channel: ChannelMain,
		Artifacts: []Artifact{{OS: "linux", Arch: "amd64", File: "locus-linux-amd64", Size: 3, SHA256: sum([]byte("new"))}}}
}

func TestManifestSignatureRoundTrip(t *testing.T) {
	pub, priv := newKey(t)
	data := manifestBytes(t, validManifest())
	sig := EncodeSignature(ed25519.Sign(priv, data))
	m, err := VerifyManifest(pub, data, sig)
	if err != nil {
		t.Fatal(err)
	}
	if m.Commit != newCommit || m.Channel != ChannelMain || len(m.Artifacts) != 1 {
		t.Fatalf("verified manifest %+v", m)
	}

	t.Run("tampered manifest", func(t *testing.T) {
		bad := append([]byte{}, data...)
		bad[len(bad)/2] ^= 1
		if _, err := VerifyManifest(pub, bad, sig); !errors.Is(err, ErrBadSignature) {
			t.Fatalf("tampered manifest: %v, want ErrBadSignature", err)
		}
		// Same JSON meaning, other bytes: the signature covers the bytes.
		if _, err := VerifyManifest(pub, append(data, ' '), sig); !errors.Is(err, ErrBadSignature) {
			t.Fatalf("re-encoded manifest: %v, want ErrBadSignature", err)
		}
	})
	t.Run("wrong key", func(t *testing.T) {
		other, _ := newKey(t)
		if _, err := VerifyManifest(other, data, sig); !errors.Is(err, ErrBadSignature) {
			t.Fatalf("wrong key: %v, want ErrBadSignature", err)
		}
	})
	t.Run("garbage signature", func(t *testing.T) {
		for _, s := range []string{"", "not base64!", "AAAA"} {
			if _, err := VerifyManifest(pub, data, []byte(s)); !errors.Is(err, ErrBadSignature) {
				t.Fatalf("signature %q: %v, want ErrBadSignature", s, err)
			}
		}
	})
	t.Run("no key", func(t *testing.T) {
		if _, err := VerifyManifest(nil, data, sig); !errors.Is(err, ErrNoKey) {
			t.Fatalf("nil key: %v, want ErrNoKey", err)
		}
	})
}

// A build without a release key (the placeholder) trusts nothing.
func TestEmptyReleaseKeyIsRejected(t *testing.T) {
	if _, err := ParsePublicKey(""); !errors.Is(err, ErrNoKey) {
		t.Fatalf("empty key: %v, want ErrNoKey", err)
	}
	if _, err := ParsePublicKey("abcd"); err == nil {
		t.Fatal("short key accepted")
	}
	saved := trustedKeyHex
	t.Cleanup(func() { trustedKeyHex = saved })
	trustedKeyHex = ""
	if _, err := TrustedKey(); !errors.Is(err, ErrNoKey) {
		t.Fatalf("TrustedKey with the placeholder: %v, want ErrNoKey", err)
	}
	u := New(Options{DataDir: t.TempDir()})
	if u.Enabled() || u.Status().State != StateDisabled {
		t.Fatalf("updater without a key: enabled %v, state %q", u.Enabled(), u.Status().State)
	}
	if _, err := u.Manual(t.Context(), Request{Check: true}); !errors.Is(err, ErrNoKey) {
		t.Fatalf("check without a key: %v, want ErrNoKey", err)
	}
}

// The committed key is either the placeholder or a valid key.
func TestCommittedReleaseKeyParses(t *testing.T) {
	if ReleasePublicKey == "" {
		t.Skip("no release key committed yet")
	}
	if _, err := ParsePublicKey(ReleasePublicKey); err != nil {
		t.Fatal(err)
	}
}

func TestManifestValidation(t *testing.T) {
	_, priv := newKey(t)
	pub := priv.Public().(ed25519.PublicKey)
	for name, mutate := range map[string]func(*Manifest){
		"unknown channel":  func(m *Manifest) { m.Channel = "nightly" },
		"no artifacts":     func(m *Manifest) { m.Artifacts = nil },
		"path in file":     func(m *Manifest) { m.Artifacts[0].File = "../locus" },
		"bad hash":         func(m *Manifest) { m.Artifacts[0].SHA256 = "abc" },
		"zero size":        func(m *Manifest) { m.Artifacts[0].Size = 0 },
		"no commit time":   func(m *Manifest) { m.CommitTime = time.Time{} },
		"no commit":        func(m *Manifest) { m.Commit = "" },
		"missing platform": func(m *Manifest) { m.Artifacts[0].OS = "" },
	} {
		m := validManifest()
		mutate(&m)
		data := manifestBytes(t, m)
		if _, err := VerifyManifest(pub, data, EncodeSignature(ed25519.Sign(priv, data))); !errors.Is(err, ErrBadManifest) {
			t.Errorf("%s: %v, want ErrBadManifest", name, err)
		}
	}
}

func TestManifestNewer(t *testing.T) {
	m := validManifest() // newCommit at t0
	for _, tc := range []struct {
		name    string
		running Build
		want    bool
	}{
		{"older build", Build{Commit: oldCommit, Time: t0.Add(-time.Hour)}, true},
		{"same commit", Build{Commit: newCommit, Time: t0.Add(-time.Hour)}, false},
		{"same commit abbreviated", Build{Commit: newCommit[:7]}, false},
		{"newer build: no downgrade", Build{Commit: oldCommit, Time: t0.Add(time.Hour)}, false},
		{"same time, other commit", Build{Commit: oldCommit, Time: t0}, false},
		{"unknown build time", Build{Commit: oldCommit}, true},
	} {
		if got := m.Newer(tc.running); got != tc.want {
			t.Errorf("%s: Newer = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestArtifactForUnknownPlatform(t *testing.T) {
	m := validManifest()
	if _, ok := m.ArtifactFor("plan9", "386"); ok {
		t.Fatal("found an artifact for a platform the release does not have")
	}
	if a, ok := m.ArtifactFor("linux", "amd64"); !ok || a.File != "locus-linux-amd64" {
		t.Fatalf("linux/amd64: %+v %v", a, ok)
	}
}

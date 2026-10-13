package update

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Release channels: "main" follows every build of the main branch
// (prereleases tagged build-<sha>), "stable" only releases tagged v*.
const (
	ChannelMain   = "main"
	ChannelStable = "stable"
)

// ValidChannel reports whether c names a release channel.
func ValidChannel(c string) bool { return c == ChannelMain || c == ChannelStable }

// Names of a release's metadata assets.
const (
	ManifestFile  = "manifest.json"
	SignatureFile = "manifest.sig"
	SumsFile      = "SHA256SUMS"
)

// MaxManifestSize bounds what is downloaded as a manifest or signature.
const MaxManifestSize = 1 << 20

// Manifest describes one release: what was built, from which commit, for
// which channel, and the size and SHA-256 of every binary. manifest.sig is
// an ed25519 signature over the exact bytes of manifest.json, so nothing in
// it, the hashes included, can change without the signing key.
type Manifest struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	// CommitTime is the commit's committer time; it orders builds, so a
	// replayed older release is never taken for an upgrade.
	CommitTime time.Time  `json:"commit_time"`
	Created    time.Time  `json:"created"`
	Channel    string     `json:"channel"`
	Artifacts  []Artifact `json:"artifacts"`
}

// Artifact is one binary of a release.
type Artifact struct {
	OS     string `json:"os"`
	Arch   string `json:"arch"`
	File   string `json:"file"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// ArtifactName is the asset name of the binary for goos/goarch.
func ArtifactName(goos, goarch string) string { return "locus-" + goos + "-" + goarch }

// Errors of manifest verification.
var (
	ErrBadSignature = errors.New("release manifest signature does not verify with the compiled-in release key")
	ErrBadManifest  = errors.New("invalid release manifest")
)

// EncodeSignature renders a signature as manifest.sig holds it: standard
// base64 and a newline.
func EncodeSignature(sig []byte) []byte {
	return []byte(base64.StdEncoding.EncodeToString(sig) + "\n")
}

// DecodeSignature reads manifest.sig.
func DecodeSignature(b []byte) ([]byte, error) {
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("%w: signature is not base64: %v", ErrBadSignature, err)
	}
	if len(sig) != ed25519.SignatureSize {
		return nil, fmt.Errorf("%w: signature is %d bytes, want %d", ErrBadSignature, len(sig), ed25519.SignatureSize)
	}
	return sig, nil
}

// VerifyManifest checks sigFile against the exact bytes of data with pub,
// and only then parses and validates the manifest. A nil or malformed key
// verifies nothing.
func VerifyManifest(pub ed25519.PublicKey, data, sigFile []byte) (*Manifest, error) {
	if len(pub) != ed25519.PublicKeySize {
		return nil, ErrNoKey
	}
	sig, err := DecodeSignature(sigFile)
	if err != nil {
		return nil, err
	}
	if !ed25519.Verify(pub, data, sig) {
		return nil, ErrBadSignature
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadManifest, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// Validate rejects a manifest the updater could not act on safely.
func (m *Manifest) Validate() error {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrBadManifest, fmt.Sprintf(format, args...))
	}
	if m.Version == "" || len(m.Commit) < 7 {
		return bad("version and commit are required")
	}
	if m.CommitTime.IsZero() {
		return bad("commit_time is required")
	}
	if !ValidChannel(m.Channel) {
		return bad("unknown channel %q", m.Channel)
	}
	if len(m.Artifacts) == 0 {
		return bad("no artifacts")
	}
	for _, a := range m.Artifacts {
		if a.OS == "" || a.Arch == "" || a.File == "" || strings.ContainsAny(a.File, `/\`) || a.File == "." || a.File == ".." {
			return bad("artifact %q: os, arch and a plain file name are required", a.File)
		}
		if a.Size <= 0 {
			return bad("artifact %s: size must be positive", a.File)
		}
		if h, err := hex.DecodeString(a.SHA256); err != nil || len(h) != 32 {
			return bad("artifact %s: sha256 must be 64 hex digits", a.File)
		}
	}
	return nil
}

// ArtifactFor returns the binary built for goos/goarch, if the release has
// one.
func (m *Manifest) ArtifactFor(goos, goarch string) (*Artifact, bool) {
	for i := range m.Artifacts {
		if m.Artifacts[i].OS == goos && m.Artifacts[i].Arch == goarch {
			return &m.Artifacts[i], true
		}
	}
	return nil, false
}

// Newer reports whether the release m is an upgrade from the running build.
// The same commit is not; neither is a commit no newer than the running
// one's, so an old signed release served again is never installed. A build
// whose commit time is unknown (built outside a checkout) takes any release.
func (m *Manifest) Newer(running Build) bool {
	if sameCommit(m.Commit, running.Commit) {
		return false
	}
	if running.Time.IsZero() {
		return true
	}
	return m.CommitTime.After(running.Time)
}

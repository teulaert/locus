package update

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// trustedKeyHex is the key releases are verified with. It starts as the
// committed ReleasePublicKey; tests replace it with a key of their own (in
// this package, or with -ldflags -X for a binary they build).
var trustedKeyHex = ReleasePublicKey

// ErrNoKey is returned when no release key is compiled into this build.
var ErrNoKey = errors.New("no release key compiled in: this build cannot verify releases (install a CI-built release, or fill internal/update.ReleasePublicKey)")

// TrustedKey returns the public key releases must be signed with.
func TrustedKey() (ed25519.PublicKey, error) {
	return ParsePublicKey(trustedKeyHex)
}

// ParsePublicKey decodes a hex ed25519 public key. An empty string is
// ErrNoKey.
func ParsePublicKey(s string) (ed25519.PublicKey, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, ErrNoKey
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("release public key: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("release public key: %d bytes, want %d", len(b), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(b), nil
}

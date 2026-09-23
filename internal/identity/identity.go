// Package identity provides the persistent cryptographic identity every
// Locus installation generates on first launch.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Identity is a peer's stable application identity.
type Identity struct {
	PeerID    string `json:"peer_id"`
	PublicKey []byte `json:"public_key"`
	SecretKey []byte `json:"secret_key"`

	path string
}

// Load reads an identity from dir, generating one when absent.
func Load(dir string) (*Identity, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "identity.json")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		id, err := generate()
		if err != nil {
			return nil, err
		}
		id.path = path
		if err := id.save(); err != nil {
			return nil, err
		}
		return id, nil
	}
	if err != nil {
		return nil, err
	}
	var id Identity
	if err := json.Unmarshal(b, &id); err != nil {
		return nil, fmt.Errorf("parse identity: %w", err)
	}
	if len(id.SecretKey) != ed25519.PrivateKeySize || len(id.PublicKey) != ed25519.PublicKeySize {
		return nil, errors.New("identity: corrupt key material")
	}
	if PeerIDOf(ed25519.PrivateKey(id.SecretKey)) != id.PeerID {
		return nil, errors.New("identity: peer id does not match key")
	}
	id.path = path
	return &id, nil
}

func generate() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{PeerID: PeerIDOf(priv), PublicKey: []byte(pub), SecretKey: []byte(priv)}, nil
}

// PeerIDOf derives the stable peer ID (hex encoded public key).
func PeerIDOf(priv ed25519.PrivateKey) string {
	return hex.EncodeToString(priv.Public().(ed25519.PublicKey))
}

func (id *Identity) save() error {
	b, err := json.MarshalIndent(id, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(id.path, b, 0o600)
}

// Sign signs msg with the peer's private key.
func (id *Identity) Sign(msg []byte) []byte {
	return ed25519.Sign(ed25519.PrivateKey(id.SecretKey), msg)
}

// Public returns the public key.
func (id *Identity) Public() ed25519.PublicKey {
	return ed25519.PublicKey(id.PublicKey)
}

// Verify checks a signature against a peer ID (hex public key).
func Verify(peerID string, msg, sig []byte) bool {
	raw, err := hex.DecodeString(peerID)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(raw), msg, sig)
}

// Short returns the first 8 characters of a peer ID for display.
func Short(peerID string) string {
	if len(peerID) <= 8 {
		return peerID
	}
	return peerID[:8]
}

// Timestamp is included in signed handshakes to bind them in time.
func Timestamp() int64 { return time.Now().Unix() }

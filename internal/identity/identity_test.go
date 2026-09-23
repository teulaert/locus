package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGeneratesOnFirstLaunchAndPersists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	id1, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(id1.PeerID) != 64 {
		t.Fatalf("peer id = %q", id1.PeerID)
	}
	if _, err := os.Stat(filepath.Join(dir, "identity.json")); err != nil {
		t.Fatalf("identity not persisted: %v", err)
	}
	id2, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if id1.PeerID != id2.PeerID {
		t.Fatal("peer identity must be stable across launches")
	}
	if mode, _ := os.Stat(filepath.Join(dir, "identity.json")); mode != nil && mode.Mode().Perm()&0o077 != 0 {
		t.Fatalf("key material is group/other readable: %v", mode.Mode())
	}
}

func TestSignAndVerify(t *testing.T) {
	id, err := Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("handshake")
	sig := id.Sign(msg)
	if !Verify(id.PeerID, msg, sig) {
		t.Fatal("valid signature rejected")
	}
	if Verify(id.PeerID, []byte("tampered"), sig) {
		t.Fatal("tampered message accepted")
	}
	if Verify(otherID(t), msg, sig) {
		t.Fatal("signature accepted for a different peer id")
	}
	if Verify("deadbeef", msg, sig) {
		t.Fatal("malformed peer id accepted")
	}
}

func TestCorruptKeyMaterialRejected(t *testing.T) {
	dir := t.TempDir()
	id, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = id
	path := filepath.Join(dir, "identity.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append([]byte{}, b[:len(b)/2]...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("truncated identity accepted")
	}
}

func otherID(t *testing.T) string {
	t.Helper()
	other, err := Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return other.PeerID
}

func TestShort(t *testing.T) {
	if got := Short("0123456789"); got != "01234567" {
		t.Fatalf("Short = %q", got)
	}
	if got := Short("abc"); got != "abc" {
		t.Fatalf("Short of short id = %q", got)
	}
}

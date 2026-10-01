package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/vvector"
)

func testIdentity(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func otherPeerID(t *testing.T) string {
	t.Helper()
	other, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return other.PeerID
}

func signAt(id *identity.Identity, method, target string, ts int64, body []byte) (int64, string) {
	sum := sha256.Sum256(body)
	msg := CanonicalRequestMessage(method, target, ts, sum[:])
	return ts, hex.EncodeToString(id.Sign(msg))
}

func mapToHeader(m map[string]string) http.Header {
	h := http.Header{}
	for k, v := range m {
		h.Set(k, v)
	}
	return h
}

func itoa(i int64) string { return strconv.FormatInt(i, 10) }

func mustVer(s string) string {
	v, err := vvector.Parse(s)
	if err != nil {
		panic(err)
	}
	return v.String()
}

func alive(rel, hash, ver string) ManifestEntry {
	return ManifestEntry{RelPath: rel, Hash: hash, Version: mustVer(ver)}
}

func deleted(rel, hash, ver string) ManifestEntry {
	e := alive(rel, hash, ver)
	e.Deleted = true
	return e
}

func TestPlanOnlyLocal(t *testing.T) {
	local := []ManifestEntry{alive("new.txt", "h1", "a:1")}
	acts := Plan(local, nil)
	if len(acts) != 1 || acts[0].Kind != ActionPushCreate {
		t.Fatalf("actions = %+v", acts)
	}
}

func TestPlanOnlyRemote(t *testing.T) {
	remote := []ManifestEntry{alive("theirs.txt", "h1", "b:1")}
	acts := Plan(nil, remote)
	if len(acts) != 1 || acts[0].Kind != ActionPullCreate {
		t.Fatalf("actions = %+v", acts)
	}
}

func TestPlanIdenticalIsNoop(t *testing.T) {
	e := alive("same.txt", "h", "a:1,b:1")
	acts := Plan([]ManifestEntry{e}, []ManifestEntry{e})
	if len(acts) != 1 || acts[0].Kind != ActionNoop {
		t.Fatalf("actions = %+v", acts)
	}
}

func TestPlanDescendantUpdate(t *testing.T) {
	// Remote is causally newer: pull.
	acts := Plan(
		[]ManifestEntry{alive("f.txt", "h1", "a:1")},
		[]ManifestEntry{alive("f.txt", "h2", "a:2")},
	)
	if acts[0].Kind != ActionPullUpdate {
		t.Fatalf("expected pull of descendant, got %+v", acts[0])
	}
	// Local is newer: push.
	acts = Plan(
		[]ManifestEntry{alive("f.txt", "h2", "a:2")},
		[]ManifestEntry{alive("f.txt", "h1", "a:1")},
	)
	if acts[0].Kind != ActionPushUpdate {
		t.Fatalf("expected push of descendant, got %+v", acts[0])
	}
}

func TestPlanDivergencePushesForConflictResolution(t *testing.T) {
	acts := Plan(
		[]ManifestEntry{alive("f.txt", "localhash", "a:1,b:1,c:1")},
		[]ManifestEntry{alive("f.txt", "remotehash", "a:1,b:2")},
	)
	if acts[0].Kind != ActionPushUpdate {
		t.Fatalf("divergent edits must be pushed so both peers resolve identically, got %+v", acts[0])
	}
}

// Identical content with differing vectors (the state an incoming-wins
// conflict leaves behind on the pusher) must still reconcile the vectors, or
// the next ordinary edit would look divergent (LOC-6).
func TestPlanSameContentReconcilesVectors(t *testing.T) {
	tests := []struct {
		name       string
		local, rem string
		want       ActionKind
	}{
		{"remote dominates", "a:1", "a:1,b:2", ActionPullVersion},
		{"concurrent", "a:1,c:1", "a:1,b:2", ActionPullVersion},
		{"local dominates", "a:1,b:2", "a:1", ActionNoop},
		{"equal", "a:1,b:2", "a:1,b:2", ActionNoop},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acts := Plan(
				[]ManifestEntry{alive("f.txt", "same", tc.local)},
				[]ManifestEntry{alive("f.txt", "same", tc.rem)},
			)
			if acts[0].Kind != tc.want {
				t.Fatalf("got %s, want %s", acts[0].Kind, tc.want)
			}
		})
	}
}

func TestPlanDeletionPropagation(t *testing.T) {
	// Remote deleted an older version we have: pull delete.
	acts := Plan(
		[]ManifestEntry{alive("f.txt", "h", "a:1")},
		[]ManifestEntry{deleted("f.txt", "h", "a:2")},
	)
	if acts[0].Kind != ActionPullDelete {
		t.Fatalf("actions = %+v", acts[0])
	}
	// We deleted newer: push delete.
	acts = Plan(
		[]ManifestEntry{deleted("f.txt", "h", "a:2")},
		[]ManifestEntry{alive("f.txt", "h", "a:1")},
	)
	if acts[0].Kind != ActionPushDelete {
		t.Fatalf("actions = %+v", acts[0])
	}
	// Their delete is older than our local edit: our edit wins and is pushed.
	acts = Plan(
		[]ManifestEntry{alive("f.txt", "edited", "a:2")},
		[]ManifestEntry{deleted("f.txt", "h", "a:1")},
	)
	if acts[0].Kind != ActionPushUpdate {
		t.Fatalf("actions = %+v", acts[0])
	}
	// Remote edited after our delete: pull the file back.
	acts = Plan(
		[]ManifestEntry{deleted("f.txt", "h", "a:1")},
		[]ManifestEntry{alive("f.txt", "edited", "a:2")},
	)
	if acts[0].Kind != ActionPullCreate {
		t.Fatalf("actions = %+v", acts[0])
	}
}

func TestPlanTombstoneWithoutCounterpartIsQuiet(t *testing.T) {
	acts := Plan([]ManifestEntry{deleted("gone.txt", "h", "a:1")}, nil)
	if acts[0].Kind != ActionNoop {
		t.Fatalf("tombstones for unseen paths must not do anything, got %+v", acts[0])
	}
}

func TestWinnerDeterministic(t *testing.T) {
	// The documented rule (README, LOCUS_DESIGN §7): the greater content hash
	// wins; the writer peer ID only breaks a tie between identical hashes.
	cases := []struct {
		name            string
		local, incoming Version
		want            Version
	}{
		{"greater hash wins over greater writer", Version{"bbb", "h0"}, Version{"aaa", "h1"}, Version{"aaa", "h1"}},
		{"greater hash wins locally", Version{"aaa", "h1"}, Version{"bbb", "h0"}, Version{"aaa", "h1"}},
		{"same writer, hash decides", Version{"same", "a"}, Version{"same", "b"}, Version{"same", "b"}},
		{"same writer, hash decides (reverse)", Version{"same", "b"}, Version{"same", "a"}, Version{"same", "b"}},
		{"equal hash, greater writer breaks tie", Version{"aaa", "h"}, Version{"bbb", "h"}, Version{"bbb", "h"}},
		{"equal hash, greater writer breaks tie (reverse)", Version{"bbb", "h"}, Version{"aaa", "h"}, Version{"bbb", "h"}},
		{"identical versions keep local", Version{"aaa", "h"}, Version{"aaa", "h"}, Version{"aaa", "h"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Winner(tc.local, tc.incoming); got != tc.want {
				t.Fatalf("Winner(%v, %v) = %v, want %v", tc.local, tc.incoming, got, tc.want)
			}
			// Both peers see the same pair from opposite sides and must agree.
			if got := Winner(tc.incoming, tc.local); got != tc.want {
				t.Fatalf("Winner(%v, %v) = %v, want %v (other peer's perspective)", tc.incoming, tc.local, got, tc.want)
			}
			if tc.local == tc.incoming {
				return
			}
			if got, want := IsIncomingWinner(tc.local, tc.incoming), tc.want == tc.incoming; got != want {
				t.Fatalf("IsIncomingWinner(%v, %v) = %v, want %v", tc.local, tc.incoming, got, want)
			}
			if got, want := IsIncomingWinner(tc.incoming, tc.local), tc.want == tc.local; got != want {
				t.Fatalf("IsIncomingWinner(%v, %v) = %v, want %v", tc.incoming, tc.local, got, want)
			}
		})
	}
}

func TestSiblingNameDeterministicAndSanitized(t *testing.T) {
	hosts := map[string]string{"peer-a": "MacBook-Pro", "peer-b": "linux box!"}
	a := SiblingName("dir/proposal.docx", "peer-a", "1a2b3c4d5e6f", hosts)
	if a != "dir/proposal.conflict-macbook-pro-1a2b3c4d.docx" {
		t.Fatalf("sibling name = %q", a)
	}
	b := SiblingName("dir/proposal.docx", "peer-a", "1a2b3c4d5e6f", hosts)
	if a != b {
		t.Fatal("sibling names must be a pure function of the losing version")
	}
	unknown := SiblingName("notes.txt", "deadbeefcafe", "abcdef0123", nil)
	if unknown != "notes.conflict-deadbeef-abcdef01.txt" {
		t.Fatalf("unknown-host sibling = %q", unknown)
	}
	nested := SiblingName("a/b/c/file.conflict-x.txt", "p", "010203040506", map[string]string{"p": "h"})
	if nested != "a/b/c/file.conflict-x.conflict-h-01020304.txt" {
		t.Fatalf("nested sibling = %q", nested)
	}
	evil := SiblingName("dir/x.txt", "p", "h1hash1234", map[string]string{"p": "../../etc/passwd"})
	base := evil[strings.LastIndex(evil, "/")+1:]
	if strings.Contains(base, "..") || strings.Contains(base, "/") || base != "x.conflict-etc-passwd-h1hash12.txt" {
		t.Fatalf("unsanitized hostname leaked into sibling name: %q", base)
	}
	for _, name := range []string{a, unknown, nested} {
		if len(name) > 400 {
			t.Fatalf("unbounded sibling name length: %d", len(name))
		}
	}
}

func TestConflictIDStable(t *testing.T) {
	a := ConflictID("x.txt", "abcdef1234", "123456abcdef")
	b := ConflictID("x.txt", "abcdef1234", "123456abcdef")
	if a != b {
		t.Fatal("conflict ids must be stable")
	}
	if ConflictID("x.txt", "a", "b") == ConflictID("y.txt", "a", "b") {
		t.Fatal("conflict ids must include the path")
	}
}

func TestCanonicalRequestSigningRoundTrip(t *testing.T) {
	id := testIdentity(t)
	body := []byte("payload")
	ts, sig := SignRequest(id, "POST", "/v1/put?root=/r&relpath=a.txt", body)
	if !VerifySignature(id.PeerID, "POST", "/v1/put?root=/r&relpath=a.txt", ts, body, sig) {
		t.Fatal("valid signature rejected")
	}
	if VerifySignature(id.PeerID, "POST", "/v1/put?root=/r&relpath=b.txt", ts, body, sig) {
		t.Fatal("signature must cover the request target")
	}
	if VerifySignature(id.PeerID, "POST", "/v1/put?root=/r&relpath=a.txt", ts, []byte("tampered"), sig) {
		t.Fatal("signature must cover the body")
	}
	if VerifySignature(otherPeerID(t), "POST", "/v1/put?root=/r&relpath=a.txt", ts, body, sig) {
		t.Fatal("signature must be bound to the signer")
	}
}

func TestVerifyAuthHeadersRejectsStaleTimestamp(t *testing.T) {
	id := testIdentity(t)
	target := "/v1/manifest?root=/r"
	old := time.Now().Add(-2 * MaxClockSkew).Unix()
	_, sig := signAt(id, "GET", target, old, nil)
	h := mapToHeader(map[string]string{
		"X-Locus-Peer":      id.PeerID,
		"X-Locus-Timestamp": itoa(old),
		"X-Locus-Signature": sig,
	})
	if _, err := VerifyAuthHeaders(h, "GET", target, nil); err == nil {
		t.Fatal("stale signature timestamp accepted")
	}
}

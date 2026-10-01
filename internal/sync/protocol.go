// Package sync replicates files between equal Locus peers.
//
// The filesystem is the source of truth: synchronization replicates files,
// never search state. Transfers are whole-file, verified by content hash and
// installed with an atomic rename, so partial files never appear as valid
// documents. Version vectors distinguish descendants from genuine
// divergence; conflicts preserve both sides.
package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/teulaert/locus/internal/identity"
)

// ManifestEntry is one row of a peer's manifest.
type ManifestEntry struct {
	RelPath    string `json:"relpath"`
	Size       int64  `json:"size"`
	Hash       string `json:"hash"`
	Version    string `json:"version"`
	LastWriter string `json:"last_writer"`
	Deleted    bool   `json:"deleted,omitempty"`
}

// Manifest is the full file list a peer offers for one root.
type Manifest struct {
	PeerID string `json:"peer_id"`
	// Root is the local absolute path (diagnostics only); RootName is how
	// peers identify the same logical root on different machines.
	Root     string          `json:"root"`
	RootName string          `json:"root_name"`
	Entries  []ManifestEntry `json:"entries"`
}

// RootName is the default name of a synchronization root: its basename. Two
// peers agree on a root when its name matches, which lets the same logical
// root live at different absolute paths on different machines. A configured
// name (Engine.RootNames) overrides it, so the folder names may differ too.
func RootName(path string) string {
	clean := strings.TrimRight(path, "/\\")
	if clean == "" {
		return "/"
	}
	if i := strings.LastIndexAny(clean, "/\\"); i >= 0 {
		clean = clean[i+1:]
	}
	if clean == "" {
		return "/"
	}
	return clean
}

// Result is the outcome of applying an incoming operation.
type Result string

const (
	ResultApplied    Result = "applied"
	ResultNoop       Result = "noop"
	ResultConflicted Result = "conflicted"
	ResultRejected   Result = "rejected"
)

// CanonicalRequestMessage builds the string signed for a peer request. Both
// peers must build it identically.
func CanonicalRequestMessage(method, target string, ts int64, bodyHash []byte) []byte {
	return []byte(strings.Join([]string{method, target, strconv.FormatInt(ts, 10), hex.EncodeToString(bodyHash)}, "\n"))
}

// SignRequest signs an outgoing peer request.
func SignRequest(id *identity.Identity, method, target string, body []byte) (ts int64, sig string) {
	ts = time.Now().Unix()
	sum := sha256.Sum256(body)
	msg := CanonicalRequestMessage(method, target, ts, sum[:])
	return ts, hex.EncodeToString(id.Sign(msg))
}

// VerifySignature checks a signed peer request against the sender's public
// key (derived from the peer ID).
func VerifySignature(peerID, method, target string, ts int64, body []byte, sig string) bool {
	raw, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(body)
	return identity.Verify(peerID, CanonicalRequestMessage(method, target, ts, sum[:]), raw)
}

// AuthHeaders returns the identity headers a request must carry.
func AuthHeaders(id *identity.Identity, method, target string, body []byte) map[string]string {
	ts, sig := SignRequest(id, method, target, body)
	return map[string]string{
		"X-Locus-Peer":      id.PeerID,
		"X-Locus-Timestamp": strconv.FormatInt(ts, 10),
		"X-Locus-Signature": sig,
	}
}

// MaxClockSkew bounds accepted signature timestamps.
const MaxClockSkew = 5 * time.Minute

// VerifyAuthHeaders validates the auth headers of an incoming request and
// returns the claimed peer ID.
func VerifyAuthHeaders(h http.Header, method, target string, body []byte) (string, error) {
	peerID := h.Get("X-Locus-Peer")
	if peerID == "" {
		return "", fmt.Errorf("sync: missing peer identity")
	}
	tsv := h.Get("X-Locus-Timestamp")
	ts, err := strconv.ParseInt(tsv, 10, 64)
	if err != nil {
		return "", fmt.Errorf("sync: bad timestamp header")
	}
	skew := time.Since(time.Unix(ts, 0))
	if skew < -MaxClockSkew || skew > MaxClockSkew {
		return "", fmt.Errorf("sync: signature timestamp outside skew window")
	}
	if !VerifySignature(peerID, method, target, ts, body, h.Get("X-Locus-Signature")) {
		return "", fmt.Errorf("sync: signature verification failed for peer %s", identity.Short(peerID))
	}
	return peerID, nil
}

// EncodeJSON writes v as the body of a peer response.
func EncodeJSON(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

// Target builds the canonical request target (path?query) used for signing.
func Target(base, path string, q url.Values) string {
	u, err := url.Parse(base + path)
	if err != nil {
		return path
	}
	u.RawQuery = q.Encode()
	return u.RequestURI()
}

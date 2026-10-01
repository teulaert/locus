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

// SignRequest signs an outgoing peer request whose body is in memory.
func SignRequest(id *identity.Identity, method, target string, body []byte) (ts int64, sig string) {
	sum := sha256.Sum256(body)
	return SignRequestHash(id, method, target, sum[:])
}

// SignRequestHash signs an outgoing peer request given the SHA-256 of its
// body. A streamed put signs the content hash it declares in its query, so
// it can be signed, and verified, before the body is read (LOC-13).
func SignRequestHash(id *identity.Identity, method, target string, bodyHash []byte) (ts int64, sig string) {
	ts = time.Now().Unix()
	msg := CanonicalRequestMessage(method, target, ts, bodyHash)
	return ts, hex.EncodeToString(id.Sign(msg))
}

// VerifySignature checks a signed peer request against the sender's public
// key (derived from the peer ID).
func VerifySignature(peerID, method, target string, ts int64, body []byte, sig string) bool {
	sum := sha256.Sum256(body)
	return VerifySignatureHash(peerID, method, target, ts, sum[:], sig)
}

// VerifySignatureHash is VerifySignature for a body known by its hash.
func VerifySignatureHash(peerID, method, target string, ts int64, bodyHash []byte, sig string) bool {
	raw, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	return identity.Verify(peerID, CanonicalRequestMessage(method, target, ts, bodyHash), raw)
}

// emptyBodyHash is the body hash signed by every peer request but a put.
var emptyBodyHash = func() []byte { s := sha256.Sum256(nil); return s[:] }()

// DeclaredContentHash decodes the content hash a put declares in its query:
// the lowercase hex SHA-256 of the body. Only that canonical spelling is
// accepted, so the hash that was signed is the hash Install enforces.
func DeclaredContentHash(s string) ([]byte, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != sha256.Size || hex.EncodeToString(b) != s {
		return nil, fmt.Errorf("sync: missing or malformed content hash %q", s)
	}
	return b, nil
}

// AuthHeaders returns the identity headers a request must carry, plus the
// sync protocol version it is written in.
func AuthHeaders(id *identity.Identity, method, target string, body []byte) map[string]string {
	sum := sha256.Sum256(body)
	return AuthHeadersHash(id, method, target, sum[:])
}

// AuthHeadersHash is AuthHeaders for a body known by its hash.
func AuthHeadersHash(id *identity.Identity, method, target string, bodyHash []byte) map[string]string {
	ts, sig := SignRequestHash(id, method, target, bodyHash)
	return map[string]string{
		"X-Locus-Peer":      id.PeerID,
		"X-Locus-Timestamp": strconv.FormatInt(ts, 10),
		"X-Locus-Signature": sig,
		ProtocolHeader:      strconv.Itoa(ProtocolVersion),
	}
}

// ProtocolHeader carries the sync protocol version on every peer request and
// response. It is not signed: it negotiates, it does not authenticate.
const ProtocolHeader = "X-Locus-Protocol"

// ProtocolVersion is the sync protocol version spoken by this build: the
// manifest format and the meaning of put, delete and Apply. Bump it when a
// change would make an older peer misread what this one sends.
const ProtocolVersion = 1

// requestProtocol returns the sync protocol a request claims. Peers from
// before the header existed send none; they speak version 1.
func requestProtocol(h http.Header) (int, error) {
	v := h.Get(ProtocolHeader)
	if v == "" {
		return 1, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("sync: bad %s header %q", ProtocolHeader, v)
	}
	return n, nil
}

// IncompatibleError reports a peer that refused a request because it speaks a
// different sync protocol. It is an answer, not a transport failure: retrying
// cannot help until one side changes version.
type IncompatibleError struct {
	PeerProtocol int // the version the peer speaks, 0 if it did not say
	Msg          string
}

func (e *IncompatibleError) Error() string {
	if e.PeerProtocol != 0 {
		return fmt.Sprintf("sync: incompatible peer: it speaks sync protocol %d, this peer %d", e.PeerProtocol, ProtocolVersion)
	}
	return fmt.Sprintf("sync: incompatible peer: %s", e.Msg)
}

// MaxClockSkew bounds accepted signature timestamps.
const MaxClockSkew = 5 * time.Minute

// VerifyAuthHeaders validates the auth headers of an incoming request and
// returns the claimed peer ID.
func VerifyAuthHeaders(h http.Header, method, target string, body []byte) (string, error) {
	sum := sha256.Sum256(body)
	return VerifyAuthHeadersHash(h, method, target, sum[:])
}

// VerifyAuthHeadersHash validates the auth headers of a request whose body
// is known only by the hash it claims. The caller must then make sure the
// body it consumes has that hash before acting on it.
func VerifyAuthHeadersHash(h http.Header, method, target string, bodyHash []byte) (string, error) {
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
	if !VerifySignatureHash(peerID, method, target, ts, bodyHash, h.Get("X-Locus-Signature")) {
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

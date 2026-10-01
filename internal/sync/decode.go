package sync

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/teulaert/locus/internal/pathguard"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// ManifestError is a manifest the peer answered with but that cannot be
// trusted: undecodable JSON, a missing field, an entry whose version vector
// does not parse, or the manifest of another root. The peer answered, so it
// is an application error for that root, never a transport failure; and the
// root is skipped for that peer rather than planned against what would
// otherwise read as empty vectors (LOC-15).
type ManifestError struct {
	Root string // the root name that was requested
	Err  error
}

func (e *ManifestError) Error() string {
	return fmt.Sprintf("sync: malformed manifest for root %q: %v", e.Root, e.Err)
}

func (e *ManifestError) Unwrap() error { return e.Err }

// DecodeManifest parses and validates a peer's manifest for the root it was
// asked for. Every entry must carry a clean relative path, a writer, a
// non-empty well-formed version vector and, unless it is a tombstone, a
// content hash; a relpath may appear only once.
func DecodeManifest(body []byte, rootName string) (*Manifest, error) {
	fail := func(err error) (*Manifest, error) { return nil, &ManifestError{Root: rootName, Err: err} }
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return fail(err)
	}
	switch {
	case m.PeerID == "":
		return fail(errors.New("missing peer_id"))
	case m.RootName != rootName:
		return fail(fmt.Errorf("answered for root %q", m.RootName))
	case m.Entries == nil:
		// The server always sends a list, empty for an empty root. A missing
		// or null list would plan every local file as unknown to the peer.
		return fail(errors.New("missing entries"))
	}
	seen := make(map[string]bool, len(m.Entries))
	for i := range m.Entries {
		en := &m.Entries[i]
		if err := validateEntry(en); err != nil {
			return fail(fmt.Errorf("entry %d (%q): %w", i, en.RelPath, err))
		}
		if seen[en.RelPath] {
			return fail(fmt.Errorf("entry %d: duplicate relpath %q", i, en.RelPath))
		}
		seen[en.RelPath] = true
	}
	return &m, nil
}

func validateEntry(en *ManifestEntry) error {
	clean, err := pathguard.CleanRel(en.RelPath)
	if err != nil {
		return err
	}
	if clean != en.RelPath {
		return fmt.Errorf("relpath is not canonical (%q)", clean)
	}
	if en.LastWriter == "" {
		return errors.New("missing last_writer")
	}
	if !en.Deleted && en.Hash == "" {
		return errors.New("missing hash")
	}
	if en.Size < 0 {
		return fmt.Errorf("negative size %d", en.Size)
	}
	if _, err := ParseManifestVersion(en.Version); err != nil {
		return err
	}
	return nil
}

// ParseManifestVersion parses a version vector as a peer states it in a
// manifest. Stricter than vvector.Parse: every document record has seen at
// least one write, so an empty vector, a zero counter or a peer listed twice
// is corruption rather than a vector that precedes everything.
func ParseManifestVersion(s string) (vvector.Vector, error) {
	if strings.TrimSpace(s) == "" {
		return nil, errors.New("missing version")
	}
	v, err := vvector.Parse(s)
	if err != nil {
		return nil, err
	}
	if n := strings.Count(s, ",") + 1; n != len(v) {
		return nil, fmt.Errorf("version %q lists a peer twice", s)
	}
	for k, n := range v {
		if n == 0 {
			return nil, fmt.Errorf("version %q has a zero counter for %s", s, k)
		}
	}
	return v, nil
}

// applyResponse is a peer's answer to /v1/put and /v1/delete.
type applyResponse struct {
	Result   Result          `json:"result"`
	Error    string          `json:"error,omitempty"`
	Conflict *store.Conflict `json:"conflict,omitempty"`
}

// decodeApplyResponse reads the answer to a put or delete. A body that does
// not decode, names no known result, or carries an error is an error: a
// silently dropped rejection would let the round count as clean, and a clean
// round is what tombstone pruning relies on (LOC-15, LOC-34).
func decodeApplyResponse(body []byte, what string) (applyResponse, error) {
	var res applyResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return res, fmt.Errorf("sync: undecodable answer to %s: %w", what, err)
	}
	if res.Error != "" {
		return res, fmt.Errorf("peer rejected %s: %s", what, res.Error)
	}
	switch res.Result {
	case ResultApplied, ResultNoop, ResultConflicted:
		return res, nil
	case ResultRejected:
		return res, fmt.Errorf("peer rejected %s", what)
	default:
		return res, fmt.Errorf("sync: unknown result %q in answer to %s", res.Result, what)
	}
}

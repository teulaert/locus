package sync

import (
	"fmt"
	"path"
	"strings"

	"github.com/teulaert/locus/internal/identity"
)

// Version describes one side of a divergence for conflict decisions.
type Version struct {
	Writer string // peer ID of the last writer
	Hash   string // content hash
}

// Winner chooses the normal-path version with a deterministic rule that is
// computed identically on every peer: the version with the greater content
// hash wins, and only when both hashes are equal (identical content, so the
// choice changes nothing on disk) does the greater writer peer ID decide.
// Hashing first means no peer gets a standing advantage in every conflict.
// Nothing is ever discarded; the loser is preserved as a sibling conflict
// file. Every place that picks a winner (Apply, PullVersion) goes through
// this function or IsIncomingWinner, which share compareVersion.
func Winner(local, incoming Version) Version {
	if compareVersion(local, incoming) >= 0 {
		return local
	}
	return incoming
}

// IsIncomingWinner reports whether the incoming version wins the normal path.
func IsIncomingWinner(local, incoming Version) bool {
	return compareVersion(local, incoming) < 0
}

// compareVersion orders versions by content hash, then by writer peer ID.
func compareVersion(a, b Version) int {
	if a.Hash != b.Hash {
		return strings.Compare(a.Hash, b.Hash)
	}
	return strings.Compare(a.Writer, b.Writer)
}

// SiblingName returns the conflict sibling path for the losing version:
//
//	dir/proposal.conflict-macbook-1a2b3c4d.docx
//
// The name is a pure function of the loser's writer hostname (when known),
// content hash and the original name, so peers that detect the same conflict
// independently converge on the same sibling path.
func SiblingName(relpath, loserWriter, loserHash string, hostnames map[string]string) string {
	dir, base := path.Split(relpath)
	stem, ext := splitExt(base)
	host := hostnames[loserWriter]
	if host == "" {
		host = identity.Short(loserWriter)
	}
	host = sanitize(host)
	short := shortHash(loserHash)
	name := fmt.Sprintf("%s.conflict-%s-%s%s", stem, host, short, ext)
	return dir + name
}

func splitExt(name string) (stem, ext string) {
	i := strings.LastIndex(name, ".")
	if i <= 0 {
		return name, ""
	}
	return name[:i], name[i:]
}

func shortHash(h string) string {
	if len(h) <= 8 {
		return h
	}
	return h[:8]
}

func sanitize(s string) string {
	var sb strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			sb.WriteRune(r)
		default:
			sb.WriteByte('-')
		}
	}
	out := strings.Trim(sb.String(), "-")
	if out == "" {
		return "peer"
	}
	if len(out) > 32 {
		out = out[:32]
	}
	return out
}

// ConflictID is a stable identifier for a conflict record:
//
//	docs:dir/proposal.docx:1a2b3c4d:9f8e7d6c
//
// rootName is the name peers know the root by (Engine.NameOf), never its
// local path, so every peer derives the same ID for the same conflict and a
// conflict reported back by a peer matches the one recorded here (LOC-72).
// The hashes are the kept (normal-path) version's and the other version's.
// Records written by earlier builds carry the local root path or no root at
// all in their ID; they keep it and stay resolvable by it.
func ConflictID(rootName, relpath, keepHash, otherHash string) string {
	return fmt.Sprintf("%s:%s:%s:%s", rootName, relpath, shortHash(keepHash), shortHash(otherHash))
}

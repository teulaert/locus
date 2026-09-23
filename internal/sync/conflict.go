package sync

import (
	"fmt"
	"path"
	"strings"

	"locus/internal/identity"
)

// Version describes one side of a divergence for conflict decisions.
type Version struct {
	Writer string // peer ID of the last writer
	Hash   string // content hash
}

// Winner chooses the normal-path version with a deterministic rule that is
// computed identically on every peer: the greater (writer peer ID, content
// hash) pair wins. Nothing is ever discarded; the loser is preserved as a
// sibling conflict file.
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

func compareVersion(a, b Version) int {
	if a.Writer != b.Writer {
		return strings.Compare(a.Writer, b.Writer)
	}
	return strings.Compare(a.Hash, b.Hash)
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

// ConflictID is a stable identifier for a conflict record.
func ConflictID(relpath, localHash, conflictHash string) string {
	return fmt.Sprintf("%s:%s:%s", relpath, shortHash(localHash), shortHash(conflictHash))
}

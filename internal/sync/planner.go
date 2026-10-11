package sync

import (
	"cmp"
	"slices"

	"github.com/teulaert/locus/internal/vvector"
)

// ActionKind enumerates planned operations.
type ActionKind string

const (
	ActionPushCreate ActionKind = "push_create"
	ActionPushUpdate ActionKind = "push_update"
	ActionPushDelete ActionKind = "push_delete"
	ActionPullCreate ActionKind = "pull_create"
	ActionPullUpdate ActionKind = "pull_update"
	ActionPullDelete ActionKind = "pull_delete"
	// ActionPullVersion adopts the peer's version vector for a file both
	// sides already hold with identical content. It transfers no data.
	ActionPullVersion ActionKind = "pull_version"
	ActionNoop        ActionKind = "noop"
)

// Action is one planned operation between two manifest views.
type Action struct {
	Kind    ActionKind
	RelPath string
	Local   *ManifestEntry
	Remote  *ManifestEntry
}

// Plan compares manifests and determines operations. Divergent edits are
// planned as pushes; the receiving peer applies deterministic conflict
// resolution, which converges because both sides compute the same outcome.
func Plan(local, remote []ManifestEntry) []Action {
	rm := index(remote)
	var out []Action
	seen := map[string]bool{}
	for _, e := range local {
		seen[e.RelPath] = true
		r, ok := rm[e.RelPath]
		if !ok {
			switch {
			case e.Deleted:
				// We deleted something the other peer never had.
				out = append(out, Action{Kind: ActionNoop, RelPath: e.RelPath, Local: &e})
			default:
				out = append(out, Action{Kind: ActionPushCreate, RelPath: e.RelPath, Local: &e})
			}
			continue
		}
		out = append(out, decide(&e, &r))
	}
	for _, e := range remote {
		if seen[e.RelPath] {
			continue
		}
		if e.Deleted {
			// Remote deleted something we never had.
			out = append(out, Action{Kind: ActionNoop, RelPath: e.RelPath, Remote: &e})
			continue
		}
		out = append(out, Action{Kind: ActionPullCreate, RelPath: e.RelPath, Remote: &e})
	}
	return out
}

// Schedule orders planned actions for execution: everything that moves no
// file content (deletions, version adoption, no-ops) first, then content
// transfers smallest first, so a round cut short by a shutdown, a sleeping
// laptop or a changed network has delivered as many files as it could
// before it reached the large ones (LOC-67). Each action concerns its own
// path, so the order changes nothing about the outcome. Ties keep the plan's
// order.
func Schedule(actions []Action) []Action {
	out := slices.Clone(actions)
	slices.SortStableFunc(out, func(a, b Action) int {
		return cmp.Compare(transferSize(a), transferSize(b))
	})
	return out
}

// transferSize is the content an action moves: -1 for none.
func transferSize(a Action) int64 {
	switch a.Kind {
	case ActionPushCreate, ActionPushUpdate:
		if a.Local != nil {
			return a.Local.Size
		}
	case ActionPullCreate, ActionPullUpdate:
		if a.Remote != nil {
			return a.Remote.Size
		}
	}
	return -1
}

func decide(l, r *ManifestEntry) Action {
	a := Action{RelPath: l.RelPath, Local: l, Remote: r}
	lv, lerr := vvector.Parse(l.Version)
	rv, rerr := vvector.Parse(r.Version)
	switch {
	case lerr != nil || rerr != nil:
		// A vector that does not parse must not read as empty: an empty
		// vector precedes everything and would plan a push or delete over
		// the other side. FetchManifest rejects such manifests; this keeps
		// Plan safe for any other caller (LOC-15).
		a.Kind = ActionNoop
	case l.Deleted && r.Deleted:
		a.Kind = ActionNoop
	case l.Deleted && !r.Deleted:
		switch vvector.Compare(lv, rv) {
		case vvector.After, vvector.Equal:
			a.Kind = ActionPushDelete
		case vvector.Before:
			a.Kind = ActionPullCreate // remote edit supersedes our tombstone
		default:
			a.Kind = ActionPushDelete // delete-vs-edit conflict resolved on apply
		}
	case !l.Deleted && r.Deleted:
		switch vvector.Compare(lv, rv) {
		case vvector.Before, vvector.Equal:
			a.Kind = ActionPullDelete
		case vvector.After:
			a.Kind = ActionPushUpdate
		default:
			a.Kind = ActionPushUpdate
		}
	default: // both alive
		rel := vvector.Compare(lv, rv)
		if l.Hash == r.Hash {
			// Same content, but the vectors can still differ: after an
			// incoming-wins conflict only the receiver records the merged
			// vector. Left alone, the next ordinary edit on the other side
			// would look divergent and conflict again, so the side that is
			// behind (or concurrent) adopts the vectors without a transfer.
			// When we are ahead the peer adopts ours on its own round.
			switch rel {
			case vvector.Before, vvector.Divergent:
				a.Kind = ActionPullVersion
			default:
				a.Kind = ActionNoop
			}
			break
		}
		switch rel {
		case vvector.Before:
			a.Kind = ActionPullUpdate
		case vvector.After:
			a.Kind = ActionPushUpdate
		default:
			// Genuine divergence: push; the peer resolves the conflict
			// deterministically and identically.
			a.Kind = ActionPushUpdate
		}
	}
	return a
}

func index(entries []ManifestEntry) map[string]ManifestEntry {
	m := make(map[string]ManifestEntry, len(entries))
	for _, e := range entries {
		m[e.RelPath] = e
	}
	return m
}

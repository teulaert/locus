package sync

import (
	"locus/internal/vvector"
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
	ActionNoop       ActionKind = "noop"
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

func decide(l, r *ManifestEntry) Action {
	a := Action{RelPath: l.RelPath, Local: l, Remote: r}
	switch {
	case l.Deleted && r.Deleted:
		a.Kind = ActionNoop
	case l.Deleted && !r.Deleted:
		lv, _ := vvector.Parse(l.Version)
		rv, _ := vvector.Parse(r.Version)
		switch vvector.Compare(lv, rv) {
		case vvector.After, vvector.Equal:
			a.Kind = ActionPushDelete
		case vvector.Before:
			a.Kind = ActionPullCreate // remote edit supersedes our tombstone
		default:
			a.Kind = ActionPushDelete // delete-vs-edit conflict resolved on apply
		}
	case !l.Deleted && r.Deleted:
		lv, _ := vvector.Parse(l.Version)
		rv, _ := vvector.Parse(r.Version)
		switch vvector.Compare(lv, rv) {
		case vvector.Before, vvector.Equal:
			a.Kind = ActionPullDelete
		case vvector.After:
			a.Kind = ActionPushUpdate
		default:
			a.Kind = ActionPushUpdate
		}
	default: // both alive
		if l.Hash == r.Hash {
			a.Kind = ActionNoop
			break
		}
		lv, _ := vvector.Parse(l.Version)
		rv, _ := vvector.Parse(r.Version)
		switch vvector.Compare(lv, rv) {
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

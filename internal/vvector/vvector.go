// Package vvector implements per-file causal version vectors used to tell
// descendants apart from genuine divergence.
package vvector

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Vector maps peer ID to a monotonically increasing counter.
type Vector map[string]uint64

// New returns an empty vector.
func New() Vector { return Vector{} }

// Clone returns a deep copy.
func (v Vector) Clone() Vector {
	out := make(Vector, len(v))
	for k, n := range v {
		out[k] = n
	}
	return out
}

// Increment returns a copy with the counter for peer bumped by one.
func (v Vector) Increment(peer string) Vector {
	out := v.Clone()
	out[peer]++
	return out
}

// MergeMax returns the element-wise maximum of two vectors.
func MergeMax(a, b Vector) Vector {
	out := a.Clone()
	for k, v := range b {
		if v > out[k] {
			out[k] = v
		}
	}
	return out
}

// Compare reports the causal relation between a and b.
func Compare(a, b Vector) Relation {
	var lt, gt bool
	for _, k := range unionKeys(a, b) {
		av, bv := a[k], b[k]
		switch {
		case av < bv:
			lt = true
		case av > bv:
			gt = true
		}
	}
	switch {
	case lt && gt:
		return Divergent
	case lt:
		return Before
	case gt:
		return After
	default:
		return Equal
	}
}

// Relation describes how two version vectors relate causally.
type Relation int

const (
	// Equal means identical history.
	Equal Relation = iota
	// Before means a is a strict ancestor of b.
	Before
	// After means a is a strict descendant of b.
	After
	// Divergent means concurrent edits; neither supersedes the other.
	Divergent
)

func (r Relation) String() string {
	switch r {
	case Equal:
		return "equal"
	case Before:
		return "before"
	case After:
		return "after"
	default:
		return "divergent"
	}
}

func unionKeys(a, b Vector) []string {
	seen := map[string]bool{}
	var keys []string
	for k := range a {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	for k := range b {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// String renders a canonical, sorted "peer:counter,peer:counter" form.
func (v Vector) String() string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%s:%d", k, v[k])
	}
	return sb.String()
}

// Parse reads the canonical form produced by String.
func Parse(s string) (Vector, error) {
	v := Vector{}
	s = strings.TrimSpace(s)
	if s == "" {
		return v, nil
	}
	for _, part := range strings.Split(s, ",") {
		k, n, ok := strings.Cut(part, ":")
		if !ok || k == "" {
			return nil, fmt.Errorf("vvector: malformed entry %q", part)
		}
		val, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("vvector: malformed counter %q: %w", part, err)
		}
		v[k] = val
	}
	return v, nil
}

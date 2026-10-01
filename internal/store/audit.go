package store

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// PruneAudit bounds the audit log: rows older than maxAge are removed, then
// all but the newest maxRows. A non-positive limit disables that half of the
// sweep. It returns the number of rows deleted.
func (s *Store) PruneAudit(maxRows int, maxAge time.Duration) (int64, error) {
	var total int64
	if maxAge > 0 {
		res, err := s.State.Exec(`DELETE FROM audit WHERE ts < ?`, time.Now().Add(-maxAge).Unix())
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	if maxRows > 0 {
		// The subquery is NULL while the table holds at most maxRows rows,
		// and "seq <= NULL" matches nothing.
		res, err := s.State.Exec(`DELETE FROM audit WHERE seq <= (SELECT seq FROM audit ORDER BY seq DESC LIMIT 1 OFFSET ?)`, maxRows)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

// maxTallyKeys caps the number of distinct sources an AuditTally tracks
// between flushes; further sources are counted under one overflow key so
// memory stays bounded however many addresses a caller uses.
const maxTallyKeys = 256

type tallyKey struct{ actor, action, source string }

type tallyEntry struct {
	count  int
	sample string
}

// AuditTally aggregates events that arrive from unauthenticated (or not yet
// authorized) callers, such as rejected signatures and bearer tokens. Writing
// one audit row per request would let anyone who can reach a port grow
// state.db without bound and contend with real writes on its single
// connection, so Note only counts in memory and Flush writes one summary row
// per (actor, action, source) seen since the previous flush.
type AuditTally struct {
	mu      sync.Mutex
	pending map[tallyKey]*tallyEntry
}

// NewAuditTally returns an empty tally.
func NewAuditTally() *AuditTally {
	return &AuditTally{pending: map[tallyKey]*tallyEntry{}}
}

// Note counts one event from source (typically the remote host). sample is
// a short detail kept from the most recent event for the summary row. A nil
// tally ignores events.
func (t *AuditTally) Note(actor, action, source, sample string) {
	if t == nil {
		return
	}
	if len(sample) > 200 {
		sample = sample[:200]
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	k := tallyKey{actor, action, source}
	e, ok := t.pending[k]
	if !ok && len(t.pending) >= maxTallyKeys {
		k.source = "other"
		e, ok = t.pending[k]
	}
	if !ok {
		e = &tallyEntry{}
		t.pending[k] = e
	}
	e.count++
	e.sample = sample
}

// Flush writes one audit row per aggregated source and resets the tally.
func (t *AuditTally) Flush(s *Store) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	pending := t.pending
	t.pending = map[tallyKey]*tallyEntry{}
	t.mu.Unlock()
	keys := make([]tallyKey, 0, len(pending))
	for k := range pending {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.actor != b.actor {
			return a.actor < b.actor
		}
		if a.action != b.action {
			return a.action < b.action
		}
		return a.source < b.source
	})
	var firstErr error
	for _, k := range keys {
		e := pending[k]
		detail := fmt.Sprintf("%d from %s", e.count, k.source)
		if e.sample != "" {
			detail += " (last: " + e.sample + ")"
		}
		if err := s.Audit(k.actor, k.action, detail); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

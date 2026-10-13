package sync

import (
	"database/sql"
	"errors"
)

// pullKey names one pull: a version of a file, by content, in a root.
type pullKey struct{ root, relpath, hash string }

// claimPull marks the pull of relpath's content hash as in flight from
// peerID, so another peer's round, planned from the same local manifest,
// does not fetch the same bytes alongside it (LOC-77). It reports false
// when another round already holds the claim. The returned release must be
// called once the pull is over, whether it succeeded or not; calling it
// more than once is harmless.
func (e *Engine) claimPull(root, relpath, hash, peerID string) (release func(), ok bool) {
	k := pullKey{root, relpath, hash}
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	if _, held := e.inflight[k]; held {
		return nil, false
	}
	if e.inflight == nil {
		e.inflight = make(map[pullKey]string)
	}
	e.inflight[k] = peerID
	released := false
	return func() {
		e.inflightMu.Lock()
		defer e.inflightMu.Unlock()
		if !released {
			released = true
			delete(e.inflight, k)
		}
	}, true
}

// pullsInFlight counts the claimed pulls.
func (e *Engine) pullsInFlight() int {
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	return len(e.inflight)
}

// holdsLive reports whether the state database records relpath as live
// with content hash: a pull planned before another peer's round applied
// that content would only download what is already here.
func (e *Engine) holdsLive(root, relpath, hash string) (bool, error) {
	doc, err := e.Store.GetDoc(root, relpath)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !doc.Deleted && doc.Hash == hash, nil
}

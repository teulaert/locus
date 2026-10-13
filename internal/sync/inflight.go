package sync

import (
	"database/sql"
	"errors"
)

// transferKey names one incoming transfer: a version of a file, by content,
// in a root. Pulls this peer's rounds make and puts other peers send it are
// keyed alike, so neither fetches nor receives bytes the other is already
// bringing in.
type transferKey struct{ root, relpath, hash string }

// transferClaim is who holds a transferKey: the peer the content comes
// from, and whether it arrives through that peer's put (else this peer's
// own pull).
type transferClaim struct {
	peerID string
	put    bool
}

// claimPull marks the pull of relpath's content hash as in flight from
// peerID, so another peer's round, planned from the same local manifest,
// does not fetch the same bytes alongside it (LOC-77), and a peer pushing
// the same content meanwhile is told it is on its way (LOC-78). It reports
// false when a pull or an incoming put already holds the claim. The
// returned release must be called once the pull is over, whether it
// succeeded or not; calling it more than once is harmless.
func (e *Engine) claimPull(root, relpath, hash, peerID string) (release func(), ok bool) {
	return e.claimTransfer(transferKey{root, relpath, hash}, transferClaim{peerID: peerID})
}

// claimPut marks a put of relpath's content hash from peerID as being
// received, so another peer's put of the same content is answered as in
// progress instead of streamed alongside it, and a pull of it is skipped
// (LOC-78). It reports false when a pull or another put holds the claim.
// release follows the rules of claimPull's.
func (e *Engine) claimPut(root, relpath, hash, peerID string) (release func(), ok bool) {
	return e.claimTransfer(transferKey{root, relpath, hash}, transferClaim{peerID: peerID, put: true})
}

func (e *Engine) claimTransfer(k transferKey, c transferClaim) (release func(), ok bool) {
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	if _, held := e.inflight[k]; held {
		return nil, false
	}
	if e.inflight == nil {
		e.inflight = make(map[transferKey]transferClaim)
	}
	e.inflight[k] = c
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

// inFlight reports whether a pull or an incoming put of relpath's content
// hash holds a claim right now.
func (e *Engine) inFlight(root, relpath, hash string) bool {
	e.inflightMu.Lock()
	defer e.inflightMu.Unlock()
	_, held := e.inflight[transferKey{root, relpath, hash}]
	return held
}

// claimsInFlight counts the claimed transfers, pulls and puts together.
func (e *Engine) claimsInFlight() int {
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

// Served reports how many files, and how many bytes of file content, this
// engine's server has sent to peers since it started. It shows how much a
// peer was asked for, such as the same file by one peer from several
// sources (LOC-77).
func (e *Engine) Served() (files, bytes int64) {
	return e.servedFiles.Load(), e.servedBytes.Load()
}

// Received reports how many puts this engine's server has received in full
// from peers since it started, whatever applying them then did. A put
// answered early, because this peer held its content or was already
// receiving it (LOC-78), is not counted.
func (e *Engine) Received() int64 { return e.receivedFiles.Load() }

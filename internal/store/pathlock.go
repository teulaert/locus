package store

import (
	"log/slog"
	"sync"
	"time"
)

// pathLocks is a set of mutexes keyed by root and relpath, created on demand
// and dropped when the last holder releases them. The zero value is ready.
type pathLocks struct {
	mu sync.Mutex
	m  map[string]*pathLock
}

type pathLock struct {
	mu   sync.Mutex
	refs int
	// holder names the operation holding mu (guarded by pathLocks.mu).
	holder string
}

// DefaultLockWarnAfter is how long a LockPath wait may take before it is
// logged as contention.
const DefaultLockWarnAfter = 5 * time.Second

// LockPath acquires the lock for root/relpath and returns its release.
//
// Every read-decide-write of one path's document record holds it: the sync
// engine's Apply and PullVersion (LOC-7) and the reconciler's record writes
// (LOC-58). It lives on the Store because that is what both share, so a
// received file cannot be installed and recorded between the reconciler
// re-reading a record and replacing it.
//
// Nothing may wait on a peer while holding it: received content is staged
// and verified first, so a hold lasts as long as a local install and a
// record write (LOC-69).
func (s *Store) LockPath(root, relpath string) (unlock func()) {
	return s.LockPathFor(root, relpath, "")
}

// LockPathFor is LockPath for an operation named op. A wait longer than
// LockWarnAfter is logged while it lasts and again once the lock is taken,
// naming the path, the operation holding it and how long the wait took, so
// contention is visible rather than a silent stall (LOC-69).
func (s *Store) LockPathFor(root, relpath, op string) (unlock func()) {
	p := &s.paths
	key := root + "\x00" + relpath
	p.mu.Lock()
	if p.m == nil {
		p.m = make(map[string]*pathLock)
	}
	l := p.m[key]
	if l == nil {
		l = &pathLock{}
		p.m[key] = l
	}
	l.refs++
	p.mu.Unlock()

	if !l.mu.TryLock() {
		warnAfter := s.LockWarnAfter
		if warnAfter <= 0 {
			warnAfter = DefaultLockWarnAfter
		}
		started := time.Now()
		holder := func() string {
			p.mu.Lock()
			defer p.mu.Unlock()
			return l.holder
		}
		slow := time.AfterFunc(warnAfter, func() {
			s.log().Warn("waiting for path lock", "root", root, "relpath", relpath, "op", op, "held_by", holder(), "waited", time.Since(started).Round(time.Millisecond).String())
		})
		l.mu.Lock()
		if !slow.Stop() {
			s.log().Warn("path lock acquired after a long wait", "root", root, "relpath", relpath, "op", op, "waited", time.Since(started).Round(time.Millisecond).String())
		}
	}
	p.mu.Lock()
	l.holder = op
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		l.holder = ""
		p.mu.Unlock()
		l.mu.Unlock()
		p.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(p.m, key)
		}
		p.mu.Unlock()
	}
}

func (s *Store) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

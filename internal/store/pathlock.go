package store

import "sync"

// pathLocks is a set of mutexes keyed by root and relpath, created on demand
// and dropped when the last holder releases them. The zero value is ready.
type pathLocks struct {
	mu sync.Mutex
	m  map[string]*pathLock
}

type pathLock struct {
	mu   sync.Mutex
	refs int
}

// LockPath acquires the lock for root/relpath and returns its release.
//
// Every read-decide-write of one path's document record holds it: the sync
// engine's Apply and PullVersion (LOC-7) and the reconciler's record writes
// (LOC-58). It lives on the Store because that is what both share, so a
// received file cannot be installed and recorded between the reconciler
// re-reading a record and replacing it.
func (s *Store) LockPath(root, relpath string) (unlock func()) {
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
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		p.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(p.m, key)
		}
		p.mu.Unlock()
	}
}

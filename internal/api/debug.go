package api

import (
	"errors"
	"net"
	"net/http"
	"net/netip"
)

// The Go runtime profiles (LOC-84), so memory and CPU can be measured on a
// live peer:
//
//	curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8910/debug/pprof/heap > heap.pprof
//	go tool pprof -sample_index=inuse_space -top heap.pprof
//
// They need the API token like every other route, and are served only to
// clients on the loopback interface, whatever api.addr is: a profile shows
// file paths, and a CPU profile or trace keeps the process busy while it
// runs. Importing net/http/pprof also registers these handlers on
// http.DefaultServeMux, which Locus never serves.

// loopbackOnly refuses requests that do not come from a loopback address.
func (s *server) loopbackOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !fromLoopback(r) {
			writeErr(w, http.StatusForbidden, errors.New("profiles are served to loopback clients only"))
			return
		}
		next(w, r)
	}
}

func fromLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.Unmap().IsLoopback()
}

// debug wraps a profile handler in the token check and the loopback rule.
func (s *server) debug(h http.HandlerFunc) http.HandlerFunc {
	return s.auth(s.loopbackOnly(h))
}

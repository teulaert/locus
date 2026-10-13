package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// service is an HTTP server Serve runs, with what its shutdown needs: the
// requests in flight, and a base context their contexts derive from, which
// is cancelled to cut short the ones still running after the grace period
// (LOC-81).
type service struct {
	srv      *http.Server
	requests *inflight
	cancel   context.CancelFunc
}

func newService(srv *http.Server) *service {
	base, cancel := context.WithCancel(context.Background())
	svc := &service{srv: srv, requests: newInflight(), cancel: cancel}
	srv.BaseContext = func(net.Listener) context.Context { return base }
	srv.Handler = svc.requests.wrap(srv.Handler)
	return svc
}

// stop shuts the server down: it stops accepting connections and lets the
// requests in flight finish for up to grace. Those still running then are
// cut short: their contexts are cancelled and their connections closed, so
// a put body trickling in on a slow link ends at once rather than at its
// idle deadline (an interrupted put is resumed, LOC-67). stop returns once
// every handler has returned.
func (s *service) stop(grace time.Duration, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		if d := s.requests.describe(3); d != "" {
			log.Info("cutting short requests still running at shutdown", "addr", s.srv.Addr, "grace", grace.String(), "requests", d)
		}
		s.cancel()
		_ = s.srv.Close()
	}
	s.cancel()
	s.requests.wait()
}

// inflight counts the requests a handler is serving, so a shutdown can wait
// for them (http.Server.Close forgets its connections without waiting) and
// say which ones are still running.
type inflight struct {
	mu   sync.Mutex
	cond *sync.Cond
	reqs map[*http.Request]struct{}
}

func newInflight() *inflight {
	f := &inflight{reqs: map[*http.Request]struct{}{}}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *inflight) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.reqs[r] = struct{}{}
		f.mu.Unlock()
		defer func() {
			f.mu.Lock()
			delete(f.reqs, r)
			f.cond.Broadcast()
			f.mu.Unlock()
		}()
		h.ServeHTTP(w, r)
	})
}

// wait returns once no request is in flight.
func (f *inflight) wait() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.reqs) > 0 {
		f.cond.Wait()
	}
}

// describe names up to limit requests in flight ("POST /v1/put docs/a.pdf"),
// sorted, and how many more there are; "" when there are none.
func (f *inflight) describe(limit int) string {
	f.mu.Lock()
	names := make([]string, 0, len(f.reqs))
	for r := range f.reqs {
		name := r.Method + " " + r.URL.Path
		if rel := r.URL.Query().Get("relpath"); rel != "" {
			name += " " + rel
		}
		names = append(names, name)
	}
	f.mu.Unlock()
	sort.Strings(names)
	if len(names) > limit {
		names = append(names[:limit], fmt.Sprintf("%d more", len(names)-limit))
	}
	return strings.Join(names, ", ")
}

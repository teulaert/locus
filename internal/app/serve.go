package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// ServeOption configures Serve.
type ServeOption func(*serveOptions)

type serveOptions struct {
	APIHandler      http.Handler
	ShutdownTimeout time.Duration
}

// DefaultShutdownTimeout bounds how long Serve waits, once its context is
// cancelled, for HTTP requests to finish and its goroutines to stop.
const DefaultShutdownTimeout = 15 * time.Second

// WithAPI mounts an HTTP handler (typically the versioned API + embedded PWA).
func WithAPI(h http.Handler) ServeOption { return func(o *serveOptions) { o.APIHandler = h } }

// WithShutdownTimeout replaces DefaultShutdownTimeout.
func WithShutdownTimeout(d time.Duration) ServeOption {
	return func(o *serveOptions) { o.ShutdownTimeout = d }
}

// ShutdownError reports the parts of the daemon that had not stopped when
// Serve's shutdown deadline ran out. Serve returns anyway, so a stalled
// download or a stuck embedding call cannot hold SIGTERM hostage.
type ShutdownError struct {
	Timeout time.Duration
	Pending []string
}

func (e *ShutdownError) Error() string {
	return fmt.Sprintf("shutdown: not stopped after %s: %s", e.Timeout, strings.Join(e.Pending, ", "))
}

// tasks tracks the named goroutines Serve starts, so it can wait for them
// with a deadline and say which ones are still running.
type tasks struct {
	wg      sync.WaitGroup
	mu      sync.Mutex
	running map[string]int
}

func (t *tasks) Go(name string, fn func()) {
	t.mu.Lock()
	if t.running == nil {
		t.running = map[string]int{}
	}
	t.running[name]++
	t.mu.Unlock()
	t.wg.Add(1)
	go func() {
		defer func() {
			t.mu.Lock()
			if t.running[name]--; t.running[name] == 0 {
				delete(t.running, name)
			}
			t.mu.Unlock()
			t.wg.Done()
		}()
		fn()
	}()
}

// Wait waits for every task until deadline and returns the names of those
// still running, sorted.
func (t *tasks) Wait(deadline time.Time) []string {
	done := make(chan struct{})
	go func() { t.wg.Wait(); close(done) }()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	pending := make([]string, 0, len(t.running))
	for name := range t.running {
		pending = append(pending, name)
	}
	sort.Strings(pending)
	return pending
}

// Serve runs the peer daemon until ctx is cancelled: discovery, presence,
// filesystem watching and reconciliation, index workers, synchronization and
// the HTTP services. It holds the data directory's daemon lock while it runs
// and refuses to start when another process holds it (ErrDaemonRunning).
//
// Every goroutine it starts is stopped before it returns, on cancellation and
// on an early error alike, within the shutdown timeout (WithShutdownTimeout).
// Whatever is still running at that deadline is reported as a *ShutdownError
// instead of being waited for indefinitely.
func (a *App) Serve(ctx context.Context, opts ...ServeOption) (err error) {
	so := &serveOptions{ShutdownTimeout: DefaultShutdownTimeout}
	for _, o := range opts {
		o(so)
	}
	if err := fswatch.EnsureRoots(a.Reconciler.Roots); err != nil {
		return err
	}
	lock, err := lockDataDir(a.DataDir())
	if err != nil {
		return err
	}
	defer lock.Release()
	a.setServing(true)
	defer a.setServing(false)
	a.Log.Info("locus peer starting", "peer", identity.Short(a.Identity.PeerID), "roots", len(a.Reconciler.Roots))

	// Everything below runs under sctx, so an early return (a port that
	// cannot be bound) stops what was already started, just as cancelling
	// ctx does.
	sctx, cancel := context.WithCancel(ctx)
	var bg tasks
	var servers []*http.Server
	var watcher *fswatch.Watcher
	defer func() {
		cancel()
		deadline := time.Now().Add(so.ShutdownTimeout)
		sdCtx, sdCancel := context.WithDeadline(context.Background(), deadline)
		defer sdCancel()
		var pending []string
		for _, srv := range servers {
			if serr := srv.Shutdown(sdCtx); serr != nil {
				// Requests still in flight at the deadline are cut off.
				_ = srv.Close()
				pending = append(pending, "requests on "+srv.Addr)
			}
		}
		if watcher != nil {
			bg.Go("filesystem watcher", watcher.Stop)
		}
		pending = append(pending, bg.Wait(deadline)...)
		// After the servers: rejections counted by the last requests are
		// written too.
		a.MaintainAudit()
		if len(pending) > 0 {
			a.Log.Error("daemon did not stop cleanly", "timeout", so.ShutdownTimeout.String(), "pending", strings.Join(pending, ", "))
			if err == nil {
				err = &ShutdownError{Timeout: so.ShutdownTimeout, Pending: pending}
			}
		}
	}()

	// Discovery responder + prober.
	discSrv := &discovery.Server{Port: a.Cfg.Discovery.Port, Identity: a.Identity, Info: a.PeerInfo(), Authorized: a.Cfg.AuthorizedPeers}
	discSrv.OnProbe = func(addr string, p *discovery.Packet) {
		a.Presence.Observe(addr, p.Info, p.Protocol, discovery.Compatible(p, syncpkg.ProtocolVersion))
	}
	if err := discSrv.ListenAndServe(sctx); err != nil {
		a.Log.Warn("discovery responder unavailable", "err", err)
	} else {
		bg.Go("discovery responder", func() { <-discSrv.Done() })
	}
	prober := &discovery.Prober{
		Identity:   a.Identity,
		Info:       a.PeerInfo(),
		Table:      a.Presence,
		Targets:    a.Peers,
		Authorized: a.Cfg.AuthorizedPeers,
		Interval:   a.Cfg.Discovery.Interval.D(),

		SyncProtocol: syncpkg.ProtocolVersion,
	}
	bg.Go("discovery prober", func() { prober.Run(sctx) })

	// Peer synchronization service. There is deliberately no WriteTimeout:
	// it bounds a whole response, so it would cut off a large file transfer
	// on a slow link. A stalled transfer is cut off by the shutdown deadline.
	syncSrv := &syncpkg.Server{Engine: a.Sync, Identity: a.Identity, Roots: a.Reconciler.Roots, AuthorizedPeers: a.Cfg.AuthorizedPeers, Known: a.Presence.Known, Rejections: a.Rejections, Log: a.Log}
	peerAddr := fmt.Sprintf("%s:%d", a.Cfg.Peer.Addr, a.Cfg.Peer.Port)
	peerLn, err := net.Listen("tcp", peerAddr)
	if err != nil {
		return fmt.Errorf("listen peer port: %w", err)
	}
	peerSrv := &http.Server{Addr: peerAddr, Handler: syncSrv.Handler(), ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 5 * time.Minute, IdleTimeout: 2 * time.Minute}
	servers = append(servers, peerSrv)
	bg.Go("peer service", func() {
		if err := peerSrv.Serve(peerLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Log.Error("peer service", "err", err)
		}
	})

	// API service.
	if so.APIHandler != nil {
		apiAddr := a.APIAddr()
		apiLn, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return fmt.Errorf("listen api port: %w", err)
		}
		apiSrv := &http.Server{Addr: apiAddr, Handler: so.APIHandler, ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute}
		servers = append(servers, apiSrv)
		bg.Go("api service", func() {
			if err := apiSrv.Serve(apiLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				a.Log.Error("api service", "err", err)
			}
		})
	}

	// Index workers. Embeddings left stale by a model change detected in New
	// (or missing after an interrupted run) are queued once here; while the
	// daemon runs, every chunk is embedded by the job that wrote it.
	if n, err := a.Indexer.RequeueStaleEmbeddings(sctx); err != nil {
		a.Log.Warn("queue stale embeddings", "err", err)
	} else if n > 0 {
		a.Log.Info("queued regeneration of stale embeddings", "docs", n)
	}
	bg.Go("index workers", func() { a.Indexer.Run(sctx, a.Cfg.Workers) })

	// Reconciliation: watcher hints plus a periodic safety net.
	w := fswatch.NewWatcher(a.Reconciler.Roots, 300*time.Millisecond, func(reason string) {
		a.reconcileOnce(sctx)
	})
	w.Log = a.Log
	if err := w.Start(); err != nil {
		a.Log.Warn("filesystem watcher unavailable; relying on periodic reconciliation", "err", err)
	} else {
		watcher = w
	}
	a.reconcileOnce(sctx)

	syncTicker := time.NewTicker(a.Cfg.Sync.Interval.D())
	defer syncTicker.Stop()
	reconcileTicker := time.NewTicker(a.Cfg.ReconcileInterval.D())
	defer reconcileTicker.Stop()
	presenceTicker := time.NewTicker(5 * time.Second)
	defer presenceTicker.Stop()
	a.MaintainAudit()
	auditTicker := time.NewTicker(a.Cfg.Audit.Interval.D())
	defer auditTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.persistPresence()
			return nil
		case <-reconcileTicker.C:
			a.reconcileOnce(sctx)
		case <-syncTicker.C:
			a.syncOnce(sctx)
		case <-auditTicker.C:
			a.MaintainAudit()
		case <-presenceTicker.C:
			a.persistPresence()
		}
	}
}

// MaintainAudit writes the aggregated request rejections to the audit log and
// then applies the configured retention, so the log stays bounded however
// many requests are refused.
func (a *App) MaintainAudit() {
	if err := a.Rejections.Flush(a.Store); err != nil {
		a.Log.Error("audit flush", "err", err)
	}
	n, err := a.Store.PruneAudit(a.Cfg.Audit.MaxEntries, a.Cfg.Audit.MaxAge.D())
	if err != nil {
		a.Log.Error("audit prune", "err", err)
	} else if n > 0 {
		a.Log.Debug("pruned audit log", "rows", n)
	}
}

func (a *App) reconcileOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return // shutting down; the next start reconciles
	}
	reports, err := a.Reconciler.Pass(ctx)
	if err != nil {
		a.Log.Warn("reconciliation failed", "err", err)
		return
	}
	for _, r := range reports {
		if r.Created+r.Modified+r.Deleted+r.Jobs > 0 {
			a.Log.Info("reconciled", "root", r.Root, "created", r.Created, "modified", r.Modified, "deleted", r.Deleted, "jobs", r.Jobs)
		}
	}
}

func (a *App) setServing(v bool) {
	a.mu.Lock()
	a.serving = v
	a.mu.Unlock()
}

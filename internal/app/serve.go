package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/netguard"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// ServeOption configures Serve.
type ServeOption func(*serveOptions)

type serveOptions struct {
	APIHandler      http.Handler
	ShutdownTimeout time.Duration
	SyncDrain       time.Duration
}

// DefaultSyncDrain bounds how long a shutdown lets the transfer a sync round
// is in the middle of run on before cancelling it (LOC-67). No new transfer
// starts meanwhile. It never exceeds half the shutdown timeout.
const DefaultSyncDrain = 5 * time.Second

// WithSyncDrain replaces DefaultSyncDrain.
func WithSyncDrain(d time.Duration) ServeOption { return func(o *serveOptions) { o.SyncDrain = d } }

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
// download or a stuck embedding call cannot hold SIGTERM hostage. It is
// not a failure: state is consistent whatever was left running, and
// `locus serve` exits 0 on it (LOC-81).
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
	so := &serveOptions{ShutdownTimeout: DefaultShutdownTimeout, SyncDrain: DefaultSyncDrain}
	for _, o := range opts {
		o(so)
	}
	so.SyncDrain = min(so.SyncDrain, so.ShutdownTimeout/2)
	if !a.Cfg.Rendezvous {
		if err := fswatch.EnsureRoots(a.Reconciler.Roots); err != nil {
			return err
		}
	}
	lock, err := lockDataDir(a.DataDir())
	if err != nil {
		return err
	}
	defer lock.Release()
	// An installed update ends Serve through this cancel, like SIGTERM
	// does through the caller's ctx (LOC-86).
	ctx, stopServe := context.WithCancel(ctx)
	defer stopServe()
	a.mu.Lock()
	a.stopServe, a.restart = stopServe, false
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.stopServe = nil
		a.mu.Unlock()
	}()
	a.setServing(true)
	defer a.setServing(false)
	a.Log.Info("locus peer starting", "peer", identity.Short(a.Identity.PeerID), "roots", len(a.Reconciler.Roots), "rendezvous", a.Cfg.Rendezvous)

	// Everything below runs under sctx, so an early return (a port that
	// cannot be bound) stops what was already started, just as cancelling
	// ctx does.
	sctx, cancel := context.WithCancel(ctx)
	var bg tasks
	var servers []*service
	var watcher *fswatch.Watcher
	defer func() {
		cancel()
		// Every part stops at once and independently of the others
		// (LOC-81): the watcher does not wait for the HTTP servers, and
		// the servers do not wait for the sync drain. Each is a named
		// task, so whatever is still running at the deadline is named.
		deadline := time.Now().Add(so.ShutdownTimeout)
		if watcher != nil {
			bg.Go("filesystem watcher", watcher.Stop)
		}
		for _, svc := range servers {
			bg.Go("requests on "+svc.srv.Addr, func() { svc.stop(so.SyncDrain, a.Log) })
		}
		pending := bg.Wait(deadline)
		for i, name := range pending {
			for _, svc := range servers {
				if name == "requests on "+svc.srv.Addr {
					if d := svc.requests.describe(3); d != "" {
						pending[i] = name + " (" + d + ")"
					}
				}
			}
		}
		// After the servers: rejections counted by the last requests are
		// written too.
		a.MaintainAudit()
		if len(pending) > 0 {
			// Every write is atomic and an interrupted put is resumed
			// (LOC-67), so what is left running cannot leave state
			// inconsistent: this is worth a warning, not a failure.
			a.Log.Warn("daemon shutdown cut short", "timeout", so.ShutdownTimeout.String(), "pending", strings.Join(pending, ", "))
			if err == nil {
				err = &ShutdownError{Timeout: so.ShutdownTimeout, Pending: pending}
			}
		}
	}()

	// Sync rounds run under roundCtx rather than sctx: on shutdown they
	// are first asked to drain (finish the transfer in progress, start no
	// other) and cancelled only once that has taken SyncDrain, so a put
	// that is nearly done is not thrown away (LOC-67).
	//
	// Rounds run in goroutines of their own (one per tick, LOC-61), so the
	// main loop returns while they still transfer. roundCtx is therefore
	// cancelled here only, once every running round has returned or the
	// drain time is up, never by the loop's return.
	drain := make(chan struct{})
	roundCtx, roundCancel := context.WithCancel(syncpkg.WithDrain(context.WithoutCancel(sctx), drain))
	var rounds roundSet
	bg.Go("sync drain", func() {
		defer roundCancel()
		<-sctx.Done()
		close(drain)
		idle := rounds.stop()
		timer := time.NewTimer(so.SyncDrain)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-idle:
		}
	})

	// Interrupted incoming puts are kept for their senders to resume, but
	// not forever. Staging files and transfer temporaries inside the roots
	// can only be orphans of a killed process now that this one holds the
	// daemon lock, so they go regardless of age (LOC-69).
	a.sweepPartials()
	a.sweepOrphans()

	// Discovery responder + prober. The address book holds the configured
	// targets and every peer learned from discovery traffic; the responder
	// shares the peers it has verified with every prober that asks.
	book := a.discoveryBook()
	a.logAdvertised()
	discSrv := &discovery.Server{Port: a.Cfg.Discovery.Port, Identity: a.Identity, Info: a.PeerInfo(), Authorized: a.Cfg.AuthorizedPeers, OnProbe: a.observeProbe, Share: book.Share}
	if err := discSrv.ListenAndServe(sctx); err != nil {
		a.Log.Warn("discovery responder unavailable", "err", err)
	} else {
		bg.Go("discovery responder", func() { <-discSrv.Done() })
	}
	prober := &discovery.Prober{
		Identity:   a.Identity,
		Info:       a.PeerInfo(),
		Table:      a.Presence,
		Book:       book,
		Derive:     a.GatewayTargets,
		Authorized: a.Cfg.AuthorizedPeers,
		Interval:   a.Cfg.Discovery.Interval.D(),

		SyncProtocol: syncpkg.ProtocolVersion,
	}
	bg.Go("discovery prober", func() { prober.Run(sctx) })
	bg.Go("updater", func() { a.runUpdates(sctx) })

	// API service.
	startAPI := func() error {
		if so.APIHandler == nil {
			return nil
		}
		apiAddr := a.APIAddr()
		apiLn, err := net.Listen("tcp", apiAddr)
		if err != nil {
			return fmt.Errorf("listen api port: %w", err)
		}
		apiSvc := newService(&http.Server{Addr: apiAddr, Handler: so.APIHandler, ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute})
		servers = append(servers, apiSvc)
		bg.Go("api service", func() {
			if err := apiSvc.srv.Serve(apiLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				a.Log.Error("api service", "err", err)
			}
		})
		return nil
	}

	// A rendezvous peer (LOC-76) is discovery and the address book only:
	// no sync service, index workers, watcher or reconciler.
	if a.Cfg.Rendezvous {
		if err := startAPI(); err != nil {
			return err
		}
		a.markServing()
		return a.rendezvousLoop(ctx)
	}

	// Peer synchronization service. There is deliberately no WriteTimeout:
	// it bounds a whole response, so it would cut off a large file transfer
	// on a slow link. A put replaces ReadTimeout with a per-read inactivity
	// deadline (Server.BodyIdleTimeout), so a large upload on a slow link
	// completes and a stalled one ends within a minute (LOC-69).
	syncSrv := &syncpkg.Server{Engine: a.Sync, Identity: a.Identity, Roots: a.Reconciler.Roots, AuthorizedPeers: a.Cfg.AuthorizedPeers, Known: a.Presence.Known, Rejections: a.Rejections, Log: a.Log}
	peerAddr := fmt.Sprintf("%s:%d", a.Cfg.Peer.Addr, a.Cfg.Peer.Port)
	// Tests never take this machine's own sync port (LOC-80).
	if err := netguard.CheckListen(a.Cfg.Peer.Port); err != nil {
		return fmt.Errorf("listen peer port: %w", err)
	}
	peerLn, err := net.Listen("tcp", peerAddr)
	if err != nil {
		return fmt.Errorf("listen peer port: %w", err)
	}
	peerSvc := newService(&http.Server{Addr: peerAddr, Handler: syncSrv.Handler(), ReadHeaderTimeout: 30 * time.Second, ReadTimeout: 5 * time.Minute, IdleTimeout: 2 * time.Minute})
	servers = append(servers, peerSvc)
	bg.Go("peer service", func() {
		if err := peerSvc.srv.Serve(peerLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Log.Error("peer service", "err", err)
		}
	})

	if err := startAPI(); err != nil {
		return err
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

	// Reconciliation: watcher hints plus a periodic safety net. A pass the
	// watcher started runs under the watcher's context, which its Stop
	// cancels, so stopping the watcher never waits for a whole pass
	// (LOC-81).
	w := fswatch.NewWatcher(a.Reconciler.Roots, 300*time.Millisecond, func(wctx context.Context, reason string) {
		a.reconcileOnce(wctx)
	})
	w.SyncHidden = a.Reconciler.SyncHidden
	w.Log = a.Log
	// Start never fails: a watcher the platform cannot provide, or one
	// that runs out of descriptors or kernel watches, falls back to
	// polling and the reconcile ticker below finds every change (LOC-70).
	w.Start()
	watcher = w
	a.setWatcher(w)
	defer a.setWatcher(nil)
	if st := w.Stats(); st.Mode == fswatch.ModePoll {
		a.Log.Warn("filesystem changes are found by periodic reconciliation only", "every", a.Cfg.ReconcileInterval.D().String(), "reason", st.Reason)
	} else {
		a.Log.Info("watching roots", "mode", st.Mode, "watches", st.Watches)
	}
	// Listening on every port and watching the roots: a binary that got
	// this far is not rolled back (LOC-86).
	a.markServing()
	a.reconcileOnce(sctx)

	syncEvery := a.Cfg.Sync.Interval.D()
	syncTicker := time.NewTicker(syncEvery)
	defer syncTicker.Stop()
	a.setNextSync(time.Now().Add(syncEvery), syncEvery)
	defer a.setNextSync(time.Time{}, 0)
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
			a.persistLearnedPeers(false)
			return nil
		case <-reconcileTicker.C:
			a.reconcileOnce(sctx)
			a.sweepPartials()
		case <-syncTicker.C:
			a.setNextSync(time.Now().Add(syncEvery), syncEvery)
			// Each tick's round runs on its own, so a long transfer to
			// one peer neither holds up the loop nor the rounds of later
			// ticks with the other peers (LOC-61). syncOnce leaves out
			// peers a running round still holds. Rounds run under
			// roundCtx so a shutdown drains them (LOC-67).
			if rounds.start() {
				bg.Go("sync round", func() {
					defer rounds.done()
					a.syncOnce(roundCtx)
				})
			}
		case <-auditTicker.C:
			a.MaintainAudit()
		case <-presenceTicker.C:
			a.persistPresence()
			a.persistLearnedPeers(false)
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

// logAdvertised says which address this peer announces, with a warning
// when it is only the default route's (LOC-63).
func (a *App) logAdvertised() {
	adv := a.Advertised()
	addr := net.JoinHostPort(adv.Host, strconv.Itoa(a.Cfg.Peer.Port))
	if adv.Problem != "" {
		a.Log.Warn(adv.Problem+"; choosing the advertised address automatically", "peer.interface", a.Cfg.Peer.Interface)
	}
	if adv.Fallback {
		a.Log.Warn("advertising the default-route address, which may not be the WireGuard one; set peer.addr or peer.interface if peers cannot reach it", "addr", addr)
		return
	}
	a.Log.Info("advertising sync address", "addr", addr, "chosen_by", adv.Source)
}

// sweepPartials removes interrupted incoming puts too old to be resumed.
func (a *App) sweepPartials() {
	if a.Sync.Partials == nil {
		return
	}
	if n, err := a.Sync.Partials.Sweep(time.Now()); err != nil {
		a.Log.Warn("sweep partial uploads", "err", err)
	} else if n > 0 {
		a.Log.Info("removed stale partial uploads", "count", n)
	}
}

// sweepOrphans removes the staging files and in-root transfer temporaries
// a killed process left behind. It runs before any transfer starts.
func (a *App) sweepOrphans() {
	if a.Sync.Partials != nil {
		if n, err := a.Sync.Partials.SweepStaged(); err != nil {
			a.Log.Warn("sweep staged transfers", "err", err)
		} else if n > 0 {
			a.Log.Info("removed orphaned staged transfers", "count", n)
		}
	}
	for _, root := range a.Reconciler.Roots {
		if n, err := fswatch.SweepTemps(root, time.Nanosecond, a.Reconciler.SyncHidden[root]); err != nil {
			a.Log.Warn("sweep transfer temporaries", "root", root, "err", err)
		} else if n > 0 {
			a.Log.Info("removed orphaned transfer temporaries", "root", root, "count", n)
		}
	}
}

// roundSet tracks the sync rounds Serve has running, so a shutdown can wait
// for them to drain. Once stopped it starts no more.
type roundSet struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	stopped bool
}

// start reports whether a round may start, counting it if so.
func (r *roundSet) start() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return false
	}
	r.wg.Add(1)
	return true
}

func (r *roundSet) done() { r.wg.Done() }

// stop refuses further rounds and returns a channel closed once the running
// ones have returned.
func (r *roundSet) stop() <-chan struct{} {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	idle := make(chan struct{})
	go func() { r.wg.Wait(); close(idle) }()
	return idle
}

func (a *App) setNextSync(at time.Time, every time.Duration) {
	a.mu.Lock()
	a.nextSyncAt, a.syncEvery = at, every
	a.mu.Unlock()
}

func (a *App) setWatcher(w *fswatch.Watcher) {
	a.mu.Lock()
	a.watcher = w
	a.mu.Unlock()
}

// WatchStats reports the running daemon's filesystem watcher, or nil when
// this process is not serving.
func (a *App) WatchStats() *fswatch.Stats {
	a.mu.Lock()
	w := a.watcher
	a.mu.Unlock()
	if w == nil {
		return nil
	}
	st := w.Stats()
	return &st
}

func (a *App) setServing(v bool) {
	a.mu.Lock()
	a.serving = v
	a.mu.Unlock()
}

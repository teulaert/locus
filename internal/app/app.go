// Package app wires the four subsystems of a Locus peer — presence,
// synchronization, local indexing/search and the HTTP API — around the same
// service layer the CLI uses.
package app

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/jobs"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/pipeline"
	"github.com/teulaert/locus/internal/reconcile"
	"github.com/teulaert/locus/internal/search"
	"github.com/teulaert/locus/internal/store"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

// App is the service layer shared by the CLI and the HTTP API.
type App struct {
	Cfg        *config.Config
	Log        *slog.Logger
	Store      *store.Store
	Identity   *identity.Identity
	Hostname   string
	Queue      *jobs.Queue
	Embedder   embed.Embedder
	Device     embed.Device
	Gate       *pipeline.Gate
	Indexer    *pipeline.Pipeline
	Reconciler *reconcile.Reconciler
	Search     *search.Engine
	Sync       *syncpkg.Engine
	Presence   *peer.Table
	Peers      []string // discovery targets (host:port)

	mu       sync.Mutex
	lastSync *syncpkg.Stats
	// peerBackoff suppresses rounds with a peer we cannot reach for transfer,
	// so a firewalled peer does not cost a connect timeout every interval.
	peerBackoff map[string]time.Time
	dataDir     string
}

// New opens the peer's databases and constructs services.
func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	roots, err := cfg.ResolvedRoots()
	if err != nil {
		return nil, err
	}
	dataDir, err := cfg.ResolveDataDir()
	if err != nil {
		return nil, err
	}
	st, err := store.Open(dataDir)
	if err != nil {
		return nil, err
	}
	id, err := identity.Load(dataDir)
	if err != nil {
		return nil, err
	}
	host, _ := os.Hostname()
	embedder, device := embed.Resolve(cfg.Embedding.Device)
	q := jobs.NewQueue(st.Index)
	gate := &pipeline.Gate{}
	app := &App{
		Cfg:         cfg,
		Log:         log,
		Store:       st,
		Identity:    id,
		Hostname:    host,
		Queue:       q,
		Embedder:    embedder,
		Device:      device,
		Gate:        gate,
		Presence:    peer.NewTable(30 * time.Second),
		peerBackoff: map[string]time.Time{},
	}
	app.Indexer = &pipeline.Pipeline{Store: st, Queue: q, Embedder: embedder, Gate: gate, Log: log}
	app.Reconciler = &reconcile.Reconciler{Store: st, Queue: q, Identity: id, Roots: roots, Log: log, TombstoneAge: 30 * 24 * time.Hour, TempAge: time.Hour}
	app.Search = search.NewEngine(st.Index, embedder, gate)
	app.Sync = &syncpkg.Engine{
		Store:     st,
		Identity:  id,
		Roots:     roots,
		MaxFile:   cfg.UploadMaxBytes,
		Hostnames: app.hostnames,
		Log:       log,
		Notifier:  app,
	}
	// A peer that answers discovery probes is not necessarily dialable: record
	// transport outcomes so status can say so, and back off while unreachable.
	app.Sync.OnPeerError = func(peerID string, err error) {
		n := app.Presence.MarkTransportFailure(peerID, err.Error())
		delay := peerRetryDelay(n)
		app.mu.Lock()
		app.peerBackoff[peerID] = time.Now().Add(delay)
		app.mu.Unlock()
		log.Warn("peer unreachable for transfer", "peer", identity.Short(peerID), "failures", n, "retry_in", delay.String(), "err", err)
	}
	app.Sync.OnPeerSuccess = func(peerID string) {
		app.Presence.ClearTransportFailures(peerID)
		app.mu.Lock()
		delete(app.peerBackoff, peerID)
		app.mu.Unlock()
	}
	app.Peers = app.discoveryTargets(roots)

	// Crash recovery: jobs interrupted by a previous run resume immediately,
	// and a changed embedding model marks stored embeddings stale.
	if n, err := app.Queue.RequeueStale(context.Background()); err == nil && n > 0 {
		log.Info("resumed interrupted index work", "jobs", n)
	}
	if cur, err := app.Store.MetaGet(app.Store.Index, metaEmbeddingModel); err == nil && cur != "" && cur != embedder.ID() {
		if n, err := app.Store.MarkEmbeddingsStale(embedder.ID()); err == nil {
			log.Info("embedding model changed; embeddings marked stale", "count", n, "old", cur, "new", embedder.ID())
		}
	}
	_ = app.Store.MetaSet(app.Store.Index, metaEmbeddingModel, embedder.ID())
	return app, nil
}

func (a *App) discoveryTargets(roots []string) []string {
	_ = roots
	targets := append([]string{}, a.Cfg.Discovery.Peers...)
	if iface := os.Getenv("LOCUS_WG_INTERFACE"); iface != "" {
		if addrs, err := discovery.WireGuardPeers(iface); err == nil {
			targets = append(targets, addrs...)
		}
	} else if ifaces := discovery.LocalWireGuardInterfaces(); len(ifaces) > 0 {
		for _, iface := range ifaces {
			if addrs, err := discovery.WireGuardPeers(iface); err == nil {
				targets = append(targets, addrs...)
			}
		}
	}
	seen := map[string]bool{}
	var out []string
	for _, t := range targets {
		if t == "" {
			continue
		}
		// Normalize bare hosts to the default discovery port.
		if _, _, err := net.SplitHostPort(t); err != nil {
			t = net.JoinHostPort(t, fmt.Sprint(discovery.DiscoveryPortDefault))
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// DataDir returns the peer data directory.
func (a *App) DataDir() string {
	dir, _ := a.Cfg.ResolveDataDir()
	return dir
}

// Close releases databases.
func (a *App) Close() error { return a.Store.Close() }

// NotifyChange implements sync.ChangeNotifier: files changed by
// synchronization enter the normal index pipeline.
func (a *App) NotifyChange(root, relpath string, deleted bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if deleted {
		_ = a.Queue.Enqueue(ctx, jobs.KindUnindex, root, relpath, "", 5)
		return
	}
	hash, _, err := fswatch.HashFile(filepath.Join(root, filepath.FromSlash(relpath)))
	if err != nil {
		return
	}
	_ = a.Queue.Enqueue(ctx, jobs.KindIndex, root, relpath, hash, 0)
}

// PeerInfo returns this peer's discovery announcement. Receivers take only
// the port of SyncAddr; the host is replaced by the address the probe or
// reply travelled over (discovery.BindSyncAddr), so the default-route guess
// below need not be the WireGuard address.
func (a *App) PeerInfo() peer.Info {
	return peer.Info{
		PeerID:       a.Identity.PeerID,
		Hostname:     a.Hostname,
		Version:      syncpkg.BuildVersion,
		SyncAddr:     net.JoinHostPort(a.syncHost(), fmt.Sprint(a.Cfg.Peer.Port)),
		Capabilities: []string{"sync", "search", "api"},
	}
}

func (a *App) syncHost() string {
	if a.Cfg.Peer.Addr == "0.0.0.0" {
		return outboundIP()
	}
	return a.Cfg.Peer.Addr
}

func outboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if ta, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return ta.IP.String()
	}
	return "127.0.0.1"
}

func (a *App) hostnames() map[string]string {
	out := map[string]string{}
	// Include this peer: a conflict whose losing side is our own version is
	// named after our hostname, and presence only holds remote peers.
	if a.Hostname != "" {
		out[a.Identity.PeerID] = a.Hostname
	}
	for _, e := range a.Presence.Snapshot() {
		if e.Hostname != "" {
			out[e.PeerID] = e.Hostname
		}
	}
	return out
}

// Serve runs the peer daemon until ctx is cancelled: discovery, presence,
// filesystem watching and reconciliation, index workers, synchronization and
// the HTTP services.
// ServeOption configures Serve.
type ServeOption func(*serveOptions)

type serveOptions struct {
	APIHandler http.Handler
}

// WithAPI mounts an HTTP handler (typically the versioned API + embedded PWA).
func WithAPI(h http.Handler) ServeOption { return func(o *serveOptions) { o.APIHandler = h } }

func (a *App) Serve(ctx context.Context, opts ...ServeOption) error {
	so := &serveOptions{}
	for _, o := range opts {
		o(so)
	}
	if err := fswatch.EnsureRoots(a.Reconciler.Roots); err != nil {
		return err
	}
	a.Log.Info("locus peer starting", "peer", identity.Short(a.Identity.PeerID), "roots", len(a.Reconciler.Roots))

	// Discovery responder + prober.
	discSrv := &discovery.Server{Port: a.Cfg.Discovery.Port, Identity: a.Identity, Info: a.PeerInfo(), Authorized: a.Cfg.AuthorizedPeers}
	discSrv.OnProbe = func(addr string, p *discovery.Packet) {
		a.Presence.Observe(addr, p.Info, p.Protocol, p.Protocol == discovery.Protocol, "")
	}
	if err := discSrv.ListenAndServe(ctx); err != nil {
		a.Log.Warn("discovery responder unavailable", "err", err)
	}
	prober := &discovery.Prober{
		Identity:   a.Identity,
		Info:       a.PeerInfo(),
		Table:      a.Presence,
		Targets:    a.Peers,
		Authorized: a.Cfg.AuthorizedPeers,
		Interval:   a.Cfg.Discovery.Interval.D(),
	}
	go prober.Run(ctx)

	// Peer synchronization service.
	syncSrv := &syncpkg.Server{Engine: a.Sync, Identity: a.Identity, Roots: a.Reconciler.Roots, AuthorizedPeers: a.Cfg.AuthorizedPeers, Known: a.Presence.Known, Log: a.Log}
	peerLn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", a.Cfg.Peer.Addr, a.Cfg.Peer.Port))
	if err != nil {
		return fmt.Errorf("listen peer port: %w", err)
	}
	peerSrv := &http.Server{Handler: syncSrv.Handler(), ReadTimeout: 5 * time.Minute}
	go func() {
		if err := peerSrv.Serve(peerLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Log.Error("peer service", "err", err)
		}
	}()

	// API service.
	var apiSrv *http.Server
	var apiLn net.Listener
	if so.APIHandler != nil {
		apiLn, err = net.Listen("tcp", fmt.Sprintf("%s:%d", a.Cfg.API.Addr, a.Cfg.API.Port))
		if err != nil {
			peerSrv.Close()
			return fmt.Errorf("listen api port: %w", err)
		}
		apiSrv = &http.Server{Handler: so.APIHandler, ReadTimeout: 2 * time.Minute}
		go func() {
			if err := apiSrv.Serve(apiLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				a.Log.Error("api service", "err", err)
			}
		}()
	}

	// Index workers.
	go a.Indexer.Run(ctx, a.Cfg.Workers)

	// Reconciliation: watcher hints plus a periodic safety net.
	watcher := fswatch.NewWatcher(a.Reconciler.Roots, 300*time.Millisecond, func(reason string) {
		a.reconcileOnce(ctx)
	})
	if err := watcher.Start(); err != nil {
		a.Log.Warn("filesystem watcher unavailable; relying on periodic reconciliation", "err", err)
		watcher = nil
	} else {
		defer watcher.Stop()
	}
	a.reconcileOnce(ctx)

	syncTicker := time.NewTicker(a.Cfg.Sync.Interval.D())
	defer syncTicker.Stop()
	reconcileTicker := time.NewTicker(a.Cfg.ReconcileInterval.D())
	defer reconcileTicker.Stop()
	presenceTicker := time.NewTicker(5 * time.Second)
	defer presenceTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.persistPresence()
			if apiSrv != nil {
				apiSrv.Shutdown(context.Background())
			}
			peerSrv.Shutdown(context.Background())
			return nil
		case <-reconcileTicker.C:
			a.reconcileOnce(ctx)
		case <-syncTicker.C:
			a.syncOnce(ctx)
		case <-presenceTicker.C:
			a.persistPresence()
			if n, err := a.Indexer.RequeueStaleEmbeddings(ctx); err == nil && n > 0 {
				a.Log.Info("queued regeneration of stale embeddings", "docs", n)
			}
		}
	}
}

func (a *App) reconcileOnce(ctx context.Context) {
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

// peerRetryDelay grows exponentially while a peer stays unreachable, and is
// capped so a peer that comes back is noticed within a few minutes.
func peerRetryDelay(failures int) time.Duration {
	const (
		base  = 15 * time.Second
		limit = 5 * time.Minute
	)
	if failures < 1 {
		failures = 1
	}
	d := base << uint(failures-1)
	if d <= 0 || d > limit {
		return limit
	}
	return d
}

// dueForSync drops peers still inside their transport-backoff window.
func (a *App) dueForSync(peers []*peer.Entry) []*peer.Entry {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.peerBackoff) == 0 {
		return peers
	}
	now := time.Now()
	out := make([]*peer.Entry, 0, len(peers))
	for _, e := range peers {
		if until, ok := a.peerBackoff[e.PeerID]; ok && now.Before(until) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func (a *App) syncOnce(ctx context.Context) {
	online := a.dueForSync(a.Presence.Online())
	if len(online) == 0 {
		return
	}
	for _, e := range online {
		a.Presence.SetState(e.PeerID, peer.Synchronizing)
	}
	st, err := a.Sync.Round(ctx, online)
	if err != nil {
		a.Log.Warn("sync round", "err", err)
		return
	}
	a.mu.Lock()
	a.lastSync = st
	a.mu.Unlock()
	for _, e := range online {
		a.Presence.SetState(e.PeerID, peer.Online)
	}
	if st.Pushed+st.Pulled+st.Deleted+st.Conflicted > 0 {
		a.Log.Info("sync round", "peers", st.Peers, "pushed", st.Pushed, "pulled", st.Pulled, "deleted", st.Deleted, "conflicted", st.Conflicted)
	}
	for _, e := range st.Errors {
		a.Log.Warn("sync round error", "detail", e)
	}
}

// SyncNow reconciles local state and immediately exchanges changes with every
// peer that answers discovery, including peers learned only from the static
// configuration.
func (a *App) SyncNow(ctx context.Context) (*syncpkg.Stats, error) {
	if _, err := a.Reconciler.Pass(ctx); err != nil {
		return nil, err
	}
	a.probeOnce(ctx)
	online := a.Presence.Online()
	st, err := a.Sync.Round(ctx, online)
	if err != nil {
		return st, err
	}
	a.mu.Lock()
	a.lastSync = st
	a.mu.Unlock()
	a.persistPresence()
	return st, nil
}

// probeOnce runs a single discovery round and returns peers considered online.
func (a *App) probeOnce(ctx context.Context) []*peer.Entry {
	prober := &discovery.Prober{
		Identity:   a.Identity,
		Info:       a.PeerInfo(),
		Table:      a.Presence,
		Targets:    a.Peers,
		Authorized: a.Cfg.AuthorizedPeers,
		Timeout:    2 * time.Second,
	}
	prober.RoundOnce(ctx)
	return a.Presence.Online()
}

func (a *App) persistPresence() {
	b, err := json.Marshal(a.Presence.Snapshot())
	if err != nil {
		return
	}
	_ = a.Store.MetaSet(a.Store.State, metaPresence, string(b))
}

// metaEmbeddingModel records the model that produced current embeddings.
const metaEmbeddingModel = "embedding.model"

// metaPresence is the state-meta key holding the last presence snapshot.
const metaPresence = "presence.snapshot"

// PresenceSnapshot returns live presence when available, falling back to the
// snapshot persisted by a running daemon.
func (a *App) PresenceSnapshot(ctx context.Context) []*peer.Entry {
	if len(a.Peers) > 0 {
		if online := a.probeOnce(ctx); len(online) > 0 || len(a.Presence.Snapshot()) > 0 {
			return a.Presence.Snapshot()
		}
	}
	saved, err := a.Store.MetaGet(a.Store.State, metaPresence)
	if err != nil || saved == "" {
		return nil
	}
	var entries []*peer.Entry
	if err := json.Unmarshal([]byte(saved), &entries); err != nil {
		return nil
	}
	return entries
}

// LastSync returns the statistics of the most recent round in this process.
func (a *App) LastSync() *syncpkg.Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastSync
}

// RebuildIndex deletes all derived search state and reconstructs it from the
// local files. Synchronization state is untouched.
func (a *App) RebuildIndex(ctx context.Context) error {
	// Reconcile first so the state database reflects what is on disk; the
	// rebuild is then a purely derived operation.
	if _, err := a.Reconciler.Pass(ctx); err != nil {
		return err
	}
	if err := a.Store.DropDerived(); err != nil {
		return err
	}
	roots := a.Reconciler.Roots
	// Queue every currently present file for re-indexing directly from disk.
	for _, root := range roots {
		entries, err := fswatch.Scan(root)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := a.Queue.Enqueue(ctx, jobs.KindIndex, root, e.RelPath, e.Hash, 1); err != nil {
				return err
			}
		}
	}
	if err := a.Store.MetaSet(a.Store.Index, metaEmbeddingModel, a.Embedder.ID()); err != nil {
		return err
	}
	// Drain inline so the command reports a completed rebuild.
	return a.Drain(ctx, 0)
}

// Drain processes queued work inline until the queue is empty (or timeout).
func (a *App) Drain(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	if timeout <= 0 {
		deadline = time.Now().Add(24 * time.Hour)
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := a.Queue.PendingCount(ctx)
		if err != nil {
			return err
		}
		if n == 0 || time.Now().After(deadline) {
			return nil
		}
		job, err := a.Queue.Claim(ctx)
		if err != nil {
			return err
		}
		if job == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err := a.Indexer.Process(ctx, job); err != nil {
			_ = a.Queue.Fail(ctx, job, err)
			continue
		}
		_ = a.Queue.Complete(ctx, job)
	}
}

// Roots returns the configured, absolute roots.
func (a *App) Roots() []string { return a.Reconciler.Roots }

// Hostnames maps known peer IDs to hostnames (used by conflict naming and UI).
func (a *App) Hostnames() map[string]string { return a.hostnames() }

// Status aggregates everything the CLI and API report.
type Status struct {
	PeerID    string                    `json:"peer_id"`
	Hostname  string                    `json:"hostname"`
	Version   string                    `json:"version"`
	DataDir   string                    `json:"data_dir"`
	Roots     []RootStatus              `json:"roots"`
	Index     *store.IndexStats         `json:"index"`
	Jobs      map[string]map[string]int `json:"jobs"`
	Embedding embed.Device              `json:"embedding"`
	Peers     []*peer.Entry             `json:"peers"`
	Conflicts int                       `json:"conflicts"`
	LastSync  *syncpkg.Stats            `json:"last_sync,omitempty"`
}

// RootStatus is per-root synchronization state.
type RootStatus struct {
	Path      string `json:"path"`
	Files     int    `json:"files"`
	Deleted   int    `json:"deleted"`
	SyncState string `json:"sync_state"`
}

// BuildStatus collects status for display.
// ensureMap keeps documented JSON collections as {} instead of null.
func ensureMap(m map[string]map[string]int) map[string]map[string]int {
	if m == nil {
		return map[string]map[string]int{}
	}
	return m
}

// ensureSlice keeps documented JSON collections as [] instead of null.
func ensureSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func (a *App) BuildStatus(ctx context.Context) (*Status, error) {
	live, deleted := 0, 0
	var rootStats []RootStatus
	for _, r := range a.Roots() {
		l, d, err := a.Store.CountDocs(r)
		if err != nil {
			return nil, err
		}
		live += l
		deleted += d
		state := "idle"
		if a.Sync.Busy() {
			state = "synchronizing"
		}
		rootStats = append(rootStats, RootStatus{Path: r, Files: l, Deleted: d, SyncState: state})
	}
	idx, err := a.Store.GetIndexStats(a.Embedder.ID())
	if err != nil {
		return nil, err
	}
	jobs, err := a.Queue.Stats(ctx)
	if err != nil {
		return nil, err
	}
	conflicts, err := a.Store.ListConflicts()
	if err != nil {
		return nil, err
	}
	return &Status{
		PeerID:    a.Identity.PeerID,
		Hostname:  a.Hostname,
		Version:   syncpkg.BuildVersion,
		DataDir:   a.DataDir(),
		Roots:     ensureSlice(rootStats),
		Index:     idx,
		Jobs:      ensureMap(jobs),
		Embedding: a.Device,
		Peers:     ensureSlice(a.PresenceSnapshot(ctx)),
		Conflicts: len(conflicts),
		LastSync:  a.LastSync(),
	}, nil
}

// APIAddr returns the address the HTTP API listens on when serving.
func (a *App) APIAddr() string {
	return fmt.Sprintf("%s:%d", a.Cfg.API.Addr, a.Cfg.API.Port)
}

// APIToken returns the configured API token, or else the on-disk secret in
// <data_dir>/api_token, generating and persisting it on first use. It never
// returns an empty token without an error: an empty token would match a
// request with no Authorization header. A token that cannot be persisted is an
// error too, since the user would have nothing to read and every later call
// would mint a different one.
func (a *App) APIToken() (string, error) {
	if a.Cfg.API.Token != "" {
		return a.Cfg.API.Token, nil
	}
	path := filepath.Join(a.DataDir(), "api_token")
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("read api token: %w", err)
	}
	if tok := strings.TrimSpace(string(b)); tok != "" {
		return tok, nil
	}
	secret := make([]byte, 32)
	if _, err := randRead(secret); err != nil {
		return "", fmt.Errorf("generate api token: %w", err)
	}
	tok := fmt.Sprintf("%x", secret)
	if err := os.WriteFile(path, []byte(tok), 0o600); err != nil {
		return "", fmt.Errorf("write api token: %w", err)
	}
	return tok, nil
}

// randRead is a variable so tests can simulate an entropy failure.
var randRead = crand.Read

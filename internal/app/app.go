// Package app wires the four subsystems of a Locus peer — presence,
// synchronization, local indexing/search and the HTTP API — around the same
// service layer the CLI uses.
package app

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/embed"
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
	// Rejections aggregates refused peer and API requests; MaintainAudit
	// writes it to the audit log as one row per source.
	Rejections *store.AuditTally

	mu       sync.Mutex
	lastSync *syncpkg.Stats
	// serving is true while Serve holds the daemon lock, so Status can say
	// that it describes a running daemon (LOC-57).
	serving bool
	// peerBackoff suppresses rounds with a peer we cannot reach for transfer,
	// so a firewalled peer does not cost a connect timeout every interval.
	peerBackoff map[string]time.Time
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
	rootNames, err := cfg.RootNames()
	if err != nil {
		return nil, err
	}
	syncHidden, err := cfg.HiddenRoots()
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
		Rejections:  store.NewAuditTally(),
		peerBackoff: map[string]time.Time{},
	}
	app.Indexer = &pipeline.Pipeline{Store: st, Queue: q, Embedder: embedder, Gate: gate, Log: log}
	app.Reconciler = &reconcile.Reconciler{Store: st, Queue: q, Identity: id, Roots: roots, SyncHidden: syncHidden, Log: log, TombstoneAge: cfg.TombstoneAge.D(), TempAge: time.Hour, VerifyEvery: 24 * time.Hour}
	// Search reads through its own connections, so it never queues behind
	// an index worker's write transaction on the single writer connection.
	app.Search = search.NewEngine(st.Read, embedder, gate)
	app.Sync = &syncpkg.Engine{
		Store:      st,
		Identity:   id,
		Roots:      roots,
		RootNames:  rootNames,
		SyncHidden: syncHidden,
		MaxFile:    cfg.UploadMaxBytes,
		Partials:   &syncpkg.PartialStore{Dir: filepath.Join(dataDir, "partials"), MaxAge: syncpkg.DefaultPartialMaxAge},
		Hostnames:  app.hostnames,
		Log:        log,
		Notifier:   app,
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
	// A peer that refuses transfer for a protocol mismatch answered: it is
	// incompatible, not unreachable, so it leaves the rounds instead of being
	// retried with backoff, and status says why.
	app.Sync.OnPeerIncompatible = func(peerID string, err *syncpkg.IncompatibleError) {
		app.Presence.MarkIncompatible(peerID, err.PeerProtocol, err.Error())
		app.mu.Lock()
		delete(app.peerBackoff, peerID)
		app.mu.Unlock()
		log.Warn("peer speaks an incompatible sync protocol; not synchronizing with it", "peer", identity.Short(peerID), "err", err)
	}
	app.Sync.OnPeerSuccess = func(peerID string) {
		app.Presence.ClearTransportFailures(peerID)
		app.mu.Lock()
		delete(app.peerBackoff, peerID)
		app.mu.Unlock()
	}
	app.Peers = app.discoveryTargets()

	// A changed embedding model marks stored embeddings stale. Jobs left
	// running by a crashed process are resumed by Pipeline.Run when the
	// workers start, not here: every CLI command builds an App, and doing it
	// here would hand jobs a live daemon is running back to the queue.
	if cur, err := app.Store.MetaGet(app.Store.Index, metaEmbeddingModel); err == nil && cur != "" && cur != embedder.ID() {
		if n, err := app.Store.MarkEmbeddingsStale(embedder.ID()); err == nil {
			log.Info("embedding model changed; embeddings marked stale", "count", n, "old", cur, "new", embedder.ID())
		}
	}
	_ = app.Store.MetaSet(app.Store.Index, metaEmbeddingModel, embedder.ID())
	return app, nil
}

// DataDir returns the peer data directory.
func (a *App) DataDir() string {
	dir, _ := a.Cfg.ResolveDataDir()
	return dir
}

// Close releases databases.
func (a *App) Close() error { return a.Store.Close() }

// metaEmbeddingModel records the model that produced current embeddings.
const metaEmbeddingModel = "embedding.model"

// Roots returns the configured, absolute roots.
func (a *App) Roots() []string { return a.Reconciler.Roots }

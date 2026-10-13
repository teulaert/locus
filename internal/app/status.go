package app

import (
	"context"

	"github.com/teulaert/locus/internal/embed"
	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/store"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

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
	// Progress describes the peers a round is synchronizing with right
	// now (LOC-66); empty when none is running.
	Progress []syncpkg.PeerProgress `json:"progress"`
	// Watch is the daemon's filesystem watch mode and count (LOC-70); nil
	// when no daemon is running.
	Watch *fswatch.Stats `json:"watch,omitempty"`
	// Daemon is true when the status comes from a running `locus serve`
	// (directly over the API, or relayed by the CLI), false when it is a
	// snapshot read from the databases by a CLI with no daemon running.
	Daemon bool `json:"daemon"`
	// Rendezvous is true for a discovery-only peer with no roots (LOC-76).
	Rendezvous bool `json:"rendezvous,omitempty"`
}

// RootStatus is per-root synchronization state. Name is the root's identity
// across peers: the configured name, or else the basename of Path.
type RootStatus struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	Files     int    `json:"files"`
	Deleted   int    `json:"deleted"`
	SyncState string `json:"sync_state"`
}

// BuildStatus collects status for display. Roots, jobs and peers are never
// nil, so they encode as [] and {} rather than null.
func (a *App) BuildStatus(ctx context.Context) (*Status, error) {
	live, deleted := 0, 0
	rootStats := make([]RootStatus, 0, len(a.Roots()))
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
		rootStats = append(rootStats, RootStatus{Path: r, Name: a.Sync.NameOf(r), Files: l, Deleted: d, SyncState: state})
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
		Roots:     rootStats,
		Index:     idx,
		Jobs:      nonNilJobs(jobs),
		Embedding: a.Device,
		Peers:     a.PresenceSnapshot(ctx),
		Conflicts: len(conflicts),
		LastSync:  a.LastSync(),
		Progress:  a.Sync.Progress(),
		Watch:     a.WatchStats(),
		Daemon:    a.isServing(),

		Rendezvous: a.Cfg.Rendezvous,
	}, nil
}

func (a *App) isServing() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.serving
}

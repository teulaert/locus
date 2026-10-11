package app

import (
	"context"
	"errors"

	syncpkg "github.com/teulaert/locus/internal/sync"
)

// syncOnce runs a round with every online peer that is due: not inside its
// transport backoff and not already in a round. The daemon calls it in a
// goroutine of its own per tick, so a round still transferring to one peer
// does not keep the next tick from synchronizing with the others (LOC-61).
// The engine marks each peer synchronizing for as long as its part of the
// round runs (OnPeerStart/OnPeerDone).
func (a *App) syncOnce(ctx context.Context) {
	online := a.dueForSync(a.Presence.Online())
	if len(online) == 0 {
		return
	}
	st, err := a.Sync.Round(ctx, online)
	if errors.Is(err, syncpkg.ErrRoundRunning) {
		// Another round claimed these peers in between; it covers them.
		a.Log.Debug("sync round", "err", err)
		return
	}
	if err != nil {
		a.Log.Warn("sync round", "err", err)
		return
	}
	a.mu.Lock()
	a.lastSync = st
	a.mu.Unlock()
	if st.Pushed+st.Pulled+st.Deleted+st.Conflicted > 0 {
		a.Log.Info("sync round", "peers", st.Peers, "pushed", st.Pushed, "pulled", st.Pulled, "deleted", st.Deleted, "conflicted", st.Conflicted)
	}
	for _, e := range st.Errors {
		a.Log.Warn("sync round error", "detail", e)
	}
}

// SyncNow reconciles local state and immediately exchanges changes with every
// peer that answers discovery, including peers learned only from the static
// configuration. A peer a round is already synchronizing with is left to
// that round and listed in Stats.Busy.
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

// LastSync returns the statistics of the most recently finished round in
// this process.
func (a *App) LastSync() *syncpkg.Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastSync
}

// SyncState is the synchronization state the CLI's `sync status` and the
// API's GET /api/v1/sync report (LOC-66).
type SyncState struct {
	Syncing  bool                   `json:"syncing"`
	Progress []syncpkg.PeerProgress `json:"progress"`
	LastSync *syncpkg.Stats         `json:"last_sync"`
}

// SyncStatus reports the rounds running now, per peer, and the last one.
func (a *App) SyncStatus() *SyncState {
	p := a.Sync.Progress()
	return &SyncState{Syncing: len(p) > 0, Progress: p, LastSync: a.LastSync()}
}

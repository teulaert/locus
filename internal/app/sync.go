package app

import (
	"context"

	"github.com/teulaert/locus/internal/peer"
	syncpkg "github.com/teulaert/locus/internal/sync"
)

func (a *App) syncOnce(ctx context.Context) {
	online := a.dueForSync(a.Presence.Online())
	if len(online) == 0 {
		return
	}
	for _, e := range online {
		a.Presence.SetState(e.PeerID, peer.Synchronizing)
	}
	// Every exit must clear the state: Round refuses to start while another
	// round (sync now, POST /api/v1/sync) is in flight, and presence probes
	// deliberately leave a synchronizing peer alone, so an early return would
	// keep it reported as synchronizing until the next successful tick.
	defer func() {
		for _, e := range online {
			a.Presence.SetState(e.PeerID, peer.Online)
		}
	}()
	st, err := a.Sync.Round(ctx, online)
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

// LastSync returns the statistics of the most recent round in this process.
func (a *App) LastSync() *syncpkg.Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastSync
}

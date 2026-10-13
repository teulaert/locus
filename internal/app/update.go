package app

import (
	"context"
	"errors"

	"github.com/teulaert/locus/internal/netguard"
	"github.com/teulaert/locus/internal/update"
)

// ErrNotServing is an update requested of a process that is not the
// running daemon.
var ErrNotServing = errors.New("not serving: only the running daemon installs updates through the API")

// UpdateNow does what `locus update` asks of the running daemon (POST
// /api/v1/update): a check, an install or a rollback. After an install or
// a rollback the daemon shuts down as on SIGTERM and re-executes the new
// binary once the response has been sent: the HTTP shutdown waits for the
// request in flight.
func (a *App) UpdateNow(ctx context.Context, req update.Request) (*update.Report, error) {
	if !a.isServing() {
		return nil, ErrNotServing
	}
	rep, err := a.Update.Manual(ctx, req)
	if err != nil {
		return nil, err
	}
	if rep.Installed || rep.RolledBack {
		rep.Restarting = true
		a.Update.MarkRestarting()
		a.requestRestart()
	}
	return rep, nil
}

// requestRestart stops the running Serve and marks the stop as a restart:
// `locus serve` then re-executes its binary.
func (a *App) requestRestart() {
	a.mu.Lock()
	a.restart = true
	stop := a.stopServe
	a.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// RestartRequested reports whether Serve returned so that the daemon
// restarts into a newly installed binary.
func (a *App) RestartRequested() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.restart
}

// runUpdates is the daemon's update loop. A test run never polls GitHub
// (LOC-80): under the network guard only a configured update.url, which
// tests point at a loopback server, is checked.
func (a *App) runUpdates(ctx context.Context) {
	if netguard.Active() && a.Cfg.Update.URL == "" {
		return
	}
	a.Update.Run(ctx, a.requestRestart)
}

// markServing clears a pending update health marker: the running binary
// has reached serving, so it is not rolled back.
func (a *App) markServing() {
	if update.MarkServing(a.DataDir()) {
		a.Log.Info("update: new build is serving", "version", a.Update.Status().Running)
	}
}

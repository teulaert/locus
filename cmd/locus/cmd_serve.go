package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/teulaert/locus/internal/api"
	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/memlimit"
	syncpkg "github.com/teulaert/locus/internal/sync"
	"github.com/teulaert/locus/internal/update"
)

func cmdServe(ctx context.Context, g *globalOptions, args []string, log *slog.Logger) int {
	cfg, err := loadConfig(g)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	// Before anything allocates: the limit paces the GC from the start.
	memlimit.Apply(cfg.MemoryLimit, log)
	// Before anything else opens the data directory: a new build that
	// keeps failing to start is rolled back here (LOC-86).
	if updateHealthCheck(cfg, log) {
		return restartInto(log)
	}
	a, err := app.New(cfg, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	h, err := api.Handler(a)
	if err != nil {
		a.Close()
		fmt.Fprintln(os.Stderr, "locus: serve:", err)
		return exitError
	}
	err = a.Serve(ctx, app.WithAPI(h))
	restart := a.RestartRequested()
	a.Close()
	if restart {
		// Shut down like on SIGTERM; now the new binary takes over.
		return restartInto(log)
	}
	return serveExit(err)
}

// updateHealthCheck counts this start against a binary an update just
// installed, and rolls it back when it has failed to reach serving too
// often. It reports whether it rolled back, in which case the caller
// re-executes the restored binary.
func updateHealthCheck(cfg *config.Config, log *slog.Logger) bool {
	dataDir, err := cfg.ResolveDataDir()
	if err != nil || app.CheckDataDirOwner(dataDir) != nil {
		return false // app.New reports it
	}
	exe, err := update.ResolveExecutable()
	if err != nil {
		return false
	}
	rolledBack, err := update.StartupCheck(dataDir, exe, update.Running(syncpkg.BuildVersion), log)
	return err == nil && rolledBack
}

// restartInto re-executes the binary now installed at this executable's
// path, keeping the pid, arguments and environment, so systemd and launchd
// see the service carry on. Where that is impossible it exits 0 and the
// service manager starts the new binary.
func restartInto(log *slog.Logger) int {
	exe, err := update.ResolveExecutable()
	if err == nil && update.CanReexec {
		log.Warn("update: restarting into the binary now installed", "binary", exe)
		err = update.Reexec(exe)
	}
	log.Warn("update: cannot re-execute; exiting so the service manager starts the new binary", "err", err)
	return exitOK
}

// serveExit is the exit code for what Serve returned. A shutdown cut short
// at its deadline has been logged as a warning by Serve; nothing it left
// running can leave state inconsistent, so a restart under load is not a
// failure and exits 0 (LOC-81). Anything else Serve returns (the daemon
// lock is held, a port cannot be bound) is.
func serveExit(err error) int {
	var se *app.ShutdownError
	if err == nil || errors.As(err, &se) {
		return exitOK
	}
	fmt.Fprintln(os.Stderr, "locus: serve:", err)
	return exitError
}

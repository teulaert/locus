package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/teulaert/locus/internal/api"
	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/memlimit"
)

func cmdServe(ctx context.Context, g *globalOptions, args []string, log *slog.Logger) int {
	cfg, err := loadConfig(g)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	// Before anything allocates: the limit paces the GC from the start.
	memlimit.Apply(cfg.MemoryLimit, log)
	a, err := app.New(cfg, log)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
	defer a.Close()
	h, err := api.Handler(a)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus: serve:", err)
		return exitError
	}
	return serveExit(a.Serve(ctx, app.WithAPI(h)))
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

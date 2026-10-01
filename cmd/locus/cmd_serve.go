package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/teulaert/locus/internal/api"
	"github.com/teulaert/locus/internal/app"
)

func cmdServe(ctx context.Context, g *globalOptions, args []string, log *slog.Logger) int {
	cfg, err := loadConfig(g)
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus:", err)
		return exitConfig
	}
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
	if err := a.Serve(ctx, app.WithAPI(h)); err != nil {
		fmt.Fprintln(os.Stderr, "locus: serve:", err)
		return exitError
	}
	return exitOK
}

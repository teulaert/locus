package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/update"
)

// updateTimeout is how long `locus update` may take when --timeout is not
// given: it downloads a binary of some 20 MB.
const updateTimeout = 15 * time.Minute

// cmdUpdate checks for, installs or rolls back a release (LOC-86). With a
// daemon running on the data directory, the daemon does it (POST
// /api/v1/update) and restarts into the new binary; otherwise this process
// swaps the binary and the daemon runs it when it next starts.
func cmdUpdate(ctx context.Context, a *app.App, args []string, g *globalOptions) int {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	check := fs.Bool("check", false, "report whether a newer build is available; install nothing")
	rollback := fs.Bool("rollback", false, "put the previous binary (<binary>.previous) back")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() > 0 || (*check && *rollback) {
		fmt.Fprintln(os.Stderr, "locus: update takes --check or --rollback, not both, and no arguments")
		return exitUsage
	}
	req := update.Request{Check: *check, Rollback: *rollback}
	// A rollback only moves local files; everything else needs the key
	// that verifies releases.
	if !req.Rollback && !a.Update.Enabled() {
		fmt.Fprintln(os.Stderr, "locus: update:", update.ErrNoKey)
		return exitConfig
	}
	var rep update.Report
	viaDaemon := false
	if d := daemonFor(a); d != nil {
		if err := d.doBody(ctx, http.MethodPost, "/api/v1/update", req, &rep); err != nil {
			fmt.Fprintln(os.Stderr, "locus: update:", err)
			if errors.Is(err, errDaemonAPI) {
				fmt.Fprintln(os.Stderr, "locus: a daemon is running on this data directory but its API cannot be reached; it must install the update itself")
			}
			if strings.Contains(err.Error(), update.ErrNoPrevious.Error()) {
				return exitConfig
			}
			return exitError
		}
		viaDaemon = true
	} else {
		r, err := a.Update.Manual(ctx, req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "locus: update:", err)
			if errors.Is(err, update.ErrNoPrevious) {
				return exitConfig
			}
			return exitError
		}
		rep = *r
	}
	if g.json {
		printJSON(rep)
		return exitOK
	}
	fmt.Print(updateText(&rep, viaDaemon))
	return exitOK
}

// updateText is the human output of `locus update`.
func updateText(r *update.Report, viaDaemon bool) string {
	var b strings.Builder
	restartNote := func() {
		switch {
		case r.Restarting:
			b.WriteString("the daemon is restarting into it\n")
		case !viaDaemon:
			b.WriteString("no daemon is running here; it runs this build when it next starts\n")
		}
	}
	switch {
	case r.RolledBack:
		fmt.Fprintf(&b, "rolled back %s to the previous binary (the replaced build is kept as %s)\n", r.Binary, update.PreviousPath(r.Binary))
		restartNote()
	case r.Installed:
		fmt.Fprintf(&b, "installed %s (version %s) at %s, replacing %s (kept as %s)\n", r.Latest, r.LatestVersion, r.Binary, r.Running, update.PreviousPath(r.Binary))
		restartNote()
	case r.Available:
		fmt.Fprintf(&b, "available: %s (version %s); running %s, channel %s\n", r.Latest, r.LatestVersion, r.Running, r.Channel)
		b.WriteString("run: locus update\n")
	case r.Latest != "" && r.Note == "":
		fmt.Fprintf(&b, "up to date: running %s, latest %s on channel %s\n", r.Running, r.Latest, r.Channel)
	default:
		fmt.Fprintf(&b, "running %s, channel %s", r.Running, r.Channel)
		if r.Latest != "" {
			fmt.Fprintf(&b, ", latest %s", r.Latest)
		}
		b.WriteString("\n")
	}
	if r.Note != "" {
		fmt.Fprintf(&b, "note: %s\n", r.Note)
	}
	return b.String()
}

// updateLine is the `update` line of `locus status`.
func updateLine(u *update.Status) string {
	if u == nil {
		return ""
	}
	state := strings.ReplaceAll(u.State, "_", " ")
	if u.Error != "" && (u.State == update.StateFailed || u.State == update.StateRateLimited || u.State == update.StateRolledBack) {
		state += ": " + u.Error
	}
	switch {
	case u.State == update.StateDisabled:
		return "disabled (no release key compiled into this build)"
	case u.Auto:
		s := fmt.Sprintf("auto on, channel %s, running %s", u.Channel, u.Running)
		if u.Latest != "" {
			s += ", latest " + u.Latest
		}
		return s + " (" + state + ")"
	case u.State == update.StateAvailable:
		return fmt.Sprintf("available: %s (auto off; run: locus update)", u.Latest)
	case u.State == update.StateUpToDate:
		return "up to date (auto off)"
	}
	s := fmt.Sprintf("auto off, channel %s, running %s", u.Channel, u.Running)
	if u.Latest != "" {
		s += ", latest " + u.Latest
	}
	return s + " (" + state + ")"
}

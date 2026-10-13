package main

import (
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/update"
)

func TestUpdateStatusLine(t *testing.T) {
	for _, tc := range []struct {
		st   update.Status
		want string
	}{
		{update.Status{Auto: true, Channel: "main", Running: "e8c67fb", Latest: "build-1a2b3c4", State: update.StateDownloading},
			"auto on, channel main, running e8c67fb, latest build-1a2b3c4 (downloading)"},
		{update.Status{Channel: "main", Running: "e8c67fb", Latest: "build-1a2b3c4", State: update.StateAvailable},
			"available: build-1a2b3c4 (auto off; run: locus update)"},
		{update.Status{Channel: "main", Running: "e8c67fb", Latest: "build-e8c67fb", State: update.StateUpToDate},
			"up to date (auto off)"},
		{update.Status{Channel: "stable", Running: "e8c67fb", State: update.StateNotChecked},
			"auto off, channel stable, running e8c67fb (not checked)"},
		{update.Status{Auto: true, Channel: "main", Running: "e8c67fb", State: update.StateFailed, Error: "list releases: 502"},
			"auto on, channel main, running e8c67fb (failed: list releases: 502)"},
		{update.Status{Auto: true, Channel: "main", Running: "e8c67fb", State: update.StateDisabled},
			"disabled (no release key compiled into this build)"},
	} {
		if got := updateLine(&tc.st); got != tc.want {
			t.Errorf("updateLine(%+v)\n got %q\nwant %q", tc.st, got, tc.want)
		}
	}
}

func TestUpdateText(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rep       update.Report
		viaDaemon bool
		want      []string
	}{
		{"available", update.Report{Action: "check", Channel: "main", Running: "e8c67fb", Latest: "build-1a2b3c4", LatestVersion: "1a2b3c4", Available: true},
			false, []string{"available: build-1a2b3c4 (version 1a2b3c4); running e8c67fb, channel main", "run: locus update"}},
		{"up to date", update.Report{Action: "check", Channel: "main", Running: "1a2b3c4", Latest: "build-1a2b3c4"},
			false, []string{"up to date: running 1a2b3c4, latest build-1a2b3c4 on channel main"}},
		{"installed by the daemon", update.Report{Action: "update", Running: "e8c67fb", Latest: "build-1a2b3c4", LatestVersion: "1a2b3c4", Installed: true, Restarting: true, Binary: "/var/lib/locus/bin/locus"},
			true, []string{"installed build-1a2b3c4 (version 1a2b3c4) at /var/lib/locus/bin/locus, replacing e8c67fb (kept as /var/lib/locus/bin/locus.previous)", "the daemon is restarting into it"}},
		{"installed, no daemon", update.Report{Action: "update", Running: "e8c67fb", Latest: "build-1a2b3c4", Installed: true, Binary: "/home/u/.local/bin/locus"},
			false, []string{"no daemon is running here; it runs this build when it next starts"}},
		{"rolled back", update.Report{Action: "rollback", RolledBack: true, Restarting: true, Binary: "/b/locus"},
			true, []string{"rolled back /b/locus to the previous binary (the replaced build is kept as /b/locus.previous)", "the daemon is restarting into it"}},
		{"no release", update.Report{Action: "check", Channel: "stable", Running: "e8c67fb", Note: "no release published on channel stable"},
			false, []string{"running e8c67fb, channel stable", "note: no release published on channel stable"}},
	} {
		got := updateText(&tc.rep, tc.viaDaemon)
		for _, w := range tc.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: output lacks %q:\n%s", tc.name, w, got)
			}
		}
	}
}

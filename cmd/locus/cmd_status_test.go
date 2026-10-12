package main

import (
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
)

// LOC-70: `locus status` says how the daemon watches its roots.
func TestWatchSummary(t *testing.T) {
	every := 30 * time.Second
	cases := []struct {
		st   fswatch.Stats
		want string
	}{
		{fswatch.Stats{Mode: fswatch.ModeInotify, Watches: 120, FDLimit: 524288},
			"inotify, 120 directories"},
		{fswatch.Stats{Mode: fswatch.ModeInotify, Watches: 1},
			"inotify, 1 directory"},
		{fswatch.Stats{Mode: fswatch.ModeKqueueDirs, Watches: 412, MaxWatches: 12288, FDLimit: 24576},
			"kqueue-dirs, 412 directories (falls back to polling at 12288; descriptor limit 24576); in-place edits found by reconciliation every 30s"},
		{fswatch.Stats{Mode: fswatch.ModePoll, Reason: "watching /r/x: too many open files"},
			"poll (changes found by reconciliation every 30s): watching /r/x: too many open files"},
		{fswatch.Stats{Mode: fswatch.ModePoll},
			"poll (changes found by reconciliation every 30s)"},
	}
	for _, c := range cases {
		if got := watchSummary(&c.st, every); got != c.want {
			t.Errorf("watchSummary(%+v) = %q, want %q", c.st, got, c.want)
		}
	}
}

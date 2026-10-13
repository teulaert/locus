package sync

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// LOC-85: a round whose pushes the peer all answers "held" moves no bytes
// but finishes actions, so it is not stalled; the skips are summed per peer
// for the summary line.
func TestTrackerHeldPushesAreNotStalled(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	tr := newTracker("dddd", "hermes", []string{"Docs"}, func() time.Time { return now })
	tr.beginRoot(0)
	tr.planned([]Action{
		{Kind: ActionPushCreate, RelPath: "a", Local: &ManifestEntry{Size: 3000}},
		{Kind: ActionPushCreate, RelPath: "b", Local: &ManifestEntry{Size: 2000}},
		{Kind: ActionPushCreate, RelPath: "c", Local: &ManifestEntry{Size: 1000}},
		{Kind: ActionPullCreate, RelPath: "d", Remote: &ManifestEntry{Size: 500}},
		{Kind: ActionNoop, RelPath: "e"},
	})
	for _, f := range []struct {
		path string
		size int64
		held bool
	}{{"a", 3000, true}, {"b", 2000, false}} {
		now = now.Add(8 * time.Second)
		tr.start("push", f.path)
		tr.skipPush(f.size, f.held)
		tr.finish(ActionPushCreate, false, false)
	}
	now = now.Add(8 * time.Second)
	p := tr.snapshot()
	if p.Stalled {
		t.Fatal("round finishing held pushes reported stalled")
	}
	if p.BytesPerSec != 0 || p.ETASeconds != 0 {
		t.Fatalf("rate %.1f eta %d with no bytes moved, want 0 and none", p.BytesPerSec, p.ETASeconds)
	}
	if p.SkippedHeld != 1 || p.SkippedReceiving != 1 || p.SkippedHeldBytes != 3000 || p.SkippedReceivingBytes != 2000 || p.SkippedPushBytes != 5000 {
		t.Fatalf("skips = %+v", p)
	}
	if p.ActionsPlanned != 4 || p.ActionsDone != 2 || p.FilesPlanned != 4 || p.FilesDone != 2 {
		t.Fatalf("actions %d/%d files %d/%d, want 2/4 and 2/4", p.ActionsDone, p.ActionsPlanned, p.FilesDone, p.FilesPlanned)
	}
	tr.start("pull", "d")
	tr.skip(500)
	tr.finish(ActionPullCreate, false, false)
	if p = tr.snapshot(); p.SkippedInFlight != 1 || p.SkippedBytes != 500 || p.FilesDone != 3 {
		t.Fatalf("after an in-flight pull skip: %+v", p)
	}

	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{`"stalled":false`, `"actions_done":3`, `"actions_planned":4`, `"files_done":3`, `"files_planned":4`,
		`"skipped_held":1`, `"skipped_held_bytes":3000`, `"skipped_receiving_bytes":2000`, `"skipped_push_bytes":5000`, `"skipped_bytes":500`} {
		if !strings.Contains(string(raw), k) {
			t.Errorf("progress JSON lacks %s: %s", k, raw)
		}
	}
}

// A transfer that neither moves bytes nor finishes an action for ten
// seconds is stalled; one that is still planning is not.
func TestTrackerStalled(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	tr := newTracker("dddd", "", []string{"Docs"}, func() time.Time { return now })
	tr.beginRoot(0)
	now = now.Add(time.Minute) // a long manifest exchange is not a stall
	if tr.snapshot().Stalled {
		t.Fatal("stalled while exchanging manifests")
	}
	tr.planned([]Action{
		{Kind: ActionPushCreate, RelPath: "a", Local: &ManifestEntry{Size: 10_000}},
		{Kind: ActionPushCreate, RelPath: "b", Local: &ManifestEntry{Size: 10_000}},
	})
	if tr.snapshot().Stalled {
		t.Fatal("stalled right after planning")
	}
	tr.start("push", "a")
	now = now.Add(5 * time.Second)
	tr.moved("push", 100)
	now = now.Add(9 * time.Second)
	if tr.snapshot().Stalled {
		t.Fatal("stalled nine seconds after the last byte")
	}
	now = now.Add(time.Second)
	if p := tr.snapshot(); !p.Stalled || p.BytesPerSec != 0 {
		t.Fatalf("no progress for ten seconds: stalled %v rate %.1f", p.Stalled, p.BytesPerSec)
	}
	tr.finish(ActionPushCreate, true, false)
	if tr.snapshot().Stalled {
		t.Fatal("stalled right after an action finished")
	}
	tr.finish(ActionPushCreate, true, false)
	now = now.Add(time.Minute)
	if tr.snapshot().Stalled {
		t.Fatal("stalled with every action done")
	}
}

package update

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

func stage(t *testing.T, exe, content string) {
	t.Helper()
	if err := os.WriteFile(StagePath(exe), []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestSwapKeepsPreviousAndRollbackRestores(t *testing.T) {
	exe := installed(t, "v1")
	if err := Swap(exe); err == nil {
		t.Fatal("swap without a staged binary succeeded")
	}
	if _, err := os.Stat(PreviousPath(exe)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a failed swap left a .previous")
	}

	stage(t, exe, "v2")
	if err := Swap(exe); err != nil {
		t.Fatal(err)
	}
	if readFile(t, exe) != "v2" || readFile(t, PreviousPath(exe)) != "v1" {
		t.Fatalf("after swap: exe %q, previous %q", readFile(t, exe), readFile(t, PreviousPath(exe)))
	}
	if _, err := os.Stat(StagePath(exe)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("staged binary left behind")
	}
	if fi, _ := os.Stat(exe); fi.Mode().Perm()&0o100 == 0 {
		t.Fatalf("installed binary is not executable: %v", fi.Mode())
	}

	// A second swap replaces the older .previous.
	stage(t, exe, "v3")
	if err := Swap(exe); err != nil {
		t.Fatal(err)
	}
	if readFile(t, exe) != "v3" || readFile(t, PreviousPath(exe)) != "v2" {
		t.Fatalf("after second swap: exe %q, previous %q", readFile(t, exe), readFile(t, PreviousPath(exe)))
	}

	// Rollback exchanges the two, so a second one undoes the first.
	if err := Rollback(exe); err != nil {
		t.Fatal(err)
	}
	if readFile(t, exe) != "v2" || readFile(t, PreviousPath(exe)) != "v3" {
		t.Fatalf("after rollback: exe %q, previous %q", readFile(t, exe), readFile(t, PreviousPath(exe)))
	}
	if err := Rollback(exe); err != nil {
		t.Fatal(err)
	}
	if readFile(t, exe) != "v3" {
		t.Fatalf("after second rollback: exe %q", readFile(t, exe))
	}
}

func TestRollbackWithoutPrevious(t *testing.T) {
	exe := installed(t, "v1")
	if err := Rollback(exe); !errors.Is(err, ErrNoPrevious) {
		t.Fatalf("rollback with no previous: %v, want ErrNoPrevious", err)
	}
	if readFile(t, exe) != "v1" {
		t.Fatal("failed rollback changed the binary")
	}
}

// The swap keeps the running binary as .previous by hard link: the
// installed path is never missing, and .previous is the same file that was
// installed (not a partial copy).
func TestSwapLinksPrevious(t *testing.T) {
	exe := installed(t, "v1")
	before, _ := os.Stat(exe)
	stage(t, exe, "v2")
	if err := Swap(exe); err != nil {
		t.Fatal(err)
	}
	prev, _ := os.Stat(PreviousPath(exe))
	if !os.SameFile(before, prev) {
		t.Skip("filesystem does not support hard links; .previous is a copy")
	}
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A new build that fails to reach serving three starts in a row is rolled
// back on the fourth; one that gets there clears the marker.
func TestHealthMarkerRollsBackAfterThreeFailedStarts(t *testing.T) {
	data := t.TempDir()
	exe := installed(t, "v1")
	stage(t, exe, "v2")
	if err := Swap(exe); err != nil {
		t.Fatal(err)
	}
	if err := writeHealth(data, &healthMarker{Commit: newCommit, Version: "89abcde", SwappedAt: t0}); err != nil {
		t.Fatal(err)
	}
	running := Build{Version: "89abcde", Commit: newCommit}
	for i := 1; i <= MaxFailedStarts; i++ {
		rolled, err := StartupCheck(data, exe, running, quietLog())
		if err != nil || rolled {
			t.Fatalf("start %d: rolled back %v, err %v", i, rolled, err)
		}
		if m, err := readHealth(data); err != nil || m.Starts != i {
			t.Fatalf("start %d: marker %+v, %v", i, m, err)
		}
	}
	rolled, err := StartupCheck(data, exe, running, quietLog())
	if err != nil || !rolled {
		t.Fatalf("start %d: rolled back %v, err %v; want a rollback", MaxFailedStarts+1, rolled, err)
	}
	if readFile(t, exe) != "v1" {
		t.Fatalf("binary after rollback: %q", readFile(t, exe))
	}
	if _, err := readHealth(data); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("marker survives the rollback")
	}
	s := loadState(data)
	if !s.skipped(newCommit) || s.State != StateRolledBack {
		t.Fatalf("state after rollback %+v: the failed build must not be installed again automatically", s)
	}

	// The previous build, now running, starts without a marker.
	if rolled, err := StartupCheck(data, exe, Build{Commit: oldCommit}, quietLog()); rolled || err != nil {
		t.Fatalf("start of the restored build: %v %v", rolled, err)
	}
}

func TestHealthMarkerClearedOnServing(t *testing.T) {
	data := t.TempDir()
	exe := installed(t, "v2")
	_ = writeHealth(data, &healthMarker{Commit: newCommit, Starts: MaxFailedStarts - 1})
	running := Build{Commit: newCommit}
	if rolled, _ := StartupCheck(data, exe, running, quietLog()); rolled {
		t.Fatal("rolled back early")
	}
	if !MarkServing(data) {
		t.Fatal("MarkServing found no marker")
	}
	if MarkServing(data) {
		t.Fatal("marker still there after serving")
	}
	for i := 0; i < MaxFailedStarts+2; i++ {
		if rolled, _ := StartupCheck(data, exe, running, quietLog()); rolled {
			t.Fatal("a build that reached serving was rolled back")
		}
	}
}

// A marker written for another build than the one running (the binary was
// replaced by hand since) is dropped, never acted on.
func TestStaleHealthMarkerIsDropped(t *testing.T) {
	data := t.TempDir()
	exe := installed(t, "hand-built")
	_ = writeHealth(data, &healthMarker{Commit: newCommit, Starts: MaxFailedStarts})
	if rolled, err := StartupCheck(data, exe, Build{Commit: oldCommit}, quietLog()); rolled || err != nil {
		t.Fatalf("stale marker: rolled back %v, %v", rolled, err)
	}
	if _, err := readHealth(data); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale marker kept")
	}
	if readFile(t, exe) != "hand-built" {
		t.Fatal("binary changed")
	}
}

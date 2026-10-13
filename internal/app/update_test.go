package app

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/update"
)

// An update (here a rollback, which needs no release server) asked of the
// running daemon makes Serve shut down normally and report that the daemon
// is to restart into the new binary (LOC-86); reaching serving clears a
// pending health marker.
func TestUpdateNowRestartsServe(t *testing.T) {
	a := servableApp(t, nil)
	exe := filepath.Join(t.TempDir(), "locus")
	for path, body := range map[string]string{exe: "new", update.PreviousPath(exe): "old"} {
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	a.Update.Exe = exe
	marker := filepath.Join(a.DataDir(), "update-health.json")
	if err := os.WriteFile(marker, []byte(`{"commit":"x","starts":1}`), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := a.UpdateNow(t.Context(), update.Request{Rollback: true}); !errors.Is(err, ErrNotServing) {
		t.Fatalf("update outside Serve: %v, want ErrNotServing", err)
	}

	cancel, done := startServe(t, a)
	defer cancel()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(marker); errors.Is(err, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("health marker not cleared once serving")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if a.RestartRequested() {
		t.Fatal("restart requested before any update")
	}
	rep, err := a.UpdateNow(t.Context(), update.Request{Rollback: true})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.RolledBack || !rep.Restarting {
		t.Fatalf("report %+v", rep)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not stop for the restart")
	}
	if !a.RestartRequested() {
		t.Fatal("serve stopped without asking for a restart")
	}
	if b, _ := os.ReadFile(exe); string(b) != "old" {
		t.Fatalf("binary after rollback %q", b)
	}
	st, err := a.BuildStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if st.Update == nil || st.Update.Channel != "main" || st.Update.Auto {
		t.Fatalf("status update field %+v", st.Update)
	}
}

package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Scenario 6 boundary — a killed transfer leaves its temporary behind. It must
// never be visible as a document (it is not), but it also must not accumulate
// forever: the reconciliation pass removes leftovers once they are clearly no
// longer in flight, while a live transfer's temporary survives.
func TestCrashLeftoverTemporaryIsCleanedUp(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	a.write(t, "notes/keep-me.md", "# keep me\n\nThis file must survive the sweep.\n")
	waitFor(t, 20*time.Second, func() bool { return a.has(t, "notes/keep-me.md") }, "fixture")
	// Only real documents; the helper walks the tree and sees temps too.
	docFiles := func() []string {
		var out []string
		for _, f := range a.listRel(t) {
			if !strings.Contains(filepath.Base(f), ".locus-tmp-") {
				out = append(out, f)
			}
		}
		return out
	}
	before := docFiles()

	// A leftover from an earlier crash: a SIGKILL between the temp write and
	// the rename would leave exactly this on disk.
	leftover := filepath.Join(a.Root, "bulk", ".locus-tmp-crashed")
	if err := os.MkdirAll(filepath.Join(a.Root, "bulk"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leftover, []byte("half a document"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(leftover, old, old); err != nil {
		t.Fatal(err)
	}
	// And one belonging to a transfer happening right now.
	inflight := filepath.Join(a.Root, "bulk", ".locus-tmp-in-flight")
	if err := os.WriteFile(inflight, []byte("still writing"), 0o644); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 30*time.Second, func() bool {
		_, err := os.Stat(leftover)
		return os.IsNotExist(err)
	}, "crash leftover temporary to be swept by reconciliation")

	if _, err := os.Stat(inflight); err != nil {
		t.Fatalf("the in-flight temporary must not be removed: %v", err)
	}
	// Real documents are untouched by the sweep.
	if got := docFiles(); len(got) != len(before) {
		t.Fatalf("document set changed from %v to %v", before, got)
	}
}

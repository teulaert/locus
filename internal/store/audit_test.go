package store

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// LOC-18: nothing ever deleted audit rows, so the table grew without bound.
// Retention removes rows past the age limit and all but the newest N, and
// leaves recent rows alone.
func TestPruneAuditByAgeAndCount(t *testing.T) {
	st := openStore(t)
	old := time.Now().Add(-48 * time.Hour).Unix()
	for i := 0; i < 3; i++ {
		if _, err := st.State.Exec(`INSERT INTO audit (ts, actor, action, detail) VALUES (?, 'api', 'upload', ?)`, old, fmt.Sprintf("old-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := st.Audit("api", "upload", fmt.Sprintf("recent-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Age first: the three day-old rows go, the five recent ones stay.
	n, err := st.PruneAudit(100, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("age prune deleted %d rows, want 3", n)
	}
	entries, err := st.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("%d rows after age prune, want 5", len(entries))
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Detail, "old-") {
			t.Fatalf("expired row survived: %+v", e)
		}
	}

	// Then count: only the newest two remain.
	if n, err := st.PruneAudit(2, 0); err != nil || n != 3 {
		t.Fatalf("count prune deleted %d rows (err %v), want 3", n, err)
	}
	entries, err = st.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Detail != "recent-4" || entries[1].Detail != "recent-3" {
		t.Fatalf("count prune kept the wrong rows: %+v %+v", entries[0], entries[len(entries)-1])
	}

	// Under the limit nothing is touched.
	if n, err := st.PruneAudit(10, time.Hour); err != nil || n != 0 {
		t.Fatalf("prune under limits deleted %d rows (err %v)", n, err)
	}
}

// LOC-18: refused requests are counted in memory, never written one row per
// request; a flush writes one summary row per source, and the number of
// sources tracked between flushes is bounded.
func TestAuditTallyAggregatesPerSource(t *testing.T) {
	st := openStore(t)
	tally := NewAuditTally()
	for i := 0; i < 50; i++ {
		tally.Note("peer", "auth-rejected", "10.0.0.9", "bad signature")
	}
	tally.Note("peer", "auth-rejected", "10.0.0.7", "stale timestamp")
	entries, err := st.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("Note wrote %d audit rows; it must only count", len(entries))
	}

	if err := tally.Flush(st); err != nil {
		t.Fatal(err)
	}
	entries, err = st.ListAudit(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("flush wrote %d rows, want one per source (2)", len(entries))
	}
	var sawNine bool
	for _, e := range entries {
		if e.Actor != "peer" || e.Action != "auth-rejected" {
			t.Fatalf("unexpected row %+v", e)
		}
		if strings.HasPrefix(e.Detail, "50 from 10.0.0.9") && strings.Contains(e.Detail, "bad signature") {
			sawNine = true
		}
	}
	if !sawNine {
		t.Fatalf("summary row lacks count and source: %+v", entries)
	}

	// A flush resets the tally.
	if err := tally.Flush(st); err != nil {
		t.Fatal(err)
	}
	if entries, _ := st.ListAudit(100); len(entries) != 2 {
		t.Fatalf("second flush wrote rows from an empty tally: %d", len(entries))
	}

	// Many distinct sources collapse into an overflow row.
	for i := 0; i < maxTallyKeys+100; i++ {
		tally.Note("api", "unauthorized", fmt.Sprintf("10.1.%d.%d", i/256, i%256), "")
	}
	if err := tally.Flush(st); err != nil {
		t.Fatal(err)
	}
	entries, err = st.ListAudit(1000)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(entries) - 2; got != maxTallyKeys+1 {
		t.Fatalf("flush wrote %d rows for many sources, want %d", got, maxTallyKeys+1)
	}
	if !hasDetail(entries, "100 from other") {
		t.Fatal("overflow sources were not aggregated under one row")
	}
}

func hasDetail(entries []*AuditEntry, detail string) bool {
	for _, e := range entries {
		if e.Detail == detail {
			return true
		}
	}
	return false
}

package jobs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/teulaert/locus/internal/store"
)

func newQueue(t *testing.T) (context.Context, *Queue, func()) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return context.Background(), NewQueue(st.Index), func() {}
}

func TestEnqueueCoalescesToNewestVersion(t *testing.T) {
	ctx, q, _ := newQueue(t)
	for i := 0; i < 100; i++ {
		hash := fmt.Sprintf("v%03d", i)
		if err := q.Enqueue(ctx, KindIndex, "/root", "doc.txt", hash, 0); err != nil {
			t.Fatal(err)
		}
	}
	n, err := q.PendingCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 100 modifications to coalesce into 1 job, got %d", n)
	}
	job, err := q.Claim(ctx)
	if err != nil || job == nil {
		t.Fatalf("claim: %v", err)
	}
	if job.Hash != "v099" {
		t.Fatalf("coalesced job must reflect the newest version, got %q", job.Hash)
	}
}

func TestEnqueueKeepsNewestHash(t *testing.T) {
	ctx, q, _ := newQueue(t)
	for _, h := range []string{"v1", "v2", "v3"} {
		if err := q.Enqueue(ctx, KindIndex, "/root", "doc.txt", h, 0); err != nil {
			t.Fatal(err)
		}
	}
	job, err := q.Claim(ctx)
	if err != nil || job == nil {
		t.Fatalf("claim: %v", err)
	}
	if job.Hash != "v3" {
		t.Fatalf("coalesced job must carry newest version: %q", job.Hash)
	}
}

func TestDropObsoleteWork(t *testing.T) {
	ctx, q, _ := newQueue(t)
	for i := 0; i < 200; i++ {
		if err := q.Enqueue(ctx, KindIndex, "/root", filepath.ToSlash(filepath.Join("bulk", string(rune('a'+i%26))+string(rune('a'+i/26%26))+".txt")), "h", 0); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := q.PendingCount(ctx)
	dropped, err := q.DropFor(ctx, "/root", "bulk/aa.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = dropped
	after, _ := q.PendingCount(ctx)
	if before < 2 {
		t.Fatalf("setup: expected multiple jobs, got %d", before)
	}
	if after >= before {
		t.Fatalf("DropFor did not remove obsolete work: %d -> %d", before, after)
	}
}

func TestClaimTransitionsAndComplete(t *testing.T) {
	ctx, q, _ := newQueue(t)
	if err := q.Enqueue(ctx, KindIndex, "/r", "a.txt", "h", 0); err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(ctx)
	if err != nil || job == nil {
		t.Fatalf("claim: %v", err)
	}
	if job.State != StateRunning {
		t.Fatalf("claimed job state = %s", job.State)
	}
	// A second claim must not return the same running job.
	again, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatalf("running job was claimed twice: %+v", again)
	}
	if err := q.Complete(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	n, _ := q.PendingCount(ctx)
	if n != 0 {
		t.Fatalf("complete left %d jobs", n)
	}
}

func TestFailBacksOffForRetry(t *testing.T) {
	ctx, q, _ := newQueue(t)
	q.MaxAttempts = 3
	if err := q.Enqueue(ctx, KindIndex, "/r", "a.txt", "h", 0); err != nil {
		t.Fatal(err)
	}
	job, _ := q.Claim(ctx)
	if err := q.Fail(ctx, job.ID, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	list, err := q.List(ctx, 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %+v", err, list)
	}
	if list[0].State != StatePending || list[0].Attempts != 1 {
		t.Fatalf("first failure should requeue: %+v", list[0])
	}
	if list[0].LastError == "" {
		t.Fatal("failure reason must be recorded")
	}
	// Retried work waits for its backoff window instead of hot-looping.
	again, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatalf("job was claimable inside its backoff window: %+v", again)
	}
}

func TestFailParksAfterMaxAttempts(t *testing.T) {
	ctx, q, _ := newQueue(t)
	q.MaxAttempts = 1
	if err := q.Enqueue(ctx, KindIndex, "/r", "a.txt", "h", 0); err != nil {
		t.Fatal(err)
	}
	job, _ := q.Claim(ctx)
	if err := q.Fail(ctx, job.ID, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	list, _ := q.List(ctx, 10)
	if len(list) != 1 || list[0].State != StateFailed {
		t.Fatalf("expected parked failed job, got %+v", list)
	}
	pending, _ := q.PendingCount(ctx)
	if pending != 0 {
		t.Fatalf("parked job still counted as work: %d", pending)
	}
}

func TestRequeueStaleAfterCrash(t *testing.T) {
	ctx, q, _ := newQueue(t)
	if err := q.Enqueue(ctx, KindIndex, "/r", "a.txt", "h", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	n, err := q.RequeueStale(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("RequeueStale resumed %d jobs, want 1", n)
	}
	pending, _ := q.PendingCount(ctx)
	if pending != 1 {
		t.Fatalf("queue lost interrupted work: %d", pending)
	}
}

func TestPriorityOrdering(t *testing.T) {
	ctx, q, _ := newQueue(t)
	if err := q.Enqueue(ctx, KindIndex, "/r", "low.txt", "h", 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, KindIndex, "/r", "high.txt", "h", 9); err != nil {
		t.Fatal(err)
	}
	job, _ := q.Claim(ctx)
	if job == nil || job.RelPath != "high.txt" {
		t.Fatalf("priority job not claimed first: %+v", job)
	}
}

func TestStats(t *testing.T) {
	ctx, q, _ := newQueue(t)
	if err := q.Enqueue(ctx, KindIndex, "/r", "a.txt", "h", 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, KindUnindex, "/r", "b.txt", "", 0); err != nil {
		t.Fatal(err)
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats[KindIndex][StatePending] != 1 || stats[KindUnindex][StatePending] != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

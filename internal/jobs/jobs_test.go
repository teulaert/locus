package jobs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

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
	if err := q.Complete(ctx, job); err != nil {
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
	if err := q.Fail(ctx, job, errors.New("boom")); err != nil {
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
	if err := q.Fail(ctx, job, errors.New("boom")); err != nil {
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

func TestPermanentFailureParksImmediately(t *testing.T) {
	ctx, q, _ := newQueue(t)
	q.MaxAttempts = 3
	if err := q.Enqueue(ctx, KindIndex, "/r", "bad.pdf", "h", 0); err != nil {
		t.Fatal(err)
	}
	job, _ := q.Claim(ctx)
	if err := q.Fail(ctx, job, Permanent(errors.New("parser panicked"))); err != nil {
		t.Fatal(err)
	}
	list, _ := q.List(ctx, 10)
	if len(list) != 1 || list[0].State != StateFailed || list[0].Attempts != 1 {
		t.Fatalf("permanent failure should park on the first attempt: %+v", list)
	}
	if list[0].LastError == "" {
		t.Fatal("failure reason must be recorded")
	}
}

// A job that crashes the whole process never reaches Fail. Claim counts the
// attempt, so repeated crash-and-restart cycles park the job after
// MaxAttempts instead of crashing the daemon on every start.
func TestRequeueStaleParksCrashLoop(t *testing.T) {
	ctx, q, _ := newQueue(t)
	q.MaxAttempts = 3
	if err := q.Enqueue(ctx, KindIndex, "/r", "crash.pdf", "h", 0); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= q.MaxAttempts; i++ {
		job, err := q.Claim(ctx)
		if err != nil || job == nil {
			t.Fatalf("restart %d: claim = %+v, %v", i, job, err)
		}
		if job.Attempts != i {
			t.Fatalf("restart %d: Claim must count the attempt, got %d", i, job.Attempts)
		}
		// The process dies here; the next start runs crash recovery.
		n, err := q.RequeueStale(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if i < q.MaxAttempts && n != 1 {
			t.Fatalf("restart %d: interrupted job should resume, resumed %d", i, n)
		}
		if i == q.MaxAttempts && n != 0 {
			t.Fatalf("job that exhausted its attempts was resumed again")
		}
	}
	list, _ := q.List(ctx, 10)
	if len(list) != 1 || list[0].State != StateFailed || list[0].LastError == "" {
		t.Fatalf("crash-looping job should be parked as failed with a reason: %+v", list)
	}
	if again, _ := q.Claim(ctx); again != nil {
		t.Fatalf("parked job was claimed again: %+v", again)
	}
}

// claimRunningThenEnqueue claims the job for v1 and enqueues v2 while it runs,
// reproducing a file change (or embedding re-queue) mid-processing.
func claimRunningThenEnqueue(t *testing.T, ctx context.Context, q *Queue) *Job {
	t.Helper()
	if err := q.Enqueue(ctx, KindIndex, "/r", "a.txt", "v1", 0); err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(ctx)
	if err != nil || job == nil {
		t.Fatalf("claim: %v", err)
	}
	if err := q.Enqueue(ctx, KindIndex, "/r", "a.txt", "v2", 0); err != nil {
		t.Fatal(err)
	}
	return job
}

func TestEnqueueWhileRunningIsNotClaimableTwice(t *testing.T) {
	ctx, q, _ := newQueue(t)
	claimRunningThenEnqueue(t, ctx, q)
	again, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatalf("re-enqueued running job was handed to a second worker: %+v", again)
	}
}

func TestCompleteAfterReenqueueKeepsNewerWork(t *testing.T) {
	ctx, q, _ := newQueue(t)
	job := claimRunningThenEnqueue(t, ctx, q)
	if job.Hash != "v1" {
		t.Fatalf("claimed hash = %q", job.Hash)
	}
	if err := q.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
	next, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || next.Hash != "v2" {
		t.Fatalf("completing the old version lost the newer work: %+v", next)
	}
	if err := q.Complete(ctx, next); err != nil {
		t.Fatal(err)
	}
	if n, _ := q.PendingCount(ctx); n != 0 {
		t.Fatalf("completing the current version left %d jobs", n)
	}
}

func TestFailAfterReenqueueDoesNotPenaliseNewerWork(t *testing.T) {
	ctx, q, _ := newQueue(t)
	q.MaxAttempts = 1
	job := claimRunningThenEnqueue(t, ctx, q)
	if err := q.Fail(ctx, job, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	list, err := q.List(ctx, 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %+v", err, list)
	}
	if list[0].State != StatePending || list[0].Attempts != 0 || list[0].LastError != "" {
		t.Fatalf("failure of a superseded version leaked into the newer job: %+v", list[0])
	}
	// The newer version is immediately claimable, without the retry backoff.
	next, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || next.Hash != "v2" {
		t.Fatalf("newer work not claimable after stale failure: %+v", next)
	}
}

func TestStaleFinishDoesNotTouchReclaimedJob(t *testing.T) {
	ctx, q, _ := newQueue(t)
	old := claimRunningThenEnqueue(t, ctx, q)
	if err := q.Complete(ctx, old); err != nil {
		t.Fatal(err)
	}
	current, err := q.Claim(ctx)
	if err != nil || current == nil {
		t.Fatalf("claim: %v", err)
	}
	// A duplicate finish from the old claim must not delete or re-arm the
	// row the current worker is now running.
	if err := q.Complete(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := q.Fail(ctx, old, errors.New("late")); err != nil {
		t.Fatal(err)
	}
	list, _ := q.List(ctx, 10)
	// Attempts is 1: Claim counts the current run, and the stale Fail
	// must not have added to it.
	if len(list) != 1 || list[0].State != StateRunning || list[0].Attempts != 1 {
		t.Fatalf("stale finish disturbed the running job: %+v", list)
	}
	if again, _ := q.Claim(ctx); again != nil {
		t.Fatalf("running job became claimable again: %+v", again)
	}
}

func TestPermanentFailureAfterReenqueueKeepsNewerWork(t *testing.T) {
	ctx, q, _ := newQueue(t)
	job := claimRunningThenEnqueue(t, ctx, q)
	if err := q.Fail(ctx, job, Permanent(errors.New("parser panicked"))); err != nil {
		t.Fatal(err)
	}
	next, err := q.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || next.Hash != "v2" || next.Attempts != 1 {
		t.Fatalf("permanent failure of a superseded version parked the newer work: %+v", next)
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Workers sleep on Ready instead of polling Claim, so every way work becomes
// claimable in this process must close it: a fresh enqueue, and a rerun that
// a finishing claim turns back into pending work (LOC-42).
func TestReadySignalsClaimableWork(t *testing.T) {
	ctx, q, _ := newQueue(t)
	ready := q.Ready()
	if closed(ready) {
		t.Fatal("Ready must block while nothing was queued")
	}
	job := claimRunningThenEnqueue(t, ctx, q)
	if !closed(ready) {
		t.Fatal("Enqueue must wake waiters")
	}

	ready = q.Ready()
	if err := q.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if !closed(ready) {
		t.Fatal("completing a job flagged for rerun must wake waiters for the newer work")
	}

	ready = q.Ready()
	job, err := q.Claim(ctx)
	if err != nil || job == nil {
		t.Fatalf("claim: %v %v", job, err)
	}
	if err := q.Complete(ctx, job); err != nil {
		t.Fatal(err)
	}
	if closed(ready) {
		t.Fatal("finishing the last job must not wake idle workers")
	}
	if got := q.Claims(); got != 2 {
		t.Fatalf("Claims() = %d, want 2", got)
	}
}

// A job backing off after a failure becomes claimable without any wake-up,
// so NextDue tells idle workers when to look again.
func TestNextDueReportsRetryDelay(t *testing.T) {
	ctx, q, _ := newQueue(t)
	if _, ok, err := q.NextDue(ctx); err != nil || ok {
		t.Fatalf("empty queue: ok=%v err=%v", ok, err)
	}
	if err := q.Enqueue(ctx, KindIndex, "/root", "a.txt", "h1", 0); err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(ctx)
	if err != nil || job == nil {
		t.Fatalf("claim: %v %v", job, err)
	}
	before := time.Now()
	if err := q.Fail(ctx, job, errors.New("transient")); err != nil {
		t.Fatal(err)
	}
	due, ok, err := q.NextDue(ctx)
	if err != nil || !ok {
		t.Fatalf("NextDue after Fail: ok=%v err=%v", ok, err)
	}
	if d := due.Sub(before); d < (retryDelay-1)*time.Second || d > (retryDelay+1)*time.Second {
		t.Fatalf("next retry due in %v, want about %ds", d, retryDelay)
	}
}

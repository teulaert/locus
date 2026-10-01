package jobs

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A worker's context is cancelled at shutdown, which can land while Claim's
// UPDATE ... RETURNING is running. The cancellation must not leave the
// statement open: an unfinalized write statement keeps the database's write
// lock, so every later writer (the next process over the same data
// directory, or this one after the connection is replaced) fails with
// SQLITE_BUSY for good (LOC-50).
func TestClaimCancelledMidQueryReleasesWriteLock(t *testing.T) {
	_, q, _ := newQueue(t)
	bg := context.Background()
	for i := 0; i < 300; i++ {
		if err := q.Enqueue(bg, KindIndex, "/root", fmt.Sprintf("doc%d.txt", i), "h", 0); err != nil {
			t.Fatalf("iteration %d: enqueue after a cancelled claim: %v", i, err)
		}
		ctx, cancel := context.WithCancel(bg)
		go func(d time.Duration) {
			time.Sleep(d)
			cancel()
		}(time.Duration(i%40) * 5 * time.Microsecond)
		_, _ = q.Claim(ctx)
		cancel()
	}
	if err := q.Enqueue(bg, KindIndex, "/root", "last.txt", "h", 0); err != nil {
		t.Fatalf("enqueue after cancelled claims: %v", err)
	}
}

package store

import (
	"bytes"
	"log/slog"
	"strings"
	gosync "sync"
	"testing"
	"time"
)

// syncBuffer is a bytes.Buffer safe for a logger and a test to share.
type syncBuffer struct {
	mu  gosync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// LOC-69: a path lock held for long is logged while the wait lasts and when
// it ends, naming the path, the waiting and the holding operation, and how
// long the wait took. An uncontended lock logs nothing.
func TestLockPathLogsLongWaits(t *testing.T) {
	var logs syncBuffer
	s := &Store{Log: slog.New(slog.NewTextHandler(&logs, nil)), LockWarnAfter: 50 * time.Millisecond}

	s.LockPathFor("/r", "quick.txt", "a")()
	release := s.LockPathFor("/r", "x.bin", "sync apply from aaaa")
	acquired := make(chan struct{})
	go func() {
		s.LockPathFor("/r", "x.bin", "sync apply from bbbb")()
		close(acquired)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "waiting for path lock") {
		if time.Now().After(deadline) {
			t.Fatalf("no warning while the wait lasted; log:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	release()
	<-acquired
	out := logs.String()
	for _, want := range []string{"relpath=x.bin", `op="sync apply from bbbb"`, `held_by="sync apply from aaaa"`, "path lock acquired after a long wait", "waited="} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "quick.txt") {
		t.Errorf("an uncontended lock was logged:\n%s", out)
	}
}

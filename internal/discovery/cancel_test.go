package discovery

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// A probe waiting for a reply ends as soon as its context does, so a
// daemon shutting down does not wait out the probe timeout for every
// silent target (LOC-81).
func TestProbeEndsWithItsContext(t *testing.T) {
	id, info := testInfo(t, t.TempDir())
	silent, err := net.ListenPacket("udp", "127.0.0.1:0") // reads, never answers
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err = ProbeOnce(ctx, silent.LocalAddr().String(), id, info, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ProbeOnce returned %v, want context.Canceled", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("ProbeOnce took %s after its context was cancelled", took)
	}
}

package sync

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
)

// LOC-81: a daemon shutting down cancels the contexts of the requests still
// running once its grace period is over. A put whose body has stopped
// arriving must end at that moment, not at its idle deadline a minute
// later, and what arrived of it is kept for its sender to resume.
func TestPutEndsWhenItsContextIsCancelled(t *testing.T) {
	recv := newEngineEnv(t)
	recv.MaxFile = 8 << 20
	recv.Partials = &PartialStore{Dir: filepath.Join(t.TempDir(), "partials")}
	sender := testIdentity(t)
	s := &Server{Engine: recv.Engine, Identity: recv.Identity, Roots: []string{recv.root},
		AuthorizedPeers: []string{sender.PeerID}} // the default 60 s idle deadline
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	inPut := make(chan struct{}, 1)
	returned := make(chan time.Time, 1)
	h := s.Handler()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/put" {
			inPut <- struct{}{}
			defer func() { returned <- time.Now() }()
		}
		h.ServeHTTP(w, r)
	}))
	srv.Config.BaseContext = func(net.Listener) context.Context { return base }
	srv.Start()
	t.Cleanup(srv.Close)

	content := make([]byte, 2<<20) // resumable
	_, _ = io.ReadFull(&patternReader{}, content)
	status := stalledPut(t, srv.URL, sender, recv.root, "big.bin", content)
	select {
	case <-inPut:
	case <-time.After(5 * time.Second):
		t.Fatal("the put never reached the handler")
	}
	// Let the first half be staged.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n, _ := recv.Partials.Offset(fswatch.HashBytes(content)); n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("nothing of the put was staged")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelled := time.Now()
	cancel()
	select {
	case at := <-returned:
		if took := at.Sub(cancelled); took > time.Second {
			t.Fatalf("the put handler returned %s after its context was cancelled", took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the put handler kept reading after its context was cancelled")
	}
	select {
	case code := <-status:
		if code == http.StatusOK {
			t.Fatalf("a cut-short put answered 200")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the sender never got an answer")
	}
	if n, _ := recv.Partials.Offset(fswatch.HashBytes(content)); n <= 0 {
		t.Errorf("a cut-short resumable put left %d bytes to resume from", n)
	}
	assertNoTempFiles(t, recv.root)
}

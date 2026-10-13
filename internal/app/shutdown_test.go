package app

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	syncpkg "github.com/teulaert/locus/internal/sync"
	"github.com/teulaert/locus/internal/vvector"
)

// tricklePut starts a signed put to a's peer service whose body arrives one
// byte every 100 ms, as over a very slow link. It returns the put's status
// (-1 for a transport error) once it ends, and a func that stops sending.
func tricklePut(t *testing.T, a *App, sender *identity.Identity, rel string, size int) (<-chan int, func()) {
	t.Helper()
	content := []byte(strings.Repeat("x", size))
	q := url.Values{
		"rootname": {a.Sync.NameOf(a.Roots()[0])},
		"relpath":  {rel},
		"version":  {vvector.New().Increment(sender.PeerID).String()},
		"hash":     {fswatch.HashBytes(content)},
		"size":     {strconv.Itoa(size)},
		"writer":   {sender.PeerID},
	}
	target := "/v1/put?" + q.Encode()
	declared, err := syncpkg.DeclaredContentHash(q.Get("hash"))
	if err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	addr := net.JoinHostPort(a.Cfg.Peer.Addr, strconv.Itoa(a.Cfg.Peer.Port))
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+target, pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(size)
	for k, v := range syncpkg.AuthHeadersHash(sender, http.MethodPost, target, declared) {
		req.Header.Set(k, v)
	}
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer pw.Close()
		for i := 0; i < size; i++ {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			if _, err := pw.Write(content[i : i+1]); err != nil {
				return
			}
		}
	}()
	client := &http.Client{Transport: &http.Transport{}}
	status := make(chan int, 1)
	go func() {
		defer client.CloseIdleConnections()
		resp, err := client.Do(req)
		if err != nil {
			status <- -1
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	var once bool
	return status, func() {
		if !once {
			once = true
			close(stop)
			<-writerDone
		}
	}
}

// LOC-81: `systemctl restart` under load exited 1 because the shutdown hit
// its 15 s deadline with "requests on 0.0.0.0:8909, filesystem watcher"
// pending. The peer service waited for a put trickling in on a slow link,
// and the watcher was only asked to stop once the servers had used up the
// whole deadline. Now the watcher stops at once, the put is cut short after
// the drain grace period, and Serve returns cleanly with nothing left
// running.
func TestShutdownCutsShortATricklingPut(t *testing.T) {
	a := servableApp(t, nil)
	sender, err := identity.Load(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a.Cfg.AuthorizedPeers = []string{sender.PeerID}
	root := a.Roots()[0]
	base := countGoroutines()

	const drain = 300 * time.Millisecond
	cancel, done := startServe(t, a, WithSyncDrain(drain), WithShutdownTimeout(10*time.Second))
	status, stopSending := tricklePut(t, a, sender, "slow.txt", 1000)
	defer stopSending()
	// Let the put get under way, with the watcher seeing changes meanwhile.
	for i := range 5 {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("busy-%d.txt", i)), []byte("busy"), 0o600); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case code := <-status:
		t.Fatalf("the trickling put ended (%d) before the shutdown", code)
	default:
	}

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v, want a clean stop", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return")
	}
	if took := time.Since(start); took > drain+2*time.Second {
		t.Fatalf("Serve took %s to stop with a %s drain", took, drain)
	}
	select {
	case code := <-status:
		switch code {
		case http.StatusOK, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			t.Fatalf("the trickling put answered %d; it should have been cut short", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the trickling put never ended")
	}
	stopSending()
	waitGoroutines(t, base)
}

// countGoroutines is runtime.NumGoroutine once the count has settled, so
// goroutines of earlier tests that are still winding down are not counted
// against this one.
func countGoroutines() int {
	n := runtime.NumGoroutine()
	for range 50 {
		time.Sleep(10 * time.Millisecond)
		m := runtime.NumGoroutine()
		if m == n {
			return n
		}
		n = m
	}
	return n
}

// A shutdown cut short at its deadline is reported to the caller as a
// *ShutdownError naming what is still running, including which requests.
func TestShutdownNamesTheRequestsStillRunning(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	a := servableApp(t, nil)
	stuck := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release // ignores its context and its connection
	})
	a.Cfg.API.Addr = "127.0.0.1"
	a.Cfg.API.Port = freeTCPPort(t)
	cancel, done := startServe(t, a, WithAPI(stuck), WithSyncDrain(100*time.Millisecond), WithShutdownTimeout(500*time.Millisecond))
	defer close(release)
	go func() {
		resp, err := http.Get("http://" + a.APIAddr() + "/v1/stuck?relpath=a.txt")
		if err == nil {
			resp.Body.Close()
		}
	}()
	waitClosed(t, entered, "the API request to reach its handler")
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return")
	}
	var se *ShutdownError
	if !errors.As(err, &se) {
		t.Fatalf("Serve returned %v, want a *ShutdownError", err)
	}
	want := "requests on " + a.APIAddr() + " (GET /v1/stuck a.txt)"
	found := false
	for _, p := range se.Pending {
		found = found || p == want
	}
	if !found {
		t.Fatalf("pending = %q, want it to include %q", se.Pending, want)
	}
}

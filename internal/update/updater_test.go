package update

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/netguard"
)

var (
	oldBins = map[string][]byte{"linux/amd64": []byte("old linux"), "darwin/arm64": []byte("old mac")}
	newBins = map[string][]byte{"linux/amd64": []byte("new linux binary"), "linux/arm64": []byte("new arm"), "darwin/arm64": []byte("new mac")}
)

// Both channels: main follows build-* prereleases, stable v* releases, and
// the newest release of the channel wins.
func TestCheckFollowsTheChannel(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t,
		signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(2*time.Hour), newBins, nil),
		signedRelease(t, priv, "v1.0.0", ChannelStable, "fedcba9cccccccccccccccccccccccccccccccc", t0.Add(time.Hour), newBins, nil),
		signedRelease(t, priv, "build-0123456", ChannelMain, oldCommit, t0, oldBins, nil),
	)
	exe := installed(t, "running")

	main := testUpdater(t, g, pub, exe, ChannelMain)
	rep, err := main.Manual(t.Context(), Request{Check: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Latest != "build-89abcde" || !rep.Available || rep.LatestCommit != newCommit || rep.Installed {
		t.Fatalf("main channel check: %+v", rep)
	}
	if st := main.Status(); st.State != StateAvailable || st.Latest != "build-89abcde" || st.CheckedAt == nil {
		t.Fatalf("status after check: %+v", st)
	}
	if readFile(t, exe) != "running" {
		t.Fatal("--check installed something")
	}

	stable := testUpdater(t, g, pub, exe, ChannelStable)
	rep, err = stable.Manual(t.Context(), Request{Check: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Latest != "v1.0.0" || !rep.Available {
		t.Fatalf("stable channel check: %+v", rep)
	}
}

// The signed channel decides, not the tag: a build- prerelease whose
// manifest says stable is not a main build.
func TestSignedChannelDecides(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t,
		signedRelease(t, priv, "build-89abcde", ChannelStable, newCommit, t0.Add(2*time.Hour), newBins, func(m *Manifest) {}),
	)
	// signedRelease marks main-channel releases as prereleases; this one
	// claims stable in its manifest but is published as a build.
	g.releases[0].Prerelease = true
	u := testUpdater(t, g, pub, installed(t, "running"), ChannelMain)
	rep, err := u.Manual(t.Context(), Request{Check: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Available || rep.Latest != "" {
		t.Fatalf("a stable-signed manifest was offered on main: %+v", rep)
	}
}

func TestUpdateInstallsVerifiedBinary(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	exe := installed(t, "running")
	u := testUpdater(t, g, pub, exe, ChannelMain)
	rep, err := u.Manual(t.Context(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Installed || rep.Binary != exe || rep.Action != "update" {
		t.Fatalf("report %+v", rep)
	}
	if readFile(t, exe) != "new linux binary" || readFile(t, PreviousPath(exe)) != "running" {
		t.Fatalf("after update: exe %q, previous %q", readFile(t, exe), readFile(t, PreviousPath(exe)))
	}
	if m, err := readHealth(u.opts.DataDir); err != nil || m.Commit != newCommit || m.Starts != 0 {
		t.Fatalf("health marker %+v, %v", m, err)
	}
	if st := u.Status(); st.State != StateInstalled {
		t.Fatalf("state %q", st.State)
	}

	// A rollback by hand puts the old binary back and keeps the build it
	// replaced from being installed again on its own.
	running := New(Options{DataDir: u.opts.DataDir, Running: Build{Version: "89abcde", Commit: newCommit, Time: t0.Add(time.Hour)}})
	running.Exe = exe
	rep, err = running.Manual(t.Context(), Request{Rollback: true})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.RolledBack || readFile(t, exe) != "running" {
		t.Fatalf("rollback: %+v, exe %q", rep, readFile(t, exe))
	}
	if s := loadState(u.opts.DataDir); !s.skipped(newCommit) {
		t.Fatalf("rolled-back build not skipped: %+v", s)
	}
	if _, err := readHealth(u.opts.DataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("rollback left the health marker")
	}
}

// A binary that does not match the signed manifest is never installed and
// leaves nothing behind.
func TestTamperedArtifactIsRejected(t *testing.T) {
	pub, priv := newKey(t)
	for name, tamper := range map[string]func(*fakeRelease){
		"other content": func(r *fakeRelease) { r.Files["locus-linux-amd64"] = []byte("evil linux binary") },
		"truncated":     func(r *fakeRelease) { r.Files["locus-linux-amd64"] = []byte("new linux") },
		"longer":        func(r *fakeRelease) { r.Files["locus-linux-amd64"] = []byte("new linux binary, and more") },
		"manifest":      func(r *fakeRelease) { r.Files[ManifestFile] = append(r.Files[ManifestFile], '\n') },
	} {
		t.Run(name, func(t *testing.T) {
			rel := signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil)
			tamper(&rel)
			g := newFakeGitHub(t, rel)
			exe := installed(t, "running")
			u := testUpdater(t, g, pub, exe, ChannelMain)
			if _, err := u.Manual(t.Context(), Request{}); err == nil {
				t.Fatal("tampered release installed")
			}
			if readFile(t, exe) != "running" {
				t.Fatal("binary changed")
			}
			for _, p := range []string{StagePath(exe), PreviousPath(exe)} {
				if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("%s left behind", p)
				}
			}
			if _, err := readHealth(u.opts.DataDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("health marker written for a failed update")
			}
			if st := u.Status(); st.State != StateFailed || st.Error == "" {
				t.Fatalf("status after a failed update: %+v", st)
			}
		})
	}
}

func TestWrongKeyIsRejected(t *testing.T) {
	_, priv := newKey(t)
	other, _ := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	exe := installed(t, "running")
	u := testUpdater(t, g, other, exe, ChannelMain)
	if _, err := u.Manual(t.Context(), Request{}); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("release signed with another key: %v, want ErrBadSignature", err)
	}
	if readFile(t, exe) != "running" {
		t.Fatal("binary changed")
	}
}

func TestUnknownPlatformIsSkipped(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	exe := installed(t, "running")
	u := testUpdater(t, g, pub, exe, ChannelMain)
	u.GOOS, u.GOARCH = "plan9", "386"
	rep, err := u.Manual(t.Context(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Available || rep.Installed || rep.Note == "" {
		t.Fatalf("report for a platform without a binary: %+v", rep)
	}
	if st := u.Status(); st.State != StateNoArtifact {
		t.Fatalf("state %q", st.State)
	}
	if readFile(t, exe) != "running" {
		t.Fatal("binary changed")
	}
}

// An older release, a signed one served again included, is never installed
// over a newer running build.
func TestDowngradeIsRefused(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(-time.Hour), newBins, nil))
	exe := installed(t, "running")
	u := testUpdater(t, g, pub, exe, ChannelMain)
	rep, err := u.Manual(t.Context(), Request{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Available || rep.Installed || rep.Note == "" {
		t.Fatalf("older release: %+v", rep)
	}
	if readFile(t, exe) != "running" {
		t.Fatal("downgraded")
	}
	// The running build itself is up to date.
	g.set(signedRelease(t, priv, "build-0123456", ChannelMain, oldCommit, t0, oldBins, nil))
	rep, err = u.Manual(t.Context(), Request{})
	if err != nil || rep.Available || rep.Note != "" {
		t.Fatalf("same build: %+v, %v", rep, err)
	}
	if st := u.Status(); st.State != StateUpToDate {
		t.Fatalf("state %q", st.State)
	}
}

// Polls that find nothing new are conditional requests answered 304, and a
// verified manifest is not downloaded again.
func TestETagAvoidsRefetching(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	u := testUpdater(t, g, pub, installed(t, "running"), ChannelMain)
	for i := 0; i < 3; i++ {
		if _, err := u.Manual(t.Context(), Request{Check: true}); err != nil {
			t.Fatal(err)
		}
	}
	if g.count("lists") != 1 || g.count("304") != 2 {
		t.Fatalf("release lists: %d full, %d not modified; want 1 and 2", g.count("lists"), g.count("304"))
	}
	if n := g.count("build-89abcde/" + ManifestFile); n != 1 {
		t.Fatalf("manifest downloaded %d times", n)
	}
	// A new release changes the list, which is fetched again.
	g.set(signedRelease(t, priv, "build-fedcba9", ChannelMain, "fedcba9ddddddddddddddddddddddddddddddddd", t0.Add(2*time.Hour), newBins, nil))
	rep, err := u.Manual(t.Context(), Request{Check: true})
	if err != nil || rep.Latest != "build-fedcba9" {
		t.Fatalf("after a new release: %+v, %v", rep, err)
	}
}

func TestRateLimitBacksOff(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	u := testUpdater(t, g, pub, installed(t, "running"), ChannelMain)
	now := time.Unix(1_800_000_000, 0)
	u.Now = func() time.Time { return now }
	u.Client.Now = u.Now

	// Primary limit: 403 with no requests left until X-RateLimit-Reset.
	g.limit = func(w http.ResponseWriter) bool {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(now.Add(45*time.Minute).Unix(), 10))
		w.WriteHeader(http.StatusForbidden)
		return true
	}
	next, installedNow := u.tick(t.Context())
	if installedNow || next < 45*time.Minute {
		t.Fatalf("after a primary rate limit: next check in %s, want at least 45m", next)
	}
	if st := u.Status(); st.State != StateRateLimited || st.Error == "" {
		t.Fatalf("status %+v", st)
	}
	// Secondary limit: Retry-After, longer than the interval.
	g.limit = func(w http.ResponseWriter) bool {
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	}
	if next, _ := u.tick(t.Context()); next != 2*time.Hour {
		t.Fatalf("after Retry-After 7200: next check in %s", next)
	}
	// And then it works again.
	if next, _ := u.tick(t.Context()); next != time.Hour {
		t.Fatalf("after recovering: next check in %s", next)
	}
	if st := u.Status(); st.State != StateAvailable {
		t.Fatalf("state after recovering %q", st.State)
	}
	var rl *RateLimitError
	g.limit = func(w http.ResponseWriter) bool {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusForbidden)
		return true
	}
	if _, err := u.Manual(t.Context(), Request{Check: true}); !errors.As(err, &rl) {
		t.Fatalf("manual check under a rate limit: %v", err)
	}
}

// With auto on, a new build is installed after the staged-rollout delay and
// the daemon is asked to restart; a rendezvous peer waits one interval
// more, so the hub is never first.
func TestAutoInstallsAfterRolloutDelay(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	for _, rendezvous := range []bool{false, true} {
		exe := installed(t, "running")
		u := testUpdater(t, g, pub, exe, ChannelMain)
		u.opts.Auto, u.opts.Rendezvous = true, rendezvous
		now := t0
		u.Now = func() time.Time { return now }
		u.RolloutDelay = func() time.Duration { return 7 * time.Minute }

		next, done := u.tick(t.Context())
		want := 7 * time.Minute
		if rendezvous {
			want = time.Hour // capped at one interval; the install is later
		}
		if done || next != want || u.Status().State != StateWaiting {
			t.Fatalf("rendezvous %v: first tick: next %s, installed %v, state %q", rendezvous, next, done, u.Status().State)
		}
		now = now.Add(7 * time.Minute)
		_, done = u.tick(t.Context())
		if done != !rendezvous {
			t.Fatalf("rendezvous %v: installed after the rollout delay: %v", rendezvous, done)
		}
		if rendezvous {
			now = now.Add(time.Hour)
			if _, done = u.tick(t.Context()); !done {
				t.Fatal("rendezvous peer never installed")
			}
		}
		if readFile(t, exe) != "new linux binary" {
			t.Fatalf("rendezvous %v: binary %q", rendezvous, readFile(t, exe))
		}
	}
}

func TestRunRestartsAfterAutoInstall(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	exe := installed(t, "running")
	u := testUpdater(t, g, pub, exe, ChannelMain)
	u.opts.Auto = true
	u.FirstCheck = func() time.Duration { return 0 }
	u.RolloutDelay = func() time.Duration { return 0 }
	var restarts atomic.Int32
	done := make(chan struct{})
	go func() { u.Run(t.Context(), func() { restarts.Add(1) }); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not install and return")
	}
	if restarts.Load() != 1 || readFile(t, exe) != "new linux binary" || u.Status().State != StateRestarting {
		t.Fatalf("restarts %d, binary %q, state %q", restarts.Load(), readFile(t, exe), u.Status().State)
	}
}

// With auto off the daemon only reports; and it never installs a build
// that was rolled back, which `locus update` by hand still does.
func TestAutoOffAndSkippedBuilds(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	exe := installed(t, "running")
	u := testUpdater(t, g, pub, exe, ChannelMain)
	u.RolloutDelay = func() time.Duration { return 0 }
	if _, done := u.tick(t.Context()); done || u.Status().State != StateAvailable {
		t.Fatalf("auto off: installed %v, state %q", done, u.Status().State)
	}
	u.opts.Auto = true
	u.saved.addSkip(newCommit)
	if _, done := u.tick(t.Context()); done || u.Status().State != StateRolledBack {
		t.Fatalf("rolled-back build: installed %v, state %q", done, u.Status().State)
	}
	if readFile(t, exe) != "running" {
		t.Fatal("rolled-back build installed automatically")
	}
	if rep, err := u.Manual(t.Context(), Request{}); err != nil || !rep.Installed {
		t.Fatalf("by hand: %+v, %v", rep, err)
	}
}

// The state survives in the data directory for `locus status` run without
// a daemon, minus what only a running process can be doing.
func TestStatusPersists(t *testing.T) {
	pub, priv := newKey(t)
	g := newFakeGitHub(t, signedRelease(t, priv, "build-89abcde", ChannelMain, newCommit, t0.Add(time.Hour), newBins, nil))
	u := testUpdater(t, g, pub, installed(t, "running"), ChannelMain)
	if _, err := u.Manual(t.Context(), Request{Check: true}); err != nil {
		t.Fatal(err)
	}
	u.setState(StateDownloading, nil)
	again := New(Options{Channel: ChannelMain, DataDir: u.opts.DataDir, Running: u.opts.Running})
	again.Key = pub
	st := again.Status()
	if st.Latest != "build-89abcde" || st.State != StateAvailable || st.CheckedAt == nil {
		t.Fatalf("reloaded status %+v", st)
	}
	b, _ := json.Marshal(st)
	var keys map[string]any
	_ = json.Unmarshal(b, &keys)
	for _, k := range []string{"auto", "channel", "running", "latest", "state", "checked_at"} {
		if _, ok := keys[k]; !ok {
			t.Fatalf("status JSON lacks %q: %s", k, b)
		}
	}
}

// Under the test network guard the updater's client reaches loopback test
// servers only: GitHub itself is refused before any connection is made.
func TestGuardKeepsTestsOffGitHub(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	c := NewClient("", "locus/test")
	if _, err := c.Releases(t.Context()); !errors.Is(err, netguard.ErrBlocked) {
		t.Fatalf("request to the real GitHub API from a test: %v, want it blocked by the guard", err)
	}
}

// Package update keeps a Locus binary up to date from CI-built, signed
// releases (LOC-86).
//
// CI publishes every build of main (channel "main", GitHub prereleases
// tagged build-<sha>) and every v* tag (channel "stable") with a
// manifest.json listing each binary's size and SHA-256, and manifest.sig, an
// ed25519 signature over the manifest's exact bytes. A binary is installed
// only when the signature verifies with the public key compiled into the
// running binary (ReleasePublicKey) and the download matches the manifest:
// peers never author binaries and never take one from each other.
//
// Installing swaps the binary next to the running one (keeping the old one
// as <exe>.previous) and the daemon re-executes itself, keeping its pid.
// A new binary that fails to reach serving three starts in a row is rolled
// back on the next start.
package update

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// States reported in Status.State.
const (
	StateDisabled    = "disabled"     // no release key in this build
	StateNotChecked  = "not_checked"  // no check has run yet
	StateUpToDate    = "up_to_date"   // the running build is the latest (or newer)
	StateAvailable   = "available"    // a newer build is published
	StateWaiting     = "waiting"      // auto: staged-rollout delay before installing
	StateDownloading = "downloading"  // fetching and verifying the new binary
	StateInstalled   = "installed"    // swapped in; runs when the daemon next starts
	StateRestarting  = "restarting"   // swapped in; the daemon is re-executing
	StateRolledBack  = "rolled_back"  // the latest build was rolled back and is not installed again automatically
	StateNoArtifact  = "no_artifact"  // the latest release has no binary for this platform
	StateRateLimited = "rate_limited" // GitHub refused the check; retried after its deadline
	StateFailed      = "failed"       // the last check or install failed (error says why)
)

// MaxRolloutDelay bounds the random delay between seeing a new build and
// installing it, so peers do not all restart at once.
const MaxRolloutDelay = 10 * time.Minute

// Options configure an Updater.
type Options struct {
	Auto       bool
	Channel    string
	Interval   time.Duration
	URL        string // GitHub API base; empty is DefaultAPI
	Rendezvous bool   // a rendezvous peer waits one extra interval
	DataDir    string
	Running    Build
	Log        *slog.Logger
}

// Status is the updater's state as `locus status` reports it.
type Status struct {
	Auto      bool       `json:"auto"`
	Channel   string     `json:"channel"`
	Running   string     `json:"running"`
	Latest    string     `json:"latest"`
	State     string     `json:"state"`
	CheckedAt *time.Time `json:"checked_at,omitempty"`
	Error     string     `json:"error,omitempty"`
}

// Request is what `locus update` asks for.
type Request struct {
	Check    bool `json:"check,omitempty"`
	Rollback bool `json:"rollback,omitempty"`
}

// Report is the outcome of `locus update` (and POST /api/v1/update).
type Report struct {
	Action        string     `json:"action"` // check, update or rollback
	Channel       string     `json:"channel"`
	Running       string     `json:"running"`
	RunningCommit string     `json:"running_commit,omitempty"`
	Latest        string     `json:"latest"`
	LatestVersion string     `json:"latest_version,omitempty"`
	LatestCommit  string     `json:"latest_commit,omitempty"`
	Available     bool       `json:"available"`
	Installed     bool       `json:"installed"`
	RolledBack    bool       `json:"rolled_back"`
	Restarting    bool       `json:"restarting"`
	Binary        string     `json:"binary,omitempty"`
	CheckedAt     *time.Time `json:"checked_at,omitempty"`
	Note          string     `json:"note,omitempty"`
}

// ErrBusy is an update requested while another one is being installed.
var ErrBusy = errors.New("an update is already being installed")

// Updater checks for, verifies and installs releases.
type Updater struct {
	opts Options
	log  *slog.Logger

	// Exe is the binary an update replaces; empty means the running one
	// (symlinks followed).
	Exe string
	// Key verifies manifests; nil means TrustedKey().
	Key ed25519.PublicKey
	// Client reads releases.
	Client *Client
	// GOOS and GOARCH choose the artifact.
	GOOS, GOARCH string
	// RolloutDelay is the staged-rollout delay before an automatic install
	// of a newly seen build (default: random up to MaxRolloutDelay).
	RolloutDelay func() time.Duration
	// FirstCheck is the delay before the daemon's first check (default:
	// random one to five minutes, so a service restart loop cannot use up
	// the rate limit).
	FirstCheck func() time.Duration
	Now        func() time.Time

	applyMu sync.Mutex

	mu        sync.Mutex
	saved     savedState
	installAt map[string]time.Time // commit -> when auto installs it
	verified  map[string]*candidate
}

// candidate is a release whose manifest verified.
type candidate struct {
	release  Release
	manifest *Manifest
	artifact *Artifact // nil: no binary for this platform
	url      string
}

// New returns an Updater for opts, with the last check's outcome loaded
// from the data directory.
func New(opts Options) *Updater {
	if opts.Channel == "" {
		opts.Channel = ChannelMain
	}
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Minute
	}
	log := opts.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	u := &Updater{
		opts:      opts,
		log:       log,
		Client:    NewClient(opts.URL, "locus/"+opts.Running.Version),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		Now:       time.Now,
		installAt: map[string]time.Time{},
		verified:  map[string]*candidate{},
		RolloutDelay: func() time.Duration {
			return rand.N(MaxRolloutDelay)
		},
		FirstCheck: func() time.Duration {
			return time.Minute + rand.N(4*time.Minute)
		},
	}
	u.saved = loadState(opts.DataDir)
	// What a previous process was in the middle of is over now.
	switch u.saved.State {
	case StateWaiting, StateDownloading, StateRestarting, StateInstalled:
		switch {
		case u.saved.LatestCommit == "":
			u.saved.State = StateNotChecked
		case sameCommit(u.saved.LatestCommit, opts.Running.Commit):
			u.saved.State = StateUpToDate
		case u.saved.skipped(u.saved.LatestCommit):
			u.saved.State = StateRolledBack
		default:
			u.saved.State = StateAvailable
		}
	}
	return u
}

func (u *Updater) key() (ed25519.PublicKey, error) {
	if u.Key != nil {
		return u.Key, nil
	}
	return TrustedKey()
}

// Enabled reports whether this build can verify releases at all.
func (u *Updater) Enabled() bool {
	_, err := u.key()
	return err == nil
}

func (u *Updater) exe() (string, error) {
	if u.Exe != "" {
		return u.Exe, nil
	}
	return ResolveExecutable()
}

// Status reports the updater's state.
func (u *Updater) Status() *Status {
	u.mu.Lock()
	defer u.mu.Unlock()
	st := &Status{
		Auto:    u.opts.Auto,
		Channel: u.opts.Channel,
		Running: u.opts.Running.Version,
		Latest:  u.saved.Latest,
		State:   u.saved.State,
		Error:   u.saved.Error,
	}
	if !u.saved.CheckedAt.IsZero() {
		t := u.saved.CheckedAt
		st.CheckedAt = &t
	}
	if st.State == "" {
		st.State = StateNotChecked
	}
	if !u.Enabled() {
		st.State, st.Error = StateDisabled, ""
	}
	return st
}

// setState records the state (and error) and persists it.
func (u *Updater) setState(state string, err error) {
	u.mu.Lock()
	u.saved.State = state
	u.saved.Error = ""
	if err != nil {
		u.saved.Error = err.Error()
	}
	s := u.saved
	u.mu.Unlock()
	if werr := saveState(u.opts.DataDir, s); werr != nil {
		u.log.Debug("update: save state", "err", werr)
	}
}

// check finds the newest release on the channel and verifies its manifest.
// It returns nil when the channel has no release yet.
func (u *Updater) check(ctx context.Context) (*candidate, error) {
	key, err := u.key()
	if err != nil {
		return nil, err
	}
	rels, err := u.Client.Releases(ctx)
	if err != nil {
		return nil, err
	}
	prefix := "build-"
	if u.opts.Channel == ChannelStable {
		prefix = "v"
	}
	var found *candidate
	for _, r := range rels {
		if r.Draft || !strings.HasPrefix(r.TagName, prefix) || r.Prerelease != (u.opts.Channel == ChannelMain) {
			continue
		}
		c, err := u.verify(ctx, key, r)
		if err != nil {
			return nil, fmt.Errorf("release %s: %w", r.TagName, err)
		}
		if c.manifest.Channel != u.opts.Channel {
			continue // the signed channel is what counts, not the tag
		}
		found = c
		break
	}
	now := u.Now().UTC()
	u.mu.Lock()
	u.saved.CheckedAt = now
	if found != nil {
		u.saved.Latest, u.saved.LatestCommit = found.release.TagName, found.manifest.Commit
	}
	u.mu.Unlock()
	return found, nil
}

// verify downloads a release's manifest and signature and verifies them.
// A verified release is remembered, so a later poll does not fetch it
// again.
func (u *Updater) verify(ctx context.Context, key ed25519.PublicKey, r Release) (*candidate, error) {
	u.mu.Lock()
	c, ok := u.verified[r.TagName]
	u.mu.Unlock()
	if ok {
		return c, nil
	}
	fetch := func(name string) ([]byte, error) {
		a, ok := r.asset(name)
		if !ok {
			return nil, fmt.Errorf("no %s asset", name)
		}
		var buf strings.Builder
		if _, err := u.Client.Download(ctx, a.URL, MaxManifestSize, &buf); err != nil {
			return nil, err
		}
		return []byte(buf.String()), nil
	}
	data, err := fetch(ManifestFile)
	if err != nil {
		return nil, err
	}
	sig, err := fetch(SignatureFile)
	if err != nil {
		return nil, err
	}
	m, err := VerifyManifest(key, data, sig)
	if err != nil {
		return nil, err
	}
	c = &candidate{release: r, manifest: m}
	if art, ok := m.ArtifactFor(u.GOOS, u.GOARCH); ok {
		a, ok := r.asset(art.File)
		if !ok {
			return nil, fmt.Errorf("manifest lists %s but the release has no such asset", art.File)
		}
		c.artifact, c.url = art, a.URL
	}
	u.mu.Lock()
	u.verified[r.TagName] = c
	u.mu.Unlock()
	return c, nil
}

// report describes the running build and c.
func (u *Updater) report(action string, c *candidate) *Report {
	r := &Report{
		Action:        action,
		Channel:       u.opts.Channel,
		Running:       u.opts.Running.Version,
		RunningCommit: u.opts.Running.Commit,
	}
	u.mu.Lock()
	if !u.saved.CheckedAt.IsZero() {
		t := u.saved.CheckedAt
		r.CheckedAt = &t
	}
	u.mu.Unlock()
	if c == nil {
		r.Note = "no release published on channel " + u.opts.Channel
		return r
	}
	r.Latest, r.LatestVersion, r.LatestCommit = c.release.TagName, c.manifest.Version, c.manifest.Commit
	switch {
	case c.artifact == nil:
		r.Note = "the latest release has no binary for " + u.GOOS + "/" + u.GOARCH
	case c.manifest.Newer(u.opts.Running):
		r.Available = true
	case !sameCommit(c.manifest.Commit, u.opts.Running.Commit):
		r.Note = "the running build is newer than the latest release; not downgrading (locus update --rollback returns to the previous binary)"
	}
	return r
}

// stateFor is the state a finished check leaves.
func (u *Updater) stateFor(c *candidate) string {
	switch {
	case c == nil:
		return StateUpToDate
	case c.artifact == nil:
		return StateNoArtifact
	case !c.manifest.Newer(u.opts.Running):
		return StateUpToDate
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.saved.skipped(c.manifest.Commit) {
		return StateRolledBack
	}
	return StateAvailable
}

// failState is the state a failed check or install leaves.
func failState(err error) string {
	var rl *RateLimitError
	if errors.As(err, &rl) {
		return StateRateLimited
	}
	return StateFailed
}

// Manual performs what `locus update` asks: a check, a rollback, or a
// check followed by installing a newer build at once (no rollout delay,
// and also a build that was rolled back before). It never restarts
// anything; the caller does, when Report.Installed or RolledBack is set.
func (u *Updater) Manual(ctx context.Context, req Request) (*Report, error) {
	if req.Rollback {
		return u.rollback()
	}
	c, err := u.check(ctx)
	if err != nil {
		u.setState(failState(err), err)
		return nil, err
	}
	action := "update"
	if req.Check {
		action = "check"
	}
	rep := u.report(action, c)
	u.setState(u.stateFor(c), nil)
	if req.Check || !rep.Available {
		return rep, nil
	}
	exe, err := u.apply(ctx, c)
	if err != nil {
		return nil, err
	}
	rep.Installed, rep.Binary = true, exe
	u.setState(StateInstalled, nil)
	return rep, nil
}

// rollback puts the previous binary back and remembers not to install the
// one it replaces again on its own.
func (u *Updater) rollback() (*Report, error) {
	if !u.applyMu.TryLock() {
		return nil, ErrBusy
	}
	defer u.applyMu.Unlock()
	exe, err := u.exe()
	if err != nil {
		return nil, err
	}
	if err := Rollback(exe); err != nil {
		return nil, err
	}
	// The previous binary ran before; it needs no health watch.
	removeHealth(u.opts.DataDir)
	u.mu.Lock()
	u.saved.addSkip(u.opts.Running.Commit)
	u.mu.Unlock()
	u.setState(StateRolledBack, nil)
	u.log.Warn("update: rolled back to the previous binary", "replaced", u.opts.Running.Version, "binary", exe)
	rep := u.report("rollback", nil)
	rep.Note = ""
	rep.RolledBack, rep.Binary = true, exe
	return rep, nil
}

// apply downloads c's binary next to the running one, verifies its size and
// SHA-256 against the signed manifest, and swaps it in. Any failure leaves
// the installed binary untouched.
func (u *Updater) apply(ctx context.Context, c *candidate) (string, error) {
	if !u.applyMu.TryLock() {
		return "", ErrBusy
	}
	defer u.applyMu.Unlock()
	exe, err := u.exe()
	if err != nil {
		return "", err
	}
	u.setState(StateDownloading, nil)
	if err := u.stage(ctx, exe, c.url, c.artifact); err != nil {
		u.setState(failState(err), err)
		u.log.Warn("update: download failed; nothing installed", "release", c.release.TagName, "err", err)
		return "", err
	}
	// The marker goes first: a crash right after the swap must still be
	// counted against the new binary.
	if err := writeHealth(u.opts.DataDir, &healthMarker{Commit: c.manifest.Commit, Version: c.manifest.Version, SwappedAt: u.Now().UTC()}); err != nil && u.opts.DataDir != "" {
		_ = os.Remove(StagePath(exe))
		u.setState(StateFailed, err)
		return "", fmt.Errorf("update: health marker: %w", err)
	}
	if err := Swap(exe); err != nil {
		removeHealth(u.opts.DataDir)
		_ = os.Remove(StagePath(exe))
		u.setState(StateFailed, err)
		u.log.Warn("update: swap failed; nothing installed", "release", c.release.TagName, "err", err)
		return "", err
	}
	u.log.Info("update: installed new build", "release", c.release.TagName, "version", c.manifest.Version, "binary", exe, "previous", PreviousPath(exe))
	return exe, nil
}

// stage writes the artifact to StagePath(exe) and verifies it.
func (u *Updater) stage(ctx context.Context, exe, url string, art *Artifact) (err error) {
	mode := os.FileMode(0o755)
	if fi, err := os.Stat(exe); err == nil {
		mode = fi.Mode().Perm() | 0o100
	}
	path := StagePath(exe)
	// Opened before anything is downloaded: an unwritable install
	// directory fails here and costs no download.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("cannot write the new binary next to %s: %w", exe, err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	h := sha256.New()
	n, err := u.Client.Download(ctx, url, art.Size, io.MultiWriter(f, h))
	if err == nil && n != art.Size {
		err = fmt.Errorf("%s: got %d bytes, manifest says %d", art.File, n, art.Size)
	}
	if err == nil && !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), art.SHA256) {
		err = fmt.Errorf("%s: SHA-256 does not match the signed manifest", art.File)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(path, mode)
	}
	return err
}

// Run checks for updates every interval until ctx ends. A newer build is
// installed when auto is on, after the staged-rollout delay (plus one
// interval on a rendezvous peer, so the hub is never first), and restart
// is then called; with auto off the status says it is available. It does
// nothing in a build without a release key.
func (u *Updater) Run(ctx context.Context, restart func()) {
	if !u.Enabled() {
		return
	}
	timer := time.NewTimer(u.FirstCheck())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		next, installed := u.tick(ctx)
		if installed {
			u.setState(StateRestarting, nil)
			restart()
			return
		}
		timer.Reset(next)
	}
}

// tick is one check of Run. It returns when to check next, and whether a
// new binary was installed.
func (u *Updater) tick(ctx context.Context) (time.Duration, bool) {
	every := u.opts.Interval
	c, err := u.check(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return every, false
		}
		u.setState(failState(err), err)
		var rl *RateLimitError
		if errors.As(err, &rl) {
			u.log.Warn("update: GitHub rate limit; backing off", "until", rl.Until.Format(time.RFC3339))
			return max(every, rl.Until.Sub(u.Now())), false
		}
		u.log.Warn("update: check failed", "err", err)
		return every, false
	}
	state := u.stateFor(c)
	if state != StateAvailable || !u.opts.Auto {
		if state == StateAvailable {
			u.log.Info("update: a newer build is available (update.auto is off; run: locus update)", "release", c.release.TagName)
		}
		u.setState(state, nil)
		return every, false
	}
	now := u.Now()
	u.mu.Lock()
	at, seen := u.installAt[c.manifest.Commit]
	if !seen {
		at = now.Add(u.RolloutDelay())
		if u.opts.Rendezvous {
			at = at.Add(every)
		}
		u.installAt[c.manifest.Commit] = at
	}
	u.mu.Unlock()
	if !seen {
		u.log.Info("update: new build found; installing after the staged-rollout delay", "release", c.release.TagName, "at", at.Format(time.RFC3339))
	}
	if now.Before(at) {
		u.setState(StateWaiting, nil)
		return min(every, at.Sub(now)), false
	}
	if _, err := u.apply(ctx, c); err != nil {
		return every, false
	}
	return every, true
}

// MarkRestarting records that the daemon is restarting into a binary just
// installed.
func (u *Updater) MarkRestarting() { u.setState(StateRestarting, nil) }

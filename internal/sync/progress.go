package sync

import (
	"context"
	"errors"
	"io"
	"sort"
	gosync "sync"
	"time"
)

// rateWindow is the span the transfer rate is averaged over (LOC-66).
const rateWindow = 10 * time.Second

// PeerProgress describes the part of a running round that synchronizes with
// one peer (LOC-66): what the plan holds per root, how much of it is done,
// the file moving now, and the recent transfer rate.
type PeerProgress struct {
	PeerID    string    `json:"peer_id"`
	Hostname  string    `json:"hostname,omitempty"`
	StartedAt time.Time `json:"started_at"`
	// Root is the name of the root being synchronized now.
	Root string `json:"root"`
	// Phase is "manifest" while manifests are exchanged and planned,
	// "transfer" while the plan is carried out, "done" once every root ran.
	Phase string `json:"phase"`
	// CurrentFile and CurrentOp (push, pull, delete) name the action in
	// progress, empty between actions.
	CurrentFile string         `json:"current_file,omitempty"`
	CurrentOp   string         `json:"current_op,omitempty"`
	Roots       []RootProgress `json:"roots"`

	// Totals over Roots. Bytes planned cover the roots planned so far: a
	// root's plan is only known once its manifests have been exchanged.
	PushBytesPlanned int64 `json:"push_bytes_planned"`
	PullBytesPlanned int64 `json:"pull_bytes_planned"`
	PushedBytes      int64 `json:"pushed_bytes"`
	PulledBytes      int64 `json:"pulled_bytes"`
	// BytesPerSec is the transfer rate over the last ten seconds, in both
	// directions together.
	BytesPerSec float64 `json:"bytes_per_sec"`
	// ETASeconds estimates the time left for the planned bytes at
	// BytesPerSec; it is omitted while the rate is zero or nothing is left.
	ETASeconds int64 `json:"eta_seconds,omitempty"`
}

// RootProgress is one root's share of a peer's round.
type RootProgress struct {
	Root string `json:"root"`
	// State is "pending", "manifest", "transfer", "done" or "failed".
	State string `json:"state"`
	// Planned counts the actions that change either side (noop excluded),
	// split into pushes, pulls and deletions in either direction.
	Planned          int   `json:"planned"`
	PushPlanned      int   `json:"push_planned"`
	PullPlanned      int   `json:"pull_planned"`
	DeletePlanned    int   `json:"delete_planned"`
	PushBytesPlanned int64 `json:"push_bytes_planned"`
	PullBytesPlanned int64 `json:"pull_bytes_planned"`
	// Done counts finished actions, Failed those among them that failed.
	Done        int   `json:"done"`
	Pushed      int   `json:"pushed"`
	Pulled      int   `json:"pulled"`
	Deleted     int   `json:"deleted"`
	Failed      int   `json:"failed"`
	PushedBytes int64 `json:"pushed_bytes"`
	PulledBytes int64 `json:"pulled_bytes"`
}

// tracker records one peer's progress while its part of a round runs.
type tracker struct {
	mu      gosync.Mutex
	p       PeerProgress
	root    int // index into p.Roots of the current root
	samples []rateSample
	// pre holds the bytes already counted for the current file, so a
	// failed transfer can be taken back out of the totals.
	pre    int64
	preDir string
	now    func() time.Time
}

type rateSample struct {
	at    time.Time
	bytes int64
}

func newTracker(peerID, hostname string, roots []string, now func() time.Time) *tracker {
	if now == nil {
		now = time.Now
	}
	t := &tracker{now: now}
	t.p = PeerProgress{PeerID: peerID, Hostname: hostname, StartedAt: now(), Phase: "manifest", Roots: make([]RootProgress, len(roots))}
	for i, r := range roots {
		t.p.Roots[i] = RootProgress{Root: r, State: "pending"}
	}
	t.samples = []rateSample{{at: t.p.StartedAt}}
	return t
}

// beginRoot marks root i as having its manifests exchanged.
func (t *tracker) beginRoot(i int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.root = i
	t.p.Root = t.p.Roots[i].Root
	t.p.Phase = "manifest"
	t.p.Roots[i].State = "manifest"
}

// planned records the plan for the current root.
func (t *tracker) planned(plan []Action) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := &t.p.Roots[t.root]
	for _, a := range plan {
		switch a.Kind {
		case ActionPushCreate, ActionPushUpdate:
			r.PushPlanned++
			if a.Local != nil {
				r.PushBytesPlanned += a.Local.Size
				t.p.PushBytesPlanned += a.Local.Size
			}
		case ActionPullCreate, ActionPullUpdate:
			r.PullPlanned++
			if a.Remote != nil {
				r.PullBytesPlanned += a.Remote.Size
				t.p.PullBytesPlanned += a.Remote.Size
			}
		case ActionPushDelete, ActionPullDelete:
			r.DeletePlanned++
		case ActionPullVersion:
		default:
			continue
		}
		r.Planned++
	}
	r.State = "transfer"
	t.p.Phase = "transfer"
}

// start names the action now in progress.
func (t *tracker) start(op, relpath string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.p.CurrentOp, t.p.CurrentFile = op, relpath
	t.pre, t.preDir = 0, op
}

// finish counts a completed action. A failed transfer takes the bytes it had
// moved back out, so the totals describe content that arrived; the rate
// keeps them, since they did cross the link.
func (t *tracker) finish(kind ActionKind, changed bool, failed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := &t.p.Roots[t.root]
	if kind != ActionNoop {
		r.Done++
	}
	switch {
	case failed:
		r.Failed++
		switch t.preDir {
		case "push":
			r.PushedBytes -= t.pre
			t.p.PushedBytes -= t.pre
		case "pull":
			r.PulledBytes -= t.pre
			t.p.PulledBytes -= t.pre
		}
	case !changed:
	case kind == ActionPushCreate || kind == ActionPushUpdate:
		r.Pushed++
	case kind == ActionPullCreate || kind == ActionPullUpdate:
		r.Pulled++
	case kind == ActionPushDelete || kind == ActionPullDelete:
		r.Deleted++
	}
	t.p.CurrentOp, t.p.CurrentFile = "", ""
	t.pre, t.preDir = 0, ""
}

// endRoot closes the current root.
func (t *tracker) endRoot(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := &t.p.Roots[t.root]
	if err != nil {
		r.State = "failed"
	} else {
		r.State = "done"
	}
	t.p.CurrentOp, t.p.CurrentFile = "", ""
}

// moved counts n bytes of the current file crossing the link.
func (t *tracker) moved(dir string, n int64) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	r := &t.p.Roots[t.root]
	if dir == "push" {
		r.PushedBytes += n
		t.p.PushedBytes += n
	} else {
		r.PulledBytes += n
		t.p.PulledBytes += n
	}
	t.pre += n
	last := t.samples[len(t.samples)-1]
	now := t.now()
	total := last.bytes + n
	if now.Sub(last.at) < time.Second && len(t.samples) > 1 {
		t.samples[len(t.samples)-1].bytes = total
	} else {
		t.samples = append(t.samples, rateSample{at: now, bytes: total})
	}
	// Keep one sample at or before the window's start as the baseline.
	cut := 0
	for cut+1 < len(t.samples) && now.Sub(t.samples[cut+1].at) >= rateWindow {
		cut++
	}
	t.samples = t.samples[cut:]
}

// snapshot returns a copy with the rate and ETA computed for now.
func (t *tracker) snapshot() PeerProgress {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.p
	p.Roots = append([]RootProgress(nil), t.p.Roots...)
	now := t.now()
	if p.Phase == "done" {
		return p
	}
	cur := t.samples[len(t.samples)-1].bytes
	// The baseline is the newest sample at least a window old, or else the
	// oldest one there is. A stalled transfer has no new samples, so the
	// rate decays to zero once the window passes.
	base := t.samples[0]
	for _, s := range t.samples {
		if now.Sub(s.at) >= rateWindow {
			base = s
		}
	}
	if span := now.Sub(base.at).Seconds(); span >= 1 {
		p.BytesPerSec = float64(cur-base.bytes) / span
	}
	left := (p.PushBytesPlanned - p.PushedBytes) + (p.PullBytesPlanned - p.PulledBytes)
	if p.BytesPerSec > 0 && left > 0 {
		p.ETASeconds = int64(float64(left)/p.BytesPerSec + 0.5)
	}
	return p
}

// progressReader reports every byte read through it to a tracker.
type progressReader struct {
	r   io.Reader
	t   *tracker
	dir string
}

func (c *progressReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.t.moved(c.dir, int64(n))
	return n, err
}

type trackerKey struct{}

func withTracker(ctx context.Context, t *tracker) context.Context {
	return context.WithValue(ctx, trackerKey{}, t)
}

// counted wraps r so its bytes count towards the round's progress, when ctx
// carries a tracker.
func counted(ctx context.Context, r io.Reader, dir string) io.Reader {
	t, _ := ctx.Value(trackerKey{}).(*tracker)
	if t == nil || r == nil {
		return r
	}
	return &progressReader{r: r, t: t, dir: dir}
}

// Progress returns the progress of every peer a round is synchronizing with
// right now, sorted by peer ID. It is empty, never nil, when no round runs.
func (e *Engine) Progress() []PeerProgress {
	e.busyMu.Lock()
	ts := make([]*tracker, 0, len(e.busy))
	for _, t := range e.busy {
		ts = append(ts, t)
	}
	e.busyMu.Unlock()
	out := make([]PeerProgress, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerID < out[j].PeerID })
	return out
}

// ErrRoundRunning is returned by Round when every peer it was asked to
// synchronize with is already being synchronized by another round.
var ErrRoundRunning = errors.New("sync: round already running")

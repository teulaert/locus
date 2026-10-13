package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	gosync "sync"
	"sync/atomic"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/pathguard"
	"github.com/teulaert/locus/internal/peer"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// ChangeNotifier is informed about files changed by synchronization so index
// work can be queued through the normal pipeline.
type ChangeNotifier interface {
	NotifyChange(root, relpath string, deleted bool)
}

// Engine performs synchronization rounds between equal peers.
type Engine struct {
	Store     *store.Store
	Identity  *identity.Identity
	Roots     []string
	RootNames map[string]string
	// SyncHidden marks the roots (by path) that synchronize hidden paths
	// (.git, .obsidian, .DS_Store). On every other root a hidden path is
	// neither offered in the manifest, pushed, pulled nor accepted from a
	// peer. Nil syncs no hidden paths anywhere.
	SyncHidden map[string]bool
	MaxFile    int64
	// Partials keeps interrupted incoming puts so the sender can resume
	// them (LOC-67). Nil receives every put in one go, as before.
	Partials *PartialStore
	Notifier ChangeNotifier
	// Hostnames names peers in progress reports. It is display only:
	// conflict sibling names must not depend on it (LOC-73).
	Hostnames  func() map[string]string
	Log        *slog.Logger
	HTTPClient *http.Client

	// TransferIdleTimeout aborts a peer request that moves no bytes for
	// this long (DefaultTransferIdleTimeout when zero).
	TransferIdleTimeout time.Duration

	// OnPeerError and OnPeerSuccess report per-peer transport outcomes so the
	// caller can surface a peer that answers discovery probes but cannot be
	// reached for transfers, and back off instead of paying a connect timeout
	// every interval.
	// OnPeerSuccess is called at most once per round with a peer: as soon
	// as the peer serves a manifest, so a transport failure noted earlier
	// does not outlive it for the rest of a long round (LOC-85), or, for a
	// peer that only refused, when the round with it ends. A transport
	// failure later in the same round is still reported through
	// OnPeerError when the round with the peer ends.
	OnPeerError   func(peerID string, err error)
	OnPeerSuccess func(peerID string)
	// OnPeerIncompatible reports a peer that refused transfer because it
	// speaks a different sync protocol. It replaces OnPeerError for that
	// peer: the refusal is an answer, so there is nothing to back off from.
	OnPeerIncompatible func(peerID string, err *IncompatibleError)
	// OnPeerStart and OnPeerDone bracket the part of a round that runs
	// with one peer, so the caller can show that peer as synchronizing for
	// exactly as long as that lasts rather than for the whole round.
	OnPeerStart func(peerID string)
	OnPeerDone  func(peerID string)

	// busy holds the peers a round is synchronizing with right now, with
	// their progress. A peer is in at most one round at a time; rounds
	// over different peers run side by side (LOC-61).
	busyMu gosync.Mutex
	busy   map[string]*tracker
	// now, when set by tests, replaces time.Now for progress rates.
	now func() time.Time

	// inflight holds the content coming in right now, keyed by root, path
	// and content hash, with the peer each comes from: the pulls rounds
	// are carrying out, and the puts the server is receiving. Rounds with
	// different peers plan against the same local manifest, so without it
	// a new peer would fetch every file from every online peer at once
	// (LOC-77); and every online peer's own round pushes it the same files,
	// which the server answers as in progress while one copy arrives
	// (LOC-78). A claim lives only as long as its transfer: it is released
	// when the transfer ends, however it ends, so the set is empty whenever
	// no round runs and no put is being received.
	inflightMu gosync.Mutex
	inflight   map[transferKey]transferClaim

	// servedFiles and servedBytes count the file content served to peers
	// (GET /v1/file), for Served; receivedFiles the puts received in full,
	// for Received.
	servedFiles   atomic.Int64
	servedBytes   atomic.Int64
	receivedFiles atomic.Int64
	// adoptedFiles and adoptedBytes count the incoming files produced from
	// local copies instead of transferred (LOC-82), for Adopted.
	adoptedFiles atomic.Int64
	adoptedBytes atomic.Int64

	// conflictMu serialises adopting conflicts reported by peers, which
	// concurrent peer rounds may do for the same conflict.
	conflictMu gosync.Mutex

	// stagingOnce builds the fallback staging store used when Partials is
	// nil.
	stagingOnce gosync.Once
	stagingDef  *PartialStore

	// refusals remembers the last application-level refusal per peer and
	// root, so each one is logged once rather than every round.
	refusalMu gosync.Mutex
	refusals  map[string]string
	// hiddenNoted remembers the peer/root pairs already logged as not
	// exchanging hidden paths.
	hiddenNoted map[string]bool

	// afterRead, when set by tests, runs after Apply has read the stored
	// record and before it decides or writes anything.
	afterRead func(root, relpath string)
}

// Stats summarizes a synchronization round.
type Stats struct {
	Peers      int `json:"peers"`
	Pushed     int `json:"pushed"`
	Pulled     int `json:"pulled"`
	Deleted    int `json:"deleted"`
	Conflicted int `json:"conflicted"`
	Noop       int `json:"noop"`
	// SkippedInFlight counts the pulls left out because another peer's
	// round was already fetching the same content, or had applied it
	// since this round planned (LOC-77). The next round plans them again
	// against what arrived.
	SkippedInFlight int `json:"skipped_in_flight"`
	// SkippedHeld counts the pushes the peer answered without their
	// content because it already held it, applying only the version; and
	// SkippedReceiving those it answered as in progress because the same
	// content was on its way to it from another peer (LOC-78). Neither
	// moved content or failed; a push skipped as receiving is planned
	// again next round against what arrived.
	SkippedHeld      int `json:"skipped_held"`
	SkippedReceiving int `json:"skipped_receiving"`
	// Adopted counts the files whose content was not transferred because
	// the receiving side already held it under another path, a folder
	// renamed or moved on one peer being the usual case (LOC-82): pulls
	// this peer produced from its own copy, and pushes the peer answered
	// having done so. AdoptedBytes is their size. They are neither pushed
	// nor pulled.
	Adopted      int      `json:"adopted"`
	AdoptedBytes int64    `json:"adopted_bytes"`
	Errors       []string `json:"errors,omitempty"`
	// Busy lists the peers left out because another round was already
	// synchronizing with them.
	Busy      []string  `json:"busy,omitempty"`
	StartedAt time.Time `json:"started_at"`
	Duration  string    `json:"duration"`
}

// NameOf returns the name peers know a local root by: its entry in
// RootNames (the configured name), or else its basename (RootName).
func (e *Engine) NameOf(root string) string {
	if n := e.RootNames[root]; n != "" {
		return n
	}
	return RootName(root)
}

// SyncsHidden reports whether root synchronizes hidden paths.
func (e *Engine) SyncsHidden(root string) bool { return e.SyncHidden[root] }

// noteOnce reports whether key is new to set, recording it.
func (e *Engine) noteOnce(key string) bool {
	e.refusalMu.Lock()
	defer e.refusalMu.Unlock()
	if e.hiddenNoted == nil {
		e.hiddenNoted = make(map[string]bool)
	}
	if e.hiddenNoted[key] {
		return false
	}
	e.hiddenNoted[key] = true
	return true
}

// withoutHidden drops the entries with a hidden component.
func withoutHidden(entries []ManifestEntry) []ManifestEntry {
	out := entries[:0:0]
	for _, en := range entries {
		if !pathguard.HasHidden(en.RelPath) {
			out = append(out, en)
		}
	}
	return out
}

// staging is where received content is staged before it is installed:
// Partials, or else a directory under the system temporary directory.
func (e *Engine) staging() *PartialStore {
	if e.Partials != nil {
		return e.Partials
	}
	e.stagingOnce.Do(func() {
		e.stagingDef = &PartialStore{Dir: filepath.Join(os.TempDir(), "locus-staging")}
	})
	return e.stagingDef
}

// Busy reports whether a round is synchronizing with any peer.
func (e *Engine) Busy() bool {
	e.busyMu.Lock()
	defer e.busyMu.Unlock()
	return len(e.busy) > 0
}

// BusyWith reports whether a round is synchronizing with peerID now.
func (e *Engine) BusyWith(peerID string) bool {
	e.busyMu.Lock()
	defer e.busyMu.Unlock()
	_, ok := e.busy[peerID]
	return ok
}

// claim marks the peers no other round holds as busy and returns them with
// their trackers; the rest are returned as taken.
func (e *Engine) claim(peers []*peer.Entry) (mine []*peer.Entry, trackers []*tracker, taken []string) {
	var names map[string]string
	if e.Hostnames != nil {
		names = e.Hostnames()
	}
	roots := make([]string, len(e.Roots))
	for i, r := range e.Roots {
		roots[i] = e.NameOf(r)
	}
	e.busyMu.Lock()
	defer e.busyMu.Unlock()
	if e.busy == nil {
		e.busy = make(map[string]*tracker)
	}
	for _, p := range peers {
		if p.SyncAddr == "" {
			continue
		}
		if _, ok := e.busy[p.PeerID]; ok {
			taken = append(taken, p.PeerID)
			continue
		}
		host := p.Hostname
		if host == "" {
			host = names[p.PeerID]
		}
		t := newTracker(p.PeerID, host, roots, e.now)
		e.busy[p.PeerID] = t
		mine = append(mine, p)
		trackers = append(trackers, t)
	}
	return mine, trackers, taken
}

func (e *Engine) release(peerID string) {
	e.busyMu.Lock()
	delete(e.busy, peerID)
	e.busyMu.Unlock()
}

// Round synchronizes with every online peer. Peers are synchronized
// concurrently, each in a goroutine of its own that runs the roots one at a
// time, so a long transfer to one peer does not hold back the others
// (LOC-61). A peer another round is already synchronizing with is left out
// and listed in Stats.Busy; when that leaves nothing to do, Round returns
// ErrRoundRunning. Failures against a single peer never abort the round:
// peers may be offline for arbitrary periods.
//
// Only transport failures (the peer could not be dialed, timed out or failed
// with a 5xx) are reported through OnPeerError and so drive backoff. A peer
// that answered with an application-level refusal is reachable: a root it
// does not share (404) is skipped quietly, and other refusals (401, 403,
// 400) are recorded for that root, while the round carries on with the
// remaining roots and peers (LOC-14).
func (e *Engine) Round(ctx context.Context, peers []*peer.Entry) (*Stats, error) {
	st := &Stats{StartedAt: time.Now(), Peers: len(peers)}
	mine, trackers, taken := e.claim(peers)
	if len(mine) == 0 && len(taken) > 0 {
		return st, ErrRoundRunning
	}
	for _, id := range taken {
		st.Busy = append(st.Busy, identity.Short(id))
	}
	// Each peer counts into a Stats of its own; they are summed in peer
	// order once all are done, so the merged errors read the same way
	// whatever order the peers finished in.
	per := make([]Stats, len(mine))
	var wg gosync.WaitGroup
	for i, p := range mine {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer e.release(p.PeerID)
			if e.OnPeerStart != nil {
				e.OnPeerStart(p.PeerID)
			}
			if e.OnPeerDone != nil {
				defer e.OnPeerDone(p.PeerID)
			}
			e.syncPeer(withTracker(ctx, trackers[i]), p, trackers[i], &per[i])
		}()
	}
	wg.Wait()
	for i := range per {
		st.add(&per[i])
	}
	st.Duration = time.Since(st.StartedAt).String()
	return st, nil
}

// add sums another peer's counters into st.
func (st *Stats) add(o *Stats) {
	st.Pushed += o.Pushed
	st.Pulled += o.Pulled
	st.Deleted += o.Deleted
	st.Conflicted += o.Conflicted
	st.Noop += o.Noop
	st.SkippedInFlight += o.SkippedInFlight
	st.SkippedHeld += o.SkippedHeld
	st.SkippedReceiving += o.SkippedReceiving
	st.Adopted += o.Adopted
	st.AdoptedBytes += o.AdoptedBytes
	st.Errors = append(st.Errors, o.Errors...)
}

// syncPeer runs every root with one peer and reports the outcome through
// the OnPeer callbacks.
func (e *Engine) syncPeer(ctx context.Context, p *peer.Entry, tr *tracker, st *Stats) {
	var (
		transportErr error
		incompatible *IncompatibleError
	)
	answered := false
roots:
	for i, root := range e.Roots {
		tr.beginRoot(i)
		started, errsBefore := time.Now(), len(st.Errors)
		err := e.roundPeer(ctx, p, root, st, tr)
		tr.endRoot(err)
		var (
			se          *StatusError
			badManifest *ManifestError
		)
		switch {
		case err == nil:
			answered = true
			e.forgetRefusal(p.PeerID, root)
			if len(st.Errors) == errsBefore {
				// Every action planned for this root succeeded, so the
				// peer now holds each tombstone we had when the round
				// started. Tombstone pruning waits for this from every
				// known peer.
				if err := e.Store.RecordPeerRound(root, p.PeerID, started); err != nil {
					e.logger().Error("record peer round", "peer", identity.Short(p.PeerID), "root", root, "err", err)
				}
			}
		case errors.As(err, &incompatible):
			// Checked before the transport/application split: the peer
			// answered, but nothing it says can be trusted, and every
			// other root would be refused the same way (LOC-17).
			st.Errors = append(st.Errors, fmt.Sprintf("%s: %v", identity.Short(p.PeerID), err))
			break roots
		case IsTransport(err):
			transportErr = err
			st.Errors = append(st.Errors, fmt.Sprintf("%s: %v", identity.Short(p.PeerID), err))
			e.logger().Warn("sync round failed", "peer", identity.Short(p.PeerID), "root", root, "err", err)
		case errors.As(err, &se):
			answered = true
			e.refused(p.PeerID, root, se, st)
		case errors.As(err, &badManifest):
			// The peer answered, but with nothing to plan against: the
			// root is skipped for this peer and counted as failed.
			answered = true
			st.Errors = append(st.Errors, fmt.Sprintf("%s: %v", identity.Short(p.PeerID), err))
			if !e.seenRefusal(p.PeerID, root, err.Error()) {
				e.logger().Warn("peer sent a malformed manifest", "peer", identity.Short(p.PeerID), "root", RootName(root), "err", err)
			}
		default:
			// A local failure (state database, cancelled context) says
			// nothing about the peer either way.
			st.Errors = append(st.Errors, fmt.Sprintf("%s: %v", identity.Short(p.PeerID), err))
			e.logger().Warn("sync round failed", "peer", identity.Short(p.PeerID), "root", root, "err", err)
		}
	}
	switch {
	case incompatible != nil:
		// Neither a transport failure to back off from nor a success.
		e.logger().Warn("peer speaks an incompatible sync protocol", "peer", identity.Short(p.PeerID), "err", incompatible)
		if e.OnPeerIncompatible != nil {
			e.OnPeerIncompatible(p.PeerID, incompatible)
		}
	case transportErr != nil:
		if e.OnPeerError != nil {
			e.OnPeerError(p.PeerID, transportErr)
		}
	case answered:
		e.peerAnswered(p.PeerID, tr)
	}
}

// peerAnswered reports the peer's first answer in this round through
// OnPeerSuccess; later answers are not reported again.
func (e *Engine) peerAnswered(peerID string, tr *tracker) {
	if tr.firstAnswer() && e.OnPeerSuccess != nil {
		e.OnPeerSuccess(peerID)
	}
}

// refused handles a peer's application-level answer for one root. A root the
// peer does not have (404) is a normal configuration, not an error: it is
// logged once and skipped. Other refusals are recorded in the round's errors
// every time but logged only when they first appear or change, so a
// persistent refusal does not flood the log at every interval.
func (e *Engine) refused(peerID, root string, se *StatusError, st *Stats) {
	notShared := se.Code == http.StatusNotFound
	if !notShared {
		st.Errors = append(st.Errors, fmt.Sprintf("%s: %v", identity.Short(peerID), se))
	}
	if e.seenRefusal(peerID, root, se.Error()) {
		return
	}
	if notShared {
		e.logger().Info("peer does not share root; skipping it", "peer", identity.Short(peerID), "root", RootName(root))
		return
	}
	e.logger().Warn("peer refused sync of root", "peer", identity.Short(peerID), "root", RootName(root), "status", se.Code, "err", se)
}

// seenRefusal records msg as the last refusal of root by peerID and reports
// whether it was already the last one, so repeats are not logged again.
func (e *Engine) seenRefusal(peerID, root, msg string) bool {
	key := peerID + "\x00" + root
	e.refusalMu.Lock()
	defer e.refusalMu.Unlock()
	if e.refusals == nil {
		e.refusals = make(map[string]string)
	}
	seen := e.refusals[key] == msg
	e.refusals[key] = msg
	return seen
}

// forgetRefusal re-arms the refusal log once a root syncs again.
func (e *Engine) forgetRefusal(peerID, root string) {
	e.refusalMu.Lock()
	delete(e.refusals, peerID+"\x00"+root)
	e.refusalMu.Unlock()
}

func (e *Engine) roundPeer(ctx context.Context, p *peer.Entry, root string, st *Stats, tr *tracker) error {
	if draining(ctx) {
		return ErrDraining
	}
	base := "http://" + p.SyncAddr
	remote, err := e.FetchManifest(ctx, base, root)
	if err != nil {
		return err
	}
	e.peerAnswered(p.PeerID, tr)
	local, err := e.LocalManifest(root)
	if err != nil {
		return err
	}
	remoteEntries := remote.Entries
	if !e.SyncsHidden(root) || remote.SkipsHidden {
		// Hidden paths are exchanged only when both sides sync them. Our
		// own are already out of local unless we do; the peer's are
		// planned as though it had none, so nothing is pulled, and when
		// the peer declines them nothing is pushed it would refuse.
		local, remoteEntries = withoutHidden(local), withoutHidden(remoteEntries)
		if e.SyncsHidden(root) && e.noteOnce(p.PeerID+"\x00"+root) {
			e.logger().Info("peer does not sync hidden paths for root; not sending ours", "peer", identity.Short(p.PeerID), "root", e.NameOf(root))
		}
	}
	// Renames first, then small files (Schedule), so a large transfer does
	// not hold back the many quick ones behind it (LOC-67) and a renamed
	// folder's content is adopted before its old paths are deleted
	// (LOC-82).
	plan := Schedule(Plan(local, remoteEntries))
	tr.planned(plan)
	// The renames (Schedule's groups) whose content is not in place on
	// the receiving side: their deletion waits for the next round, so the
	// old file is still there to adopt from (LOC-82).
	unsettled := map[int]bool{}
	for _, act := range plan {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if draining(ctx) {
			return ErrDraining
		}
		if act.group > 0 && unsettled[act.group] && (act.Kind == ActionPullDelete || act.Kind == ActionPushDelete) {
			e.logger().Debug("deletion left for the next round: its renamed content is not in place yet", "peer", identity.Short(p.PeerID), "relpath", act.RelPath)
			tr.start("delete", act.RelPath)
			tr.finish(act.Kind, false, false)
			continue
		}
		if e.runAction(ctx, base, p, root, act, st, tr) && act.group > 0 {
			unsettled[act.group] = true
		}
	}
	return nil
}

// runAction carries out one planned action, counting it in st and tr. An
// action that fails is recorded in st.Errors and the plan carries on.
// unsettled reports a push or pull whose content is not in place on the
// receiving side when it returns: it failed, or the content is still on
// its way there (from another peer, or through another round).
func (e *Engine) runAction(ctx context.Context, base string, p *peer.Entry, root string, act Action, st *Stats, tr *tracker) (unsettled bool) {
	var (
		changed bool
		failed  bool
	)
	defer func() { unsettled = unsettled || failed }()
	switch act.Kind {
	case ActionPushCreate, ActionPushUpdate:
		tr.start("push", act.RelPath)
	case ActionPullCreate, ActionPullUpdate:
		tr.start("pull", act.RelPath)
	case ActionPushDelete, ActionPullDelete:
		tr.start("delete", act.RelPath)
	case ActionPullVersion:
		tr.start("version", act.RelPath)
	}
	defer func() { tr.finish(act.Kind, changed, failed) }()
	switch act.Kind {
	case ActionNoop:
		st.Noop++
	case ActionPushCreate, ActionPushUpdate:
		reply, err := e.push(ctx, base, root, act.RelPath, act.Local)
		if err != nil {
			st.Errors = append(st.Errors, fmt.Sprintf("push %s: %v", act.RelPath, err))
			failed = true
			return
		}
		// The peer held the content already, or was receiving it from
		// another peer (LOC-78): no content went over the link. Neither
		// is a failure; a pending push is planned again next round.
		switch {
		case reply.Pending:
			st.SkippedReceiving++
			tr.skipPush(act.Local.Size, false)
			return true
		case reply.Adopted:
			// The peer produced the content from its own copy under
			// another path (LOC-82).
			st.Adopted++
			st.AdoptedBytes += act.Local.Size
			if reply.Result == ResultConflicted {
				st.Conflicted++
			}
			tr.adopt(act.Local.Size, true)
			return
		case reply.Held:
			st.SkippedHeld++
			tr.skipPush(act.Local.Size, true)
			return
		}
		switch reply.Result {
		case ResultApplied:
			st.Pushed++
			changed = true
		case ResultConflicted:
			st.Pushed++
			st.Conflicted++
			changed = true
		default:
			st.Noop++
		}
	case ActionPushDelete:
		res, err := e.PushDelete(ctx, base, root, act.RelPath, act.Local)
		if err != nil {
			st.Errors = append(st.Errors, fmt.Sprintf("push delete %s: %v", act.RelPath, err))
			failed = true
			return
		}
		if res == ResultApplied || res == ResultConflicted {
			st.Deleted++
			changed = true
		}
		if res == ResultConflicted {
			st.Conflicted++
		}
	case ActionPullCreate, ActionPullUpdate:
		if act.Remote != nil {
			// Claim first (LOC-77): another peer's round may be fetching
			// the same content right now, or may have applied it since
			// this plan was made. Either way it is not fetched again.
			release, ok := e.claimPull(root, act.RelPath, act.Remote.Hash, p.PeerID)
			if !ok {
				st.SkippedInFlight++
				tr.skip(act.Remote.Size)
				return true
			}
			defer release()
			held, err := e.holdsLive(root, act.RelPath, act.Remote.Hash)
			if err != nil {
				st.Errors = append(st.Errors, fmt.Sprintf("pull %s: %v", act.RelPath, err))
				failed = true
				return
			}
			if held {
				// The content is here: adopt the peer's version vector, as
				// a pull_version planned now would.
				if _, err := e.PullVersion(root, act.RelPath, act.Remote); err != nil {
					st.Errors = append(st.Errors, fmt.Sprintf("pull version %s: %v", act.RelPath, err))
					failed = true
					return
				}
				st.SkippedInFlight++
				tr.skip(act.Remote.Size)
				return
			}
			// The content is here under another path (LOC-82): produce
			// it from that copy rather than fetch it.
			if res, adopted, err := e.adoptPull(root, act, p.PeerID); adopted {
				if err != nil {
					st.Errors = append(st.Errors, fmt.Sprintf("pull %s: %v", act.RelPath, err))
					failed = true
					return
				}
				switch res {
				case ResultApplied, ResultConflicted:
					st.Adopted++
					st.AdoptedBytes += act.Remote.Size
					if res == ResultConflicted {
						st.Conflicted++
					}
					tr.adopt(act.Remote.Size, false)
				default:
					st.Noop++
				}
				return
			}
		}
		res, err := e.Pull(ctx, base, p.PeerID, root, act.RelPath, act.Remote)
		if err != nil {
			st.Errors = append(st.Errors, fmt.Sprintf("pull %s: %v", act.RelPath, err))
			failed = true
			return
		}
		switch res {
		case ResultApplied:
			st.Pulled++
			changed = true
		case ResultConflicted:
			st.Pulled++
			st.Conflicted++
			changed = true
		}
	case ActionPullVersion:
		if _, err := e.PullVersion(root, act.RelPath, act.Remote); err != nil {
			st.Errors = append(st.Errors, fmt.Sprintf("pull version %s: %v", act.RelPath, err))
			failed = true
			return
		}
		// Metadata only: no content moved.
		st.Noop++
	case ActionPullDelete:
		res, err := e.PullDelete(ctx, root, act.RelPath, act.Remote)
		if err != nil {
			st.Errors = append(st.Errors, fmt.Sprintf("pull delete %s: %v", act.RelPath, err))
			failed = true
			return
		}
		if res != ResultNoop {
			st.Deleted++
			changed = true
		}
		if act.LinkedTo != "" {
			e.unshareLink(root, act.RelPath, act.LinkedTo)
		}
	}
	return false
}

// adoptPull carries out a planned pull from a local copy of its content,
// when there is one (LOC-82); adopted reports whether there was.
func (e *Engine) adoptPull(root string, act Action, peerID string) (Result, bool, error) {
	meta := Meta{Hash: act.Remote.Hash, Size: act.Remote.Size, LastWriter: act.Remote.LastWriter}
	var err error
	if meta.Version, err = vvector.Parse(act.Remote.Version); err != nil {
		return ResultNoop, false, nil
	}
	res, _, adopted, err := e.adoptLocal(root, act.RelPath, meta, act.AdoptFrom, act.AdoptLink, peerID)
	return res, adopted, err
}

// LocalManifest renders the state database rows for a root. Unless the root
// syncs hidden paths, rows for them are left out: they are local leftovers
// of a build that synced them (tombstoned by reconciliation), and offering
// those tombstones would delete the files on a peer that does sync them.
func (e *Engine) LocalManifest(root string) ([]ManifestEntry, error) {
	docs, err := e.Store.ListDocs(root, true)
	if err != nil {
		return nil, err
	}
	hidden := e.SyncsHidden(root)
	out := make([]ManifestEntry, 0, len(docs))
	for _, d := range docs {
		if !hidden && pathguard.HasHidden(d.RelPath) {
			continue
		}
		out = append(out, ManifestEntry{
			RelPath:    d.RelPath,
			Size:       d.Size,
			Hash:       d.Hash,
			Version:    d.Version.String(),
			LastWriter: d.LastWriter,
			Deleted:    d.Deleted,
		})
	}
	return out, nil
}

// FetchManifest requests a peer's manifest for a root. A manifest that does
// not decode or validate is a *ManifestError.
func (e *Engine) FetchManifest(ctx context.Context, base, root string) (*Manifest, error) {
	q := url.Values{"rootname": []string{e.NameOf(root)}}
	target := Target(base, "/v1/manifest", q)
	body, err := e.do(ctx, http.MethodGet, base, target, nil)
	if err != nil {
		return nil, err
	}
	return DecodeManifest(body, e.NameOf(root))
}

// Push sends the current content of one file to a peer.
func (e *Engine) Push(ctx context.Context, base, root, relpath string, local *ManifestEntry) (Result, error) {
	res, err := e.push(ctx, base, root, relpath, local)
	return res.Result, err
}

// push is Push, returning the peer's whole answer: besides the result,
// whether the peer already held the content (Held: only the version was
// applied, no content crossed the link) or was receiving it from another
// peer (Pending: nothing was applied, and the push is left for the next
// round) (LOC-78).
func (e *Engine) push(ctx context.Context, base, root, relpath string, local *ManifestEntry) (applyResponse, error) {
	noop := applyResponse{Result: ResultNoop}
	rejected := applyResponse{Result: ResultRejected}
	if local == nil {
		return noop, nil
	}
	full, err := pathguard.Join(root, relpath)
	if err != nil {
		return noop, err
	}
	hash, err := DeclaredContentHash(local.Hash)
	if err != nil {
		return rejected, err
	}
	q := url.Values{
		"rootname": []string{e.NameOf(root)},
		"relpath":  []string{relpath},
		"hash":     []string{local.Hash},
		"size":     []string{strconv.FormatInt(local.Size, 10)},
		"version":  []string{local.Version},
		"writer":   []string{local.LastWriter},
	}
	// A large file may have been cut off in an earlier round: ask the peer
	// how much of it arrived and send only the rest (LOC-67). A peer that
	// cannot say (a build before resumable puts answers 404) gets all of
	// it, exactly as before.
	//
	// The question names the path too (LOC-78), so a peer that is already
	// receiving the content from another peer says so, and the put is left
	// for the next round without a byte of it sent.
	var offset int64
	if local.Size >= ResumableMinSize {
		var st PutStatus
		st, err = e.putStatus(ctx, base, root, relpath, local.Hash)
		if ctx.Err() != nil {
			return rejected, ctx.Err()
		}
		if err == nil && st.Pending {
			return applyResponse{Result: ResultNoop, Pending: true}, nil
		}
		offset = st.Offset
		if err != nil || st.Held || offset < 0 || offset > local.Size {
			offset = 0
		}
	}
	// Bytes the peer already holds count as pushed (LOC-66), though not
	// towards the rate: they did not cross the link this round.
	credit(ctx, "push", offset)
	resp, err := e.sendPut(ctx, base, full, q, hash, offset)
	var se *StatusError
	if offset > 0 && errors.As(err, &se) && se.Code == http.StatusConflict {
		// The peer no longer holds what it reported (another upload of
		// the same content, or a sweep, got there first): start over.
		e.logger().Debug("peer refused to resume a put; sending it whole", "relpath", relpath, "err", err)
		credit(ctx, "push", -offset)
		resp, err = e.sendPut(ctx, base, full, q, hash, 0)
	}
	if os.IsNotExist(err) {
		return noop, nil // desired state changed underneath us
	}
	if err != nil {
		return rejected, err
	}
	resBody, err := e.readBody(ctx, resp)
	if err != nil {
		return rejected, err
	}
	res, err := decodeApplyResponse(resBody, relpath)
	if err != nil {
		res.Result = ResultRejected
		return res, err
	}
	if res.Result == ResultConflicted {
		e.adoptConflict(root, res.Conflict, local.Hash)
	}
	return res, nil
}

// sendPut streams the file at full from offset on as a put of the object
// described by q. Each attempt opens the file afresh: the transport closes a
// request body, possibly after Do has returned.
// The file is streamed, never read into memory, so the request is signed
// over the hash it declares rather than one computed from the body
// (LOC-13). Should the file have changed since it was recorded, the
// receiver sees the mismatch once the object is complete and rejects the
// put, as it did when the signature covered the bytes sent.
func (e *Engine) sendPut(ctx context.Context, base, full string, q url.Values, hash []byte, offset int64) (*http.Response, error) {
	f, err := os.Open(full)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	q = maps.Clone(q)
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	target := Target(base, "/v1/put", q)
	// The body waits for the receiver's go-ahead (100 Continue), so a
	// receiver can refuse a put (413, or 409 for a continuation it cannot
	// accept) or answer it without the content, because it holds it or
	// another peer is sending it (LOC-78), before the body is on the wire.
	// A receiver answers either way, so this costs a round trip per put
	// and never a timeout.
	hdr := http.Header{"Expect": []string{"100-continue"}}
	return e.sendWith(ctx, http.MethodPost, base, target, counted(ctx, f, "push"), hash, hdr)
}

// putStatus asks a peer how many bytes of the object with content hash hash
// it holds from an interrupted put, and whether it holds that content at
// relpath already or is receiving it from another peer (LOC-78).
func (e *Engine) putStatus(ctx context.Context, base, root, relpath, hash string) (PutStatus, error) {
	q := url.Values{"hash": []string{hash}, "rootname": []string{e.NameOf(root)}, "relpath": []string{relpath}}
	target := Target(base, "/v1/put/status", q)
	body, err := e.do(ctx, http.MethodGet, base, target, nil)
	if err != nil {
		return PutStatus{}, err
	}
	var st PutStatus
	if err := json.Unmarshal(body, &st); err != nil {
		return PutStatus{}, fmt.Errorf("sync: undecodable put status: %w", err)
	}
	if st.Hash != hash {
		return PutStatus{}, fmt.Errorf("sync: put status for %q, asked for %q", st.Hash, hash)
	}
	return st, nil
}

// adoptConflict records a conflict the peer reported back from a push, so
// that both sides of a divergence can explain it. The root is local, as
// absolute paths differ between machines, and the ID is derived again here
// from the root's name: a peer on an earlier build reports an ID holding its
// own root path (LOC-72). A conflict this peer already knows, open or
// resolved, under whatever ID, is left as it is.
//
// pushedHash is the hash of the version pushed (or deleted). The conflict
// that push recorded involves it on one side; a peer on an earlier build
// reports the newest conflict on the path instead, which may be another
// one, and that is not adopted.
func (e *Engine) adoptConflict(root string, c *store.Conflict, pushedHash string) {
	if c == nil || (c.LocalHash != pushedHash && c.ConflictHash != pushedHash) {
		return
	}
	e.conflictMu.Lock()
	defer e.conflictMu.Unlock()
	if _, err := e.Store.FindConflict(root, c.RelPath, c.LocalHash, c.ConflictHash); err == nil {
		return
	}
	c.ID = ConflictID(e.NameOf(root), c.RelPath, c.LocalHash, c.ConflictHash)
	c.Root = root
	c.ResolvedAt = nil
	if _, err := e.Store.PutConflict(c); err != nil {
		e.logger().Error("record adopted conflict", "err", err)
		return
	}
	if err := e.Store.Audit("sync", "conflict", fmt.Sprintf("%s keep=%s other=%s (reported by peer)", c.RelPath, identity.Short(c.LocalWriter), identity.Short(c.ConflictWriter))); err != nil {
		e.logger().Error("audit conflict", "err", err)
	}
}

// PushDelete propagates a deletion.
func (e *Engine) PushDelete(ctx context.Context, base, root, relpath string, local *ManifestEntry) (Result, error) {
	if local == nil {
		return ResultNoop, nil
	}
	q := url.Values{
		"rootname": []string{e.NameOf(root)},
		"relpath":  []string{relpath},
		"version":  []string{local.Version},
		"writer":   []string{local.LastWriter},
		"hash":     []string{local.Hash},
	}
	target := Target(base, "/v1/delete", q)
	body, err := e.do(ctx, http.MethodPost, base, target, nil)
	if err != nil {
		return ResultRejected, err
	}
	// A rejected delete must fail the round: a clean round is what lets
	// tombstone pruning assume the peer holds the deletion.
	res, err := decodeApplyResponse(body, "delete of "+relpath)
	if err != nil {
		return ResultRejected, err
	}
	if res.Result == ResultConflicted {
		e.adoptConflict(root, res.Conflict, local.Hash)
	}
	return res.Result, nil
}

// Pull downloads a file from the peer peerID, served at base, and applies it
// locally. Any conflict and audit record it writes names peerID (LOC-54).
func (e *Engine) Pull(ctx context.Context, base, peerID, root, relpath string, remote *ManifestEntry) (Result, error) {
	if remote == nil {
		return ResultNoop, nil
	}
	q := url.Values{"rootname": []string{e.NameOf(root)}, "relpath": []string{relpath}}
	meta := Meta{Hash: remote.Hash, Size: remote.Size, LastWriter: remote.LastWriter}
	var err error
	meta.Version, err = vvector.Parse(remote.Version)
	if err != nil {
		return ResultRejected, err
	}
	target := Target(base, "/v1/file", q)
	resp, err := e.send(ctx, http.MethodGet, base, target, nil, emptyBodyHash)
	if err != nil {
		return ResultRejected, err
	}
	defer resp.Body.Close()
	if max := e.MaxFile; max > 0 && resp.ContentLength > max {
		return ResultRejected, fmt.Errorf("pull %s: %w %d", relpath, ErrTooLarge, max)
	}
	// Download first, lock later (LOC-69): the response is staged outside
	// the root, bounded by MaxFile and verified against the manifest's hash
	// and size before Apply takes the path's lock, so a peer that stalls
	// mid-download holds up nothing else (and the request's inactivity
	// watchdog ends the stall).
	staged, release, err := e.staging().Stage(remote.Hash, remote.Size, e.MaxFile, &peerBody{ctx: ctx, r: counted(ctx, resp.Body, "pull")})
	resp.Body.Close()
	if err != nil {
		return ResultRejected, fmt.Errorf("pull %s: %w", relpath, err)
	}
	defer release()
	return e.Apply(root, relpath, meta, staged, peerID)
}

// peerBody reports a failure while reading a peer's response as a transport
// error (or the caller's own cancellation), as do does for buffered bodies.
type peerBody struct {
	ctx context.Context
	r   io.Reader
}

func (b *peerBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err != nil && err != io.EOF {
		if b.ctx.Err() != nil {
			return n, b.ctx.Err()
		}
		return n, &TransportError{Err: err}
	}
	return n, err
}

// PullVersion brings the local version vector of a file in line with a
// peer's when both already hold identical content. A remote vector that
// dominates ours is adopted as is; concurrent vectors are replaced by their
// element-wise maximum, which both peers compute identically. No counter is
// incremented: nothing was written, so no new event happened. If the local
// record or the file on disk no longer matches the planned content, it is
// left for the next plan. Like Apply, it holds the path's lock so a
// concurrent Apply cannot record in between its read and its write.
func (e *Engine) PullVersion(root, relpath string, remote *ManifestEntry) (Result, error) {
	if remote == nil {
		return ResultNoop, nil
	}
	relpath, err := pathguard.CleanRel(relpath)
	if err != nil {
		return ResultRejected, err
	}
	if err := pathguard.CheckHidden(relpath, e.SyncsHidden(root)); err != nil {
		return ResultRejected, err
	}
	defer e.Store.LockPathFor(root, relpath, "sync pull-version")()
	rv, err := vvector.Parse(remote.Version)
	if err != nil {
		return ResultRejected, err
	}
	doc, err := e.Store.GetDoc(root, relpath)
	if errors.Is(err, sql.ErrNoRows) {
		return ResultNoop, nil
	}
	if err != nil {
		return ResultRejected, err
	}
	if doc.Deleted || remote.Deleted || doc.Hash != remote.Hash {
		return ResultNoop, nil
	}
	// An edit the reconciler has not recorded yet is not the content both
	// peers agreed on; adopting the vector for it would misdescribe it.
	if _, drifted, err := e.onDisk(root, relpath, doc, ""); err != nil {
		return ResultRejected, err
	} else if drifted {
		return ResultNoop, nil
	}
	var (
		version vvector.Vector
		writer  string
	)
	switch vvector.Compare(doc.Version, rv) {
	case vvector.Before:
		version, writer = rv, remote.LastWriter
	case vvector.Divergent:
		version = vvector.MergeMax(doc.Version, rv)
		writer = Winner(Version{Writer: doc.LastWriter, Hash: doc.Hash}, Version{Writer: remote.LastWriter, Hash: remote.Hash}).Writer
	default:
		return ResultNoop, nil
	}
	if err := e.record(root, relpath, doc.Hash, doc.Size, version, writer, false); err != nil {
		return ResultRejected, err
	}
	return ResultApplied, nil
}

// PullDelete removes a local file that a peer deleted.
func (e *Engine) PullDelete(_ context.Context, root, relpath string, remote *ManifestEntry) (Result, error) {
	meta := Meta{Deleted: true, LastWriter: remote.LastWriter, Hash: remote.Hash}
	var err error
	if meta.Version, err = ParseManifestVersion(remote.Version); err != nil {
		return ResultRejected, err
	}
	return e.Apply(root, relpath, meta, nil, "")
}

func (e *Engine) do(ctx context.Context, method, base, target string, body []byte) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	sum := sha256.Sum256(body)
	resp, err := e.send(ctx, method, base, target, rdr, sum[:])
	if err != nil {
		return nil, err
	}
	return e.readBody(ctx, resp)
}

// readBody reads and closes a successful response, bounded by
// maxResponse.
func (e *Engine) readBody(ctx context.Context, resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, e.maxResponse()))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransportError{Err: err}
	}
	return out, nil
}

// send issues a signed peer request whose body (nil for none) hashes to
// bodyHash, streaming the body rather than buffering it. An error answer is
// read, closed and returned as an error; on success the caller owns the
// response and must close its body.
func (e *Engine) send(ctx context.Context, method, base, target string, body io.Reader, bodyHash []byte) (*http.Response, error) {
	return e.sendWith(ctx, method, base, target, body, bodyHash, nil)
}

// sendWith is send with extra, unsigned request headers.
//
// The request runs under an inactivity watchdog: when neither a byte of the
// request body is consumed nor a byte of the response arrives for
// TransferIdleTimeout, it is aborted as a transport failure (LOC-69). There
// is no limit on the request as a whole, so a large transfer on a slow but
// live link completes.
func (e *Engine) sendWith(ctx context.Context, method, base, target string, body io.Reader, bodyHash []byte, extra http.Header) (*http.Response, error) {
	rctx, cancel := context.WithCancelCause(ctx)
	wd := newWatchdog(e.idleTimeout(), func() { cancel(errTransferStalled) })
	done := func() { wd.stop(); cancel(nil) }
	if body != nil {
		body = &watchedReader{r: body, wd: wd}
	}
	req, err := http.NewRequestWithContext(rctx, method, base+target, body)
	if err != nil {
		if c, ok := body.(io.Closer); ok {
			c.Close()
		}
		done()
		return nil, err
	}
	for k, v := range extra {
		req.Header[k] = v
	}
	for k, v := range AuthHeadersHash(e.Identity, method, target, bodyHash) {
		req.Header.Set(k, v)
	}
	client := e.HTTPClient
	if client == nil {
		client = defaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		done()
		if ctx.Err() != nil {
			return nil, ctx.Err() // our own cancellation, not the peer's fault
		}
		if context.Cause(rctx) == errTransferStalled {
			err = errTransferStalled
		}
		return nil, &TransportError{Err: err}
	}
	wd.touch()
	resp.Body = &watchedReader{r: resp.Body, wd: wd, rctx: rctx, done: done}
	if resp.StatusCode < 400 {
		return resp, nil
	}
	defer resp.Body.Close()
	// An error answer is a short message; there is no reason to read more.
	out, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &TransportError{Err: err}
	}
	if resp.StatusCode == http.StatusUpgradeRequired {
		peerProto, _ := strconv.Atoi(resp.Header.Get(ProtocolHeader))
		return nil, &IncompatibleError{PeerProtocol: peerProto, Msg: strings.TrimSpace(string(out))}
	}
	msg := strings.TrimSpace(string(out))
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return nil, &StatusError{Method: method, Target: target, Code: resp.StatusCode, Status: resp.Status, Body: msg}
}

// defaultClient sends peer requests when Engine.HTTPClient is nil. It has no
// overall timeout: requests are bounded by their inactivity watchdog.
var defaultClient = &http.Client{}

// DefaultTransferIdleTimeout is how long a peer request may move no bytes in
// either direction before it is aborted.
const DefaultTransferIdleTimeout = 60 * time.Second

// errTransferStalled aborts a request that moved no bytes for the idle
// timeout.
var errTransferStalled = errors.New("sync: transfer stalled: no data within the idle timeout")

func (e *Engine) idleTimeout() time.Duration {
	if e.TransferIdleTimeout > 0 {
		return e.TransferIdleTimeout
	}
	return DefaultTransferIdleTimeout
}

// watchdog calls fire once nothing has touched it for idle.
type watchdog struct {
	idle  time.Duration
	last  atomic.Int64 // unix nanoseconds of the last touch
	timer *time.Timer
	fire  func()
}

func newWatchdog(idle time.Duration, fire func()) *watchdog {
	w := &watchdog{idle: idle, fire: fire}
	w.touch()
	w.timer = time.AfterFunc(idle, w.check)
	return w
}

func (w *watchdog) touch() { w.last.Store(time.Now().UnixNano()) }

func (w *watchdog) check() {
	quiet := time.Since(time.Unix(0, w.last.Load()))
	if quiet >= w.idle {
		w.fire()
		return
	}
	w.timer.Reset(w.idle - quiet)
}

func (w *watchdog) stop() { w.timer.Stop() }

// watchedReader feeds a watchdog with every read that moves bytes. As a
// response body it reports a stall as errTransferStalled and stops the
// watchdog when closed; as a request body it closes what it wraps, as the
// transport expects of a body.
type watchedReader struct {
	r    io.Reader
	wd   *watchdog
	rctx context.Context
	done func()
}

func (w *watchedReader) Read(p []byte) (int, error) {
	n, err := w.r.Read(p)
	if n > 0 {
		w.wd.touch()
	}
	if err != nil && err != io.EOF && w.rctx != nil && context.Cause(w.rctx) == errTransferStalled {
		err = errTransferStalled
	}
	return n, err
}

func (w *watchedReader) Close() error {
	var err error
	if c, ok := w.r.(io.Closer); ok {
		err = c.Close()
	}
	if w.done != nil {
		w.done()
	}
	return err
}

// TransportError is a request that never got an HTTP answer from the peer:
// the dial failed, the connection dropped or the client timed out.
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

// StatusError is an HTTP answer from the peer with an error status.
type StatusError struct {
	Method, Target string
	Code           int
	Status         string
	Body           string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("sync: %s %s: %s: %s", e.Method, e.Target, e.Status, e.Body)
}

// IsTransport reports whether err means the peer could not be reached
// properly: no answer at all, or a server-side (5xx) failure. Only these
// warrant backing off from the peer; a 4xx is the peer answering that it
// will not serve this request, which waiting does not change.
func IsTransport(err error) bool {
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code >= 500
	}
	var te *TransportError
	return errors.As(err, &te)
}

func (e *Engine) maxResponse() int64 {
	if e.MaxFile > 0 {
		return e.MaxFile + 1<<20
	}
	return 1 << 40
}

func (e *Engine) logger() *slog.Logger {
	if e.Log != nil {
		return e.Log
	}
	return slog.Default()
}

// Reason is the short cause of a peer failure for display: the status line
// of an HTTP error answer, the innermost error of a transport failure (such
// as "connection refused" or "i/o timeout"), or else the error itself.
func Reason(err error) string {
	if err == nil {
		return ""
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Status
	}
	var te *TransportError
	if errors.As(err, &te) {
		inner := te.Err
		for {
			next := errors.Unwrap(inner)
			if next == nil {
				break
			}
			inner = next
		}
		if msg := inner.Error(); msg != "" {
			return msg
		}
	}
	return err.Error()
}

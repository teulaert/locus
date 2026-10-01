package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
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
	Notifier   ChangeNotifier
	Hostnames  func() map[string]string
	Log        *slog.Logger
	HTTPClient *http.Client

	// OnPeerError and OnPeerSuccess report per-peer transport outcomes so the
	// caller can surface a peer that answers discovery probes but cannot be
	// reached for transfers, and back off instead of paying a connect timeout
	// every interval.
	OnPeerError   func(peerID string, err error)
	OnPeerSuccess func(peerID string)
	// OnPeerIncompatible reports a peer that refused transfer because it
	// speaks a different sync protocol. It replaces OnPeerError for that
	// peer: the refusal is an answer, so there is nothing to back off from.
	OnPeerIncompatible func(peerID string, err *IncompatibleError)

	inFlight atomic.Bool

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
	Peers      int       `json:"peers"`
	Pushed     int       `json:"pushed"`
	Pulled     int       `json:"pulled"`
	Deleted    int       `json:"deleted"`
	Conflicted int       `json:"conflicted"`
	Noop       int       `json:"noop"`
	Errors     []string  `json:"errors,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	Duration   string    `json:"duration"`
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

// Busy reports whether a round is currently running.
func (e *Engine) Busy() bool { return e.inFlight.Load() }

// Round synchronizes with every online peer, one root at a time. Failures
// against a single peer never abort the round: peers may be offline for
// arbitrary periods.
//
// Only transport failures (the peer could not be dialed, timed out or failed
// with a 5xx) are reported through OnPeerError and so drive backoff. A peer
// that answered with an application-level refusal is reachable: a root it
// does not share (404) is skipped quietly, and other refusals (401, 403,
// 400) are recorded for that root, while the round carries on with the
// remaining roots and peers (LOC-14).
func (e *Engine) Round(ctx context.Context, peers []*peer.Entry) (*Stats, error) {
	st := &Stats{StartedAt: time.Now(), Peers: len(peers)}
	if !e.inFlight.CompareAndSwap(false, true) {
		return st, errors.New("sync: round already running")
	}
	defer e.inFlight.Store(false)
	for _, p := range peers {
		if p.SyncAddr == "" {
			continue
		}
		var (
			transportErr error
			incompatible *IncompatibleError
		)
		answered := false
	roots:
		for _, root := range e.Roots {
			started, errsBefore := time.Now(), len(st.Errors)
			err := e.roundPeer(ctx, p, root, st)
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
			if e.OnPeerSuccess != nil {
				e.OnPeerSuccess(p.PeerID)
			}
		}
	}
	st.Duration = time.Since(st.StartedAt).String()
	return st, nil
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

func (e *Engine) roundPeer(ctx context.Context, p *peer.Entry, root string, st *Stats) error {
	base := "http://" + p.SyncAddr
	remote, err := e.FetchManifest(ctx, base, root)
	if err != nil {
		return err
	}
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
	for _, act := range Plan(local, remoteEntries) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch act.Kind {
		case ActionNoop:
			st.Noop++
		case ActionPushCreate, ActionPushUpdate:
			res, err := e.Push(ctx, base, root, act.RelPath, act.Local)
			if err != nil {
				st.Errors = append(st.Errors, fmt.Sprintf("push %s: %v", act.RelPath, err))
				continue
			}
			switch res {
			case ResultApplied:
				st.Pushed++
			case ResultConflicted:
				st.Pushed++
				st.Conflicted++
			default:
				st.Noop++
			}
		case ActionPushDelete:
			res, err := e.PushDelete(ctx, base, root, act.RelPath, act.Local)
			if err != nil {
				st.Errors = append(st.Errors, fmt.Sprintf("push delete %s: %v", act.RelPath, err))
				continue
			}
			if res == ResultApplied || res == ResultConflicted {
				st.Deleted++
			}
			if res == ResultConflicted {
				st.Conflicted++
			}
		case ActionPullCreate, ActionPullUpdate:
			res, err := e.Pull(ctx, base, p.PeerID, root, act.RelPath, act.Remote)
			if err != nil {
				st.Errors = append(st.Errors, fmt.Sprintf("pull %s: %v", act.RelPath, err))
				continue
			}
			switch res {
			case ResultApplied:
				st.Pulled++
			case ResultConflicted:
				st.Pulled++
				st.Conflicted++
			}
		case ActionPullVersion:
			if _, err := e.PullVersion(root, act.RelPath, act.Remote); err != nil {
				st.Errors = append(st.Errors, fmt.Sprintf("pull version %s: %v", act.RelPath, err))
				continue
			}
			// Metadata only: no content moved.
			st.Noop++
		case ActionPullDelete:
			res, err := e.PullDelete(ctx, root, act.RelPath, act.Remote)
			if err != nil {
				st.Errors = append(st.Errors, fmt.Sprintf("pull delete %s: %v", act.RelPath, err))
				continue
			}
			if res != ResultNoop {
				st.Deleted++
			}
		}
	}
	return nil
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
	if local == nil {
		return ResultNoop, nil
	}
	full, err := pathguard.Join(root, relpath)
	if err != nil {
		return ResultNoop, err
	}
	hash, err := DeclaredContentHash(local.Hash)
	if err != nil {
		return ResultRejected, err
	}
	f, err := os.Open(full)
	if err != nil {
		if os.IsNotExist(err) {
			return ResultNoop, nil // desired state changed underneath us
		}
		return ResultNoop, err
	}
	defer f.Close()
	q := url.Values{
		"rootname": []string{e.NameOf(root)},
		"relpath":  []string{relpath},
		"hash":     []string{local.Hash},
		"size":     []string{strconv.FormatInt(local.Size, 10)},
		"version":  []string{local.Version},
		"writer":   []string{local.LastWriter},
	}
	target := Target(base, "/v1/put", q)
	// The file is streamed, never read into memory, so the request is
	// signed over the hash it declares rather than one computed from the
	// body (LOC-13). Should the file have changed since it was recorded,
	// the receiver's Install sees the mismatch and rejects the put, as it
	// did when the signature covered the bytes sent.
	resp, err := e.send(ctx, http.MethodPost, base, target, f, hash)
	if err != nil {
		return ResultRejected, err
	}
	resBody, err := e.readBody(ctx, resp)
	if err != nil {
		return ResultRejected, err
	}
	res, err := decodeApplyResponse(resBody, relpath)
	if err != nil {
		return ResultRejected, err
	}
	if res.Result == ResultConflicted {
		e.adoptConflict(root, res.Conflict)
	}
	return res.Result, nil
}

// adoptConflict records a conflict the peer reported back from a push, so
// that both sides of a divergence can explain it. The record is canonical,
// but the root is local: absolute paths differ between machines.
func (e *Engine) adoptConflict(root string, c *store.Conflict) {
	if c == nil {
		return
	}
	if existing, err := e.Store.GetConflict(c.ID); err == nil && existing != nil {
		return
	}
	c.Root = root
	c.ResolvedAt = nil
	if err := e.Store.PutConflict(c); err != nil {
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
		e.adoptConflict(root, res.Conflict)
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
	// The response streams straight into Install, which bounds it by
	// MaxFile and verifies the manifest's hash before the rename.
	return e.Apply(root, relpath, meta, &peerBody{ctx: ctx, r: resp.Body}, peerID)
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
	defer e.Store.LockPath(root, relpath)()
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
	req, err := http.NewRequestWithContext(ctx, method, base+target, body)
	if err != nil {
		return nil, err
	}
	for k, v := range AuthHeadersHash(e.Identity, method, target, bodyHash) {
		req.Header.Set(k, v)
	}
	client := e.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err() // our own cancellation, not the peer's fault
		}
		return nil, &TransportError{Err: err}
	}
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

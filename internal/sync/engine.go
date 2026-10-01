package sync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	Store      *store.Store
	Identity   *identity.Identity
	Roots      []string
	RootNames  map[string]string
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

	// paths serialises every read-decide-write on one (root, relpath):
	// server handlers, the local round and PullVersion may all reach the
	// same path at once, and without it the last record wins and one
	// version vanishes without a conflict record (LOC-7).
	paths pathLocks

	// refusals remembers the last application-level refusal per peer and
	// root, so each one is logged once rather than every round.
	refusalMu gosync.Mutex
	refusals  map[string]string

	// afterRead, when set by tests, runs after Apply has read the stored
	// record and before it decides or writes anything.
	afterRead func(root, relpath string)
}

// pathLocks is a set of mutexes keyed by root and relpath, created on demand
// and dropped when the last holder releases them. The zero value is ready.
type pathLocks struct {
	mu gosync.Mutex
	m  map[string]*pathLock
}

type pathLock struct {
	mu   gosync.Mutex
	refs int
}

// lock acquires the mutex for root/relpath and returns its release.
func (p *pathLocks) lock(root, relpath string) func() {
	key := root + "\x00" + relpath
	p.mu.Lock()
	if p.m == nil {
		p.m = make(map[string]*pathLock)
	}
	l := p.m[key]
	if l == nil {
		l = &pathLock{}
		p.m[key] = l
	}
	l.refs++
	p.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		p.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(p.m, key)
		}
		p.mu.Unlock()
	}
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
			err := e.roundPeer(ctx, p, root, st)
			var se *StatusError
			switch {
			case err == nil:
				answered = true
				e.forgetRefusal(p.PeerID, root)
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
	key := peerID + "\x00" + root
	msg := se.Error()
	e.refusalMu.Lock()
	if e.refusals == nil {
		e.refusals = make(map[string]string)
	}
	seen := e.refusals[key] == msg
	e.refusals[key] = msg
	e.refusalMu.Unlock()
	if seen {
		return
	}
	if notShared {
		e.logger().Info("peer does not share root; skipping it", "peer", identity.Short(peerID), "root", RootName(root))
		return
	}
	e.logger().Warn("peer refused sync of root", "peer", identity.Short(peerID), "root", RootName(root), "status", se.Code, "err", se)
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
	for _, act := range Plan(local, remote.Entries) {
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
			res, err := e.Pull(ctx, base, root, act.RelPath, act.Remote)
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

// LocalManifest renders the state database rows for a root.
func (e *Engine) LocalManifest(root string) ([]ManifestEntry, error) {
	docs, err := e.Store.ListDocs(root, true)
	if err != nil {
		return nil, err
	}
	out := make([]ManifestEntry, 0, len(docs))
	for _, d := range docs {
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

// FetchManifest requests a peer's manifest for a root.
func (e *Engine) FetchManifest(ctx context.Context, base, root string) (*Manifest, error) {
	q := url.Values{"rootname": []string{e.NameOf(root)}}
	target := Target(base, "/v1/manifest", q)
	body, err := e.do(ctx, http.MethodGet, base, target, nil, nil)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("sync: malformed manifest from %s: %w", base, err)
	}
	return &m, nil
}

// Push sends the current content of one file to a peer.
func (e *Engine) Push(ctx context.Context, base, root, relpath string, local *ManifestEntry) (Result, error) {
	if local == nil {
		return ResultNoop, nil
	}
	full := filepath.Join(root, filepath.FromSlash(relpath))
	content, err := os.ReadFile(full)
	if err != nil {
		if os.IsNotExist(err) {
			return ResultNoop, nil // desired state changed underneath us
		}
		return ResultNoop, err
	}
	q := url.Values{
		"rootname": []string{e.NameOf(root)},
		"relpath":  []string{relpath},
		"hash":     []string{local.Hash},
		"size":     []string{strconv.FormatInt(local.Size, 10)},
		"version":  []string{local.Version},
		"writer":   []string{local.LastWriter},
	}
	target := Target(base, "/v1/put", q)
	resBody, err := e.do(ctx, http.MethodPost, base, target, content, nil)
	if err != nil {
		return ResultRejected, err
	}
	var res struct {
		Result   Result          `json:"result"`
		Error    string          `json:"error,omitempty"`
		Conflict *store.Conflict `json:"conflict,omitempty"`
	}
	_ = json.Unmarshal(resBody, &res)
	if res.Error != "" {
		return ResultRejected, fmt.Errorf("peer rejected %s: %s", relpath, res.Error)
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
	body, err := e.do(ctx, http.MethodPost, base, target, nil, nil)
	if err != nil {
		return ResultRejected, err
	}
	var res struct {
		Result   Result          `json:"result"`
		Conflict *store.Conflict `json:"conflict,omitempty"`
	}
	_ = json.Unmarshal(body, &res)
	if res.Result == ResultConflicted {
		e.adoptConflict(root, res.Conflict)
	}
	return res.Result, nil
}

// Pull downloads a file from a peer and applies it locally.
func (e *Engine) Pull(ctx context.Context, base, root, relpath string, remote *ManifestEntry) (Result, error) {
	if remote == nil {
		return ResultNoop, nil
	}
	q := url.Values{"rootname": []string{e.NameOf(root)}, "relpath": []string{relpath}}
	target := Target(base, "/v1/file", q)
	body, err := e.do(ctx, http.MethodGet, base, target, nil, nil)
	if err != nil {
		return ResultRejected, err
	}
	meta := Meta{Hash: remote.Hash, Size: remote.Size, LastWriter: remote.LastWriter}
	meta.Version, err = vvector.Parse(remote.Version)
	if err != nil {
		return ResultRejected, err
	}
	return e.Apply(root, relpath, meta, bytes.NewReader(body), base)
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
	defer e.paths.lock(root, relpath)()
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
	meta.Version, _ = vvector.Parse(remote.Version)
	return e.Apply(root, relpath, meta, nil, "")
}

func (e *Engine) do(ctx context.Context, method, base, target string, body []byte, _ http.Header) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+target, rdr)
	if err != nil {
		return nil, err
	}
	for k, v := range AuthHeaders(e.Identity, method, target, body) {
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
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, e.maxResponse()))
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
	if resp.StatusCode >= 400 {
		msg := strings.TrimSpace(string(out))
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return nil, &StatusError{Method: method, Target: target, Code: resp.StatusCode, Status: resp.Status, Body: msg}
	}
	return out, nil
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

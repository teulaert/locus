package sync

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	gosync "sync"
	"time"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/pathguard"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/vvector"
)

// Server exposes the synchronization service on the peer port. It answers
// only authenticated Locus peers and only for configured roots.
type Server struct {
	Engine          *Engine
	Identity        *identity.Identity
	Roots           []string
	AuthorizedPeers []string
	// Known reports whether a peer ID has been discovered (is in the
	// presence table). With an empty AuthorizedPeers list only discovered
	// peers are served; a nil Known then admits nobody.
	Known func(peerID string) bool
	// Rejections aggregates refused requests per remote host. Anyone who can
	// reach the peer port can send them, so they are never written to the
	// audit log one row per request; the owner flushes the tally
	// periodically. Nil only logs them.
	Rejections *store.AuditTally
	Log        *slog.Logger
	// BodyIdleTimeout bounds how long a put may deliver no body bytes
	// (DefaultBodyIdleTimeout when zero).
	BodyIdleTimeout time.Duration

	// hiddenMu guards hiddenLogged, the peer/root pairs whose hidden
	// paths have been refused and logged (once, not per file).
	hiddenMu     gosync.Mutex
	hiddenLogged map[string]bool
}

// Handler returns the authenticated HTTP handler for peer traffic.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/whoami", s.whoami)
	mux.HandleFunc("GET /v1/manifest", s.auth(s.manifest))
	mux.HandleFunc("GET /v1/file", s.auth(s.file))
	mux.HandleFunc("POST /v1/put", s.authPut(s.put))
	mux.HandleFunc("GET /v1/put/status", s.auth(s.putStatus))
	mux.HandleFunc("POST /v1/delete", s.auth(s.del))
	return mux
}

func (s *Server) whoami(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"peer_id":  s.Identity.PeerID,
		"protocol": ProtocolVersion,
		"version":  BuildVersion,
	})
}

// BuildVersion is the Locus build version reported in handshakes.
var BuildVersion = "0.1.0-dev"

// auth admits a request only from an authenticated, authorized peer that
// speaks this build's sync protocol. A protocol mismatch is refused with 426
// Upgrade Required before any handler (and so Apply) runs, so peers of
// different versions never exchange data they could misread.
//
// The request body is never read here. Every request but a put has an empty
// body, and its signature covers the hash of the empty body.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return s.authWith(func(*http.Request) ([]byte, error) { return emptyBodyHash, nil }, next)
}

// authPut authenticates a put before a byte of its body is read: the
// signature covers the content hash the put declares in its (signed) query,
// which is what an honest sender's body hashes to anyway, so peers that
// sign the body they buffered verify unchanged. Install then refuses a body
// that does not hash to the declared value before renaming it into place,
// so an unauthenticated client cannot make this peer write or buffer
// anything, and a tampered body is rejected as before (LOC-13).
func (s *Server) authPut(next http.HandlerFunc) http.HandlerFunc {
	return s.authWith(func(r *http.Request) ([]byte, error) {
		return DeclaredContentHash(r.URL.Query().Get("hash"))
	}, next)
}

// authWith admits a request whose signature verifies over the body hash
// that bodyHash claims for it.
func (s *Server) authWith(bodyHash func(*http.Request) ([]byte, error), next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ProtocolHeader, strconv.Itoa(ProtocolVersion))
		claim, err := bodyHash(r)
		var peerID string
		if err == nil {
			peerID, err = VerifyAuthHeadersHash(r.Header, r.Method, r.URL.RequestURI(), claim)
		}
		if err != nil {
			s.reject(r, "auth-rejected", err.Error())
			http.Error(w, "unauthenticated peer", http.StatusUnauthorized)
			return
		}
		if !s.authorized(peerID) {
			s.reject(r, "unauthorized-peer", peerID)
			http.Error(w, "peer not authorized", http.StatusForbidden)
			return
		}
		if v, err := requestProtocol(r.Header); err != nil || v != ProtocolVersion {
			got := r.Header.Get(ProtocolHeader)
			s.reject(r, "incompatible-protocol", fmt.Sprintf("%s speaks sync protocol %q, this peer %d", identity.Short(peerID), got, ProtocolVersion))
			w.Header().Set("Upgrade", fmt.Sprintf("locus-sync/%d", ProtocolVersion))
			http.Error(w, fmt.Sprintf("incompatible sync protocol %q: this peer speaks %d", got, ProtocolVersion), http.StatusUpgradeRequired)
			return
		}
		next(w, r)
	}
}

// reject records a refused request in the aggregated tally (never as its own
// audit row) and at debug level in the log.
func (s *Server) reject(r *http.Request, action, detail string) {
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	s.Rejections.Note("peer", action, host, detail)
	s.log().Debug("peer request refused", "action", action, "remote", host, "detail", detail)
}

func (s *Server) authorized(peerID string) bool {
	if len(s.AuthorizedPeers) == 0 {
		return s.Known != nil && s.Known(peerID)
	}
	for _, p := range s.AuthorizedPeers {
		if p == peerID {
			return true
		}
	}
	return false
}

// rootFor resolves a peer-supplied root name to the local absolute path.
func (s *Server) rootFor(name string) (string, bool) {
	for _, r := range s.Roots {
		if s.Engine.NameOf(r) == name {
			return r, true
		}
	}
	return "", false
}

func (s *Server) manifest(w http.ResponseWriter, r *http.Request) {
	root, ok := s.rootFor(r.URL.Query().Get("rootname"))
	if !ok {
		http.Error(w, "unknown root", http.StatusNotFound)
		return
	}
	entries, err := s.Engine.LocalManifest(root)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, Manifest{PeerID: s.Identity.PeerID, Root: root, RootName: s.Engine.NameOf(root), Entries: entries, SkipsHidden: !s.Engine.SyncsHidden(root)})
}

func (s *Server) file(w http.ResponseWriter, r *http.Request) {
	root, ok := s.rootFor(r.URL.Query().Get("rootname"))
	if !ok {
		http.Error(w, "unknown root", http.StatusNotFound)
		return
	}
	rel := r.URL.Query().Get("relpath")
	doc, err := s.Engine.Store.GetDoc(root, rel)
	if err != nil || doc.Deleted {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	full, err := pathguard.Join(root, rel)
	if err != nil {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	f, err := os.Open(full)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Locus-Hash", doc.Hash)
	n, _ := io.Copy(w, f)
	s.Engine.servedFiles.Add(1)
	s.Engine.servedBytes.Add(n)
}

// PutCheckWarnAfter is how long a put may take to get from its arrival to
// reading its body (or answering early) before that is logged: the checks
// before the body only read state and take an in-memory claim, so a long
// wait there is a stall worth seeing (LOC-83).
const PutCheckWarnAfter = time.Second

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	arrived := time.Now()
	root, ok := s.rootFor(r.URL.Query().Get("rootname"))
	if !ok {
		http.Error(w, "unknown root", http.StatusNotFound)
		return
	}
	rel, err := relPath(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.refuseHidden(w, r, root, rel) {
		return
	}
	q := r.URL.Query()
	ver, err := vvector.Parse(q.Get("version"))
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	// The declared size only lets an oversized upload be refused before its
	// body is read; the record uses the size Install measures.
	var size int64
	if v := q.Get("size"); v != "" {
		if size, err = strconv.ParseInt(v, 10, 64); err != nil || size < 0 {
			http.Error(w, "bad size", http.StatusBadRequest)
			return
		}
	}
	// offset (resumable puts, LOC-67): the body carries the object from
	// this byte on, continuing a partial object an interrupted put left.
	var offset int64
	if v := q.Get("offset"); v != "" {
		if offset, err = strconv.ParseInt(v, 10, 64); err != nil || offset < 0 || (q.Get("size") != "" && offset > size) {
			http.Error(w, "bad offset", http.StatusBadRequest)
			return
		}
	}
	meta := Meta{Hash: q.Get("hash"), Size: size, Version: ver, LastWriter: q.Get("writer")}
	if max := s.Engine.MaxFile; max > 0 && (size > max || offset > max || (r.ContentLength > 0 && offset+r.ContentLength > max)) {
		// Refused before the body is read; the sender's own limit is its
		// business, this peer's is upload_max_bytes. It bounds the whole
		// object, however many continuations it arrives in.
		http.Error(w, fmt.Sprintf("file exceeds size limit of %d bytes", max), http.StatusRequestEntityTooLarge)
		return
	}
	// One copy at a time (LOC-78): every peer that holds a file a new peer
	// lacks plans a push of it, so the same content may be offered by
	// several at once, and pulled by this peer's own rounds besides. The
	// first transfer claims it; any other is answered at once, before its
	// body is read, as in progress, and the pusher plans it again next
	// round against what arrived. Content this peer already holds at the
	// path is not received either: the put's version is applied to the
	// record as receiving the identical bytes would have.
	peerID := r.Header.Get("X-Locus-Peer")
	releaseClaim, claimed := s.Engine.claimPut(root, rel, meta.Hash, peerID)
	if !claimed {
		s.notePutChecks(rel, peerID, arrived, "pending")
		s.answerEarly(w, r, offset, size, map[string]any{"result": string(ResultNoop), "pending": true})
		return
	}
	defer releaseClaim()
	if res, held, err := s.Engine.applyHeld(root, rel, meta, peerID); err != nil {
		// The full put decides, with the content in hand.
		s.log().Debug("checking for held content failed; receiving the put", "relpath", rel, "err", err)
	} else if held {
		s.notePutChecks(rel, peerID, arrived, "held")
		reply := putResponse(res, nil)
		reply["held"] = true
		s.answerEarly(w, r, offset, size, reply)
		return
	}
	checked := s.notePutChecks(rel, peerID, arrived, "receive")
	// Receive first, lock later (LOC-69): the whole body is staged outside
	// the root and verified against the declared (and signed) hash before
	// Apply takes the path's lock, so a sender that stalls mid-body holds
	// up nothing but its own request. A large object is staged as a
	// resumable partial object (LOC-67).
	staged, release, ok := s.receive(w, r, meta.Hash, offset, size)
	if !ok {
		return
	}
	defer release()
	s.Engine.receivedFiles.Add(1)
	received := time.Now()
	res, conflict, err := s.Engine.applyOp(root, rel, meta, staged, r.Header.Get("X-Locus-Peer"))
	s.log().Debug("put timing", "relpath", rel, "peer", identity.Short(peerID),
		"checks", checked.Sub(arrived).Round(time.Microsecond).String(),
		"receive", received.Sub(checked).Round(time.Microsecond).String(),
		"apply", time.Since(received).Round(time.Microsecond).String())
	if errors.Is(err, ErrTooLarge) {
		http.Error(w, fmt.Sprintf("file exceeds size limit of %d bytes", s.Engine.MaxFile), http.StatusRequestEntityTooLarge)
		return
	}
	if err != nil {
		s.log().Warn("apply failed", "relpath", rel, "err", err)
		s.audit("sync", "apply-rejected", rel+": "+err.Error())
		writeJSON(w, http.StatusOK, map[string]string{"result": string(ResultRejected), "error": err.Error()})
		return
	}
	// A conflict is reported back so the pusher records it too: both peers
	// must be able to explain the divergence, not only the one that detected
	// it first.
	writeJSON(w, http.StatusOK, putResponse(res, conflict))
}

// notePutChecks returns the time a put's checks before its body ended,
// logging them when they took longer than PutCheckWarnAfter. outcome is
// what they decided: receive the body, or answer early because the
// content is held or pending.
func (s *Server) notePutChecks(rel, peerID string, arrived time.Time, outcome string) time.Time {
	now := time.Now()
	if d := now.Sub(arrived); d > PutCheckWarnAfter {
		s.log().Warn("put waited long before reading its body", "relpath", rel, "peer", identity.Short(peerID), "outcome", outcome, "waited", d.Round(time.Millisecond).String())
	}
	return now
}

// answerEarly answers a put without receiving its content (LOC-78). A
// pusher that sent "Expect: 100-continue" is waiting for the go-ahead
// before it sends the body; answering without reading it means the server
// never sends 100 Continue, and the body never crosses the link (the
// connection is closed after the reply, as it must be when a body was
// announced and not read). A pusher that sent the body along with the
// request (a build before LOC-78, for files below ResumableMinSize) has it
// read and discarded first, so it is not cut off mid-request and reads the
// reply like any other.
func (s *Server) answerEarly(w http.ResponseWriter, r *http.Request, offset, size int64, reply map[string]any) {
	if !expectsContinue(r) {
		limit := s.Engine.MaxFile
		if r.URL.Query().Get("size") != "" && (limit <= 0 || size-offset < limit) {
			limit = size - offset
		}
		if limit <= 0 {
			limit = 1 << 62
		}
		body := &idleBody{r: r.Body, rc: http.NewResponseController(w), idle: s.bodyIdleTimeout()}
		if _, err := io.Copy(io.Discard, io.LimitReader(body, limit+1)); err != nil {
			s.log().Debug("discarding a put body failed", "relpath", r.URL.Query().Get("relpath"), "err", err)
		}
	}
	writeJSON(w, http.StatusOK, reply)
}

// expectsContinue reports whether a request waits for 100 Continue before
// sending its body.
func expectsContinue(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("Expect")), "100-continue")
}

// DefaultBodyIdleTimeout is how long a put may go without delivering a byte
// of its body before the receiver gives up on it (408). What arrived of a
// resumable put is kept for the sender to continue.
const DefaultBodyIdleTimeout = 60 * time.Second

// receive stages a put's body and returns the complete, verified content.
// When it reports false it has answered the request.
func (s *Server) receive(w http.ResponseWriter, r *http.Request, hash string, offset, size int64) (*Staged, func(), bool) {
	if r.URL.Query().Get("size") == "" {
		size = -1
	}
	body := &idleBody{r: r.Body, rc: http.NewResponseController(w), idle: s.bodyIdleTimeout()}
	var (
		st      *Staged
		release func()
		err     error
	)
	switch ps := s.Engine.Partials; {
	case ps != nil && (offset > 0 || size >= ResumableMinSize):
		st, release, err = ps.Receive(hash, offset, size, s.Engine.MaxFile, body)
	case offset > 0:
		http.Error(w, "resumable uploads are not enabled", http.StatusConflict)
		return nil, nil, false
	default:
		// As before staging, the declared size only refuses an oversized
		// put early; the content is bounded by the cap and the hash.
		st, release, err = s.Engine.staging().Stage(hash, -1, s.Engine.MaxFile, body)
	}
	rel := r.URL.Query().Get("relpath")
	switch {
	case err == nil:
		return st, release, true
	case errors.Is(err, ErrTooLarge):
		http.Error(w, fmt.Sprintf("file exceeds size limit of %d bytes", s.Engine.MaxFile), http.StatusRequestEntityTooLarge)
	case errors.Is(err, ErrPartialOffset):
		// The sender starts again from zero.
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, ErrContentMismatch), errors.Is(err, ErrIncomplete):
		s.log().Warn("apply failed", "relpath", rel, "err", err)
		s.audit("sync", "apply-rejected", rel+": "+err.Error())
		writeJSON(w, http.StatusOK, map[string]string{"result": string(ResultRejected), "error": err.Error()})
	case errors.Is(err, os.ErrDeadlineExceeded):
		s.log().Warn("upload stalled; giving up on it", "relpath", rel, "peer", identity.Short(r.Header.Get("X-Locus-Peer")), "idle", s.bodyIdleTimeout().String())
		http.Error(w, "upload stalled", http.StatusRequestTimeout)
	default:
		// The body broke off (the sender went away) or could not be
		// written; a resumable partial keeps what arrived.
		s.log().Debug("upload interrupted", "relpath", rel, "err", err)
		http.Error(w, "upload interrupted: "+err.Error(), http.StatusBadRequest)
	}
	return nil, nil, false
}

func (s *Server) bodyIdleTimeout() time.Duration {
	if s.BodyIdleTimeout > 0 {
		return s.BodyIdleTimeout
	}
	return DefaultBodyIdleTimeout
}

// idleBody reads a request body under an inactivity deadline: each read
// must deliver within idle, however long the whole body takes. It replaces
// the server's ReadTimeout for the request, which would cut off a large
// transfer on a slow link.
type idleBody struct {
	r    io.Reader
	rc   *http.ResponseController
	idle time.Duration
}

func (b *idleBody) Read(p []byte) (int, error) {
	// Unsupported (a test recorder) leaves the server's own limits.
	_ = b.rc.SetReadDeadline(time.Now().Add(b.idle))
	return b.r.Read(p)
}

// putStatus answers how many bytes of an object (by content hash) this peer
// holds from an interrupted put, so the sender can continue from there. A
// peer without resumable puts answers 0, and a build that predates them 404.
func (s *Server) putStatus(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	if _, err := DeclaredContentHash(hash); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	st := PutStatus{Hash: hash}
	// A sender that names the path (LOC-78) also learns whether the content
	// is here already, or on its way from another peer: it then skips the
	// put, or sends it only to have its version applied.
	if q := r.URL.Query(); q.Get("rootname") != "" && q.Get("relpath") != "" {
		if root, ok := s.rootFor(q.Get("rootname")); ok {
			if rel, err := relPath(r); err == nil && pathguard.CheckHidden(rel, s.Engine.SyncsHidden(root)) == nil {
				st.Pending = s.Engine.inFlight(root, rel, hash)
				if !st.Pending {
					st.Held, _ = s.Engine.holdsLive(root, rel, hash)
				}
			}
		}
	}
	if s.Engine.Partials != nil && !st.Held && !st.Pending {
		var err error
		if st.Offset, err = s.Engine.Partials.Offset(hash); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) del(w http.ResponseWriter, r *http.Request) {
	root, ok := s.rootFor(r.URL.Query().Get("rootname"))
	if !ok {
		http.Error(w, "unknown root", http.StatusNotFound)
		return
	}
	rel, err := relPath(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.refuseHidden(w, r, root, rel) {
		return
	}
	q := r.URL.Query()
	ver, err := vvector.Parse(q.Get("version"))
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	meta := Meta{Deleted: true, Version: ver, LastWriter: q.Get("writer"), Hash: q.Get("hash")}
	res, conflict, err := s.Engine.applyOp(root, rel, meta, nil, r.Header.Get("X-Locus-Peer"))
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{"result": string(ResultRejected), "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, putResponse(res, conflict))
}

// refuseHidden answers a put or delete of a hidden path on a root that does
// not sync hidden paths, reporting whether it did. The refusal is the
// rejected result Apply gives a traversal: an application answer, so the
// sender records it for the path and neither backs off nor treats the peer
// as unreachable. Only the first refusal per peer and root is logged and
// audited, so a peer that keeps offering a whole .git directory cannot
// flood either, while a user who expected .notes/ to arrive can still find
// out why it did not.
func (s *Server) refuseHidden(w http.ResponseWriter, r *http.Request, root, rel string) bool {
	err := pathguard.CheckHidden(rel, s.Engine.SyncsHidden(root))
	if err == nil {
		return false
	}
	peerID := r.Header.Get("X-Locus-Peer")
	name := s.Engine.NameOf(root)
	key := peerID + "\x00" + root
	s.hiddenMu.Lock()
	if s.hiddenLogged == nil {
		s.hiddenLogged = make(map[string]bool)
	}
	first := !s.hiddenLogged[key]
	s.hiddenLogged[key] = true
	s.hiddenMu.Unlock()
	if first {
		s.log().Warn("refusing hidden paths from peer; set sync_hidden on the root to accept them", "peer", identity.Short(peerID), "root", name, "first", rel)
		s.audit("sync", "hidden-rejected", fmt.Sprintf("%s from %s (root %s); further hidden paths from this peer are not logged", rel, identity.Short(peerID), name))
	}
	writeJSON(w, http.StatusOK, map[string]string{"result": string(ResultRejected), "error": err.Error()})
	return true
}

// putResponse is the reply to an apply-style request, carrying the conflict
// record that this request's apply wrote when it diverged. It is that
// record, not the newest open conflict on the path: another conflict there
// (an older one, or one a concurrent request recorded) is not the one the
// pusher has to adopt (LOC-72).
func putResponse(res Result, conflict *store.Conflict) map[string]any {
	out := map[string]any{"result": string(res)}
	if res == ResultConflicted && conflict != nil {
		out["conflict"] = conflict
	}
	return out
}

// relPath returns the request's relpath, rejecting anything pathguard would
// not accept (traversal, absolute paths) before it reaches the engine.
func relPath(r *http.Request) (string, error) {
	rel := r.URL.Query().Get("relpath")
	if rel == "" {
		return "", errors.New("missing relpath")
	}
	return pathguard.CleanRel(rel)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) audit(actor, action, detail string) {
	_ = s.Engine.Store.Audit(actor, action, detail)
}

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/pathguard"
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
	Log   *slog.Logger
}

// Handler returns the authenticated HTTP handler for peer traffic.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/whoami", s.whoami)
	mux.HandleFunc("GET /v1/manifest", s.auth(s.manifest))
	mux.HandleFunc("GET /v1/file", s.auth(s.file))
	mux.HandleFunc("POST /v1/put", s.auth(s.put))
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

// ProtocolVersion is the sync protocol version spoken by this build.
const ProtocolVersion = 1

// BuildVersion is the Locus build version reported in handshakes.
var BuildVersion = "0.1.0-dev"

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, s.Engine.MaxFile+1<<20))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		peerID, err := VerifyAuthHeaders(r.Header, r.Method, r.URL.RequestURI(), body)
		if err != nil {
			s.audit("peer", "auth-rejected", err.Error())
			http.Error(w, "unauthenticated peer", http.StatusUnauthorized)
			return
		}
		if !s.authorized(peerID) {
			s.audit("peer", "unauthorized-peer", peerID)
			http.Error(w, "peer not authorized", http.StatusForbidden)
			return
		}
		next(w, r.WithContext(withBody(r.Context(), body)))
	}
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
	writeJSON(w, http.StatusOK, Manifest{PeerID: s.Identity.PeerID, Root: root, RootName: s.Engine.NameOf(root), Entries: entries})
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
	io.Copy(w, f)
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
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
	q := r.URL.Query()
	ver, err := vvector.Parse(q.Get("version"))
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	size, _ := strconv.ParseInt(q.Get("size"), 10, 64)
	meta := Meta{Hash: q.Get("hash"), Size: size, Version: ver, LastWriter: q.Get("writer")}
	body := bodyFrom(r.Context())
	res, err := s.Engine.Apply(root, rel, meta, bytes.NewReader(body), r.Header.Get("X-Locus-Peer"))
	if err != nil {
		s.log().Warn("apply failed", "relpath", rel, "err", err)
		s.audit("sync", "apply-rejected", rel+": "+err.Error())
		writeJSON(w, http.StatusOK, map[string]string{"result": string(ResultRejected), "error": err.Error()})
		return
	}
	// A conflict is reported back so the pusher records it too: both peers
	// must be able to explain the divergence, not only the one that detected
	// it first.
	writeJSON(w, http.StatusOK, putResponse(s, root, rel, res))
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
	q := r.URL.Query()
	ver, err := vvector.Parse(q.Get("version"))
	if err != nil {
		http.Error(w, "bad version", http.StatusBadRequest)
		return
	}
	meta := Meta{Deleted: true, Version: ver, LastWriter: q.Get("writer"), Hash: q.Get("hash")}
	res, err := s.Engine.Apply(root, rel, meta, nil, r.Header.Get("X-Locus-Peer"))
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{"result": string(ResultRejected), "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, putResponse(s, root, rel, res))
}

// putResponse is the reply to an apply-style request, carrying the conflict
// record when the apply diverged.
func putResponse(s *Server, root, rel string, res Result) map[string]any {
	out := map[string]any{"result": string(res)}
	if res == ResultConflicted {
		if c, err := s.Engine.Store.GetConflictFor(root, rel); err == nil && c != nil {
			out["conflict"] = c
		}
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

type bodyKey struct{}

func withBody(ctx context.Context, b []byte) context.Context {
	return context.WithValue(ctx, bodyKey{}, b)
}

func bodyFrom(ctx context.Context) []byte {
	if b, ok := ctx.Value(bodyKey{}).([]byte); ok {
		return b
	}
	return nil
}

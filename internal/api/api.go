// Package api exposes the versioned HTTP API used by the PWA and by remote
// tools. It calls the same services as the CLI and never duplicates
// behaviour. The API binds to configured/trusted interfaces only and requires
// an application-level bearer token, so generic network access does not
// grant document access.
package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/teulaert/locus/internal/app"
	"github.com/teulaert/locus/internal/identity"
	"github.com/teulaert/locus/internal/pathguard"
	"github.com/teulaert/locus/internal/search"
	"github.com/teulaert/locus/internal/store"
	"github.com/teulaert/locus/internal/sync"
	"github.com/teulaert/locus/internal/web"
)

// Handler returns the versioned API plus the embedded PWA. It fails when the
// API token cannot be obtained, so the API is never served without one.
func Handler(a *app.App) (http.Handler, error) {
	tok, err := a.APIToken()
	if err != nil {
		return nil, fmt.Errorf("api token: %w", err)
	}
	if tok == "" {
		return nil, errors.New("api token: empty token")
	}
	s := &server{app: a, log: a.Log, token: tok}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "peer_id": a.Identity.PeerID})
	})
	mux.HandleFunc("GET /", s.serveUI)

	mux.HandleFunc("GET /api/v1/status", s.auth(s.status))
	mux.HandleFunc("GET /api/v1/peers", s.auth(s.peers))
	mux.HandleFunc("GET /api/v1/search", s.auth(s.searchList))
	mux.HandleFunc("GET /api/v1/files", s.auth(s.listFiles))
	mux.HandleFunc("GET /api/v1/files/content", s.auth(s.getFile))
	mux.HandleFunc("POST /api/v1/files", s.auth(s.uploadFile))
	mux.HandleFunc("DELETE /api/v1/files", s.auth(s.deleteFile))
	mux.HandleFunc("POST /api/v1/files/move", s.auth(s.moveFile))
	mux.HandleFunc("POST /api/v1/directories", s.auth(s.mkdir))
	mux.HandleFunc("GET /api/v1/conflicts", s.auth(s.conflicts))
	mux.HandleFunc("POST /api/v1/conflicts/resolve", s.auth(s.resolveConflict))
	mux.HandleFunc("POST /api/v1/sync", s.auth(s.syncNow))
	mux.HandleFunc("GET /api/v1/index", s.auth(s.indexStatus))
	return logRequests(s.log, mux), nil
}

type server struct {
	app   *app.App
	log   *slog.Logger
	token string
}

// ---------- auth ----------

func (s *server) auth(next http.HandlerFunc) http.HandlerFunc {
	want := s.token
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		// An empty token would compare equal to a missing header, so it
		// authenticates nothing rather than everything.
		if want == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			// Counted per remote host rather than written as an audit row
			// per request, so refused requests cannot grow state.db.
			host := r.RemoteAddr
			if h, _, err := net.SplitHostPort(host); err == nil {
				host = h
			}
			s.app.Rejections.Note("api", "unauthorized", host, r.Method+" "+r.URL.Path)
			writeErr(w, http.StatusUnauthorized, errors.New("missing or invalid bearer token"))
			return
		}
		next(w, r)
	}
}

// ---------- status ----------

func (s *server) status(w http.ResponseWriter, r *http.Request) {
	st, err := s.app.BuildStatus(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *server) peers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"peers": s.app.PresenceSnapshot(r.Context())})
}

func (s *server) syncNow(w http.ResponseWriter, r *http.Request) {
	st, err := s.app.SyncNow(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *server) indexStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.app.IndexStatus(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// ---------- search ----------

func (s *server) searchList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := q.Get("q")
	if query == "" {
		writeErr(w, http.StatusBadRequest, errors.New("q is required"))
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	opts := search.Options{PathPrefix: q.Get("path"), Type: q.Get("type"), Limit: limit}
	// root is optional: absent searches every root, present scopes the
	// search to that configured root (the PWA's root selector).
	if rp := q.Get("root"); rp != "" {
		root, err := s.resolveRoot(rp)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		opts.Root = root
	}
	// The response is the same typed value `locus search --json` (or
	// `--context --json`) prints.
	var resp any
	var err error
	if q.Get("context") == "1" || q.Get("context") == "true" {
		budget, _ := strconv.Atoi(q.Get("budget"))
		resp, err = s.app.SearchWithContext(r.Context(), query, opts, budget)
	} else {
		resp, err = s.app.SearchDocuments(r.Context(), query, opts)
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// ---------- files ----------

// resolveRoot selects the configured root a request addresses. rootParam is
// the root exactly as the client sent it, already decoded from the query
// string or JSON body; empty means the first configured root.
func (s *server) resolveRoot(rootParam string) (string, error) {
	roots := s.app.Roots()
	if len(roots) == 0 {
		return "", errors.New("no roots configured")
	}
	if rootParam == "" {
		return roots[0], nil
	}
	// Return the configured spelling, not the client's: callers compare it
	// against roots stored by the reconciler and the index.
	clean := filepath.Clean(rootParam)
	for _, r := range roots {
		if filepath.Clean(r) == clean {
			return r, nil
		}
	}
	return "", fmt.Errorf("root %q is not configured", rootParam)
}

// resolve turns an API path into an absolute filesystem path inside a root.
// The root is a plain argument, never re-read from the request: handlers that
// take it from a JSON body must not round-trip it through a query string,
// where `+`, `&`, `%` and `#` get mangled (LOC-21). Query-string callers pass
// r.URL.Query().Get("root"), deliberately query-only: r.FormValue would parse
// the request body as a form, and a form parse consumes an upload body (curl
// defaults to application/x-www-form-urlencoded), silently storing an empty
// file.
func (s *server) resolve(rootParam, raw string) (root, rel, full string, err error) {
	root, err = s.resolveRoot(rootParam)
	if err != nil {
		return "", "", "", err
	}
	rel = strings.TrimPrefix(filepath.ToSlash(raw), "/")
	full, err = pathguard.Join(root, rel)
	if err != nil {
		return "", "", "", err
	}
	return root, rel, full, nil
}

func (s *server) listFiles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dir := q.Get("path")
	if dir == "" {
		dir = "/"
	}
	var root, rel, full string
	var err error
	if dir == "/" || dir == "." {
		// The root itself is addressable without a relative path.
		root, err = s.resolveRoot(q.Get("root"))
		full, rel = root, "/"
	} else {
		root, rel, full, err = s.resolve(q.Get("root"), dir)
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	st, err := os.Stat(full)
	if errors.Is(err, fs.ErrNotExist) {
		writeErr(w, http.StatusNotFound, errors.New("directory not found"))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if !st.IsDir() {
		writeErr(w, http.StatusBadRequest, errors.New("path is not a directory"))
		return
	}
	entries, err := os.ReadDir(full)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	type FileEntry struct {
		Name    string `json:"name"`
		Path    string `json:"path"`
		IsDir   bool   `json:"is_dir"`
		Size    int64  `json:"size"`
		Mode    string `json:"mode"`
		ModTime int64  `json:"mtime"`
		Hash    string `json:"hash,omitempty"`
	}
	out := []FileEntry{}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		relEntry := strings.TrimSuffix(rel, "/") + "/" + e.Name()
		relEntry = strings.TrimPrefix(relEntry, "/")
		fe := FileEntry{Name: e.Name(), Path: relEntry, IsDir: e.IsDir(), Size: info.Size(), Mode: info.Mode().String(), ModTime: info.ModTime().Unix()}
		if !e.IsDir() && info.Mode().IsRegular() {
			if doc, err := s.app.Store.GetDoc(root, relEntry); err == nil {
				fe.Hash = doc.Hash
			}
		}
		out = append(out, fe)
	}
	writeJSON(w, http.StatusOK, map[string]any{"root": root, "path": "/" + strings.TrimPrefix(rel, "/"), "entries": out})
}

func (s *server) getFile(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	_, _, full, err := s.resolve(q.Get("root"), q.Get("path"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	st, err := os.Stat(full)
	if err != nil || st.IsDir() {
		writeErr(w, http.StatusNotFound, errors.New("file not found"))
		return
	}
	f, err := os.Open(full)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	defer f.Close()
	// Synced files are arbitrary peer content (an .html file is served as
	// text/html), so never let a browser render one in the API origin: no
	// sniffing, and always a download. The PWA fetches the bytes with the
	// bearer header and previews them from a blob: URL instead (LOC-11).
	w.Header().Set("Content-Type", contentTypeFor(full))
	w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	disp := mime.FormatMediaType("attachment", map[string]string{"filename": filepath.Base(full)})
	if disp == "" {
		disp = "attachment"
	}
	w.Header().Set("Content-Disposition", disp)
	io.Copy(w, f)
}

// uploadFile writes the request body to the normal filesystem. There is no
// separate "web upload store": the reconcile loop notices the change, the
// sync engine propagates it and index work is queued like any other change.
func (s *server) uploadFile(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	relRaw := q.Get("path")
	relRaw2 := strings.TrimPrefix(filepath.ToSlash(relRaw), "/")
	var targetDir string
	if strings.HasSuffix(relRaw2, "/") || relRaw2 == "" {
		name := q.Get("name")
		if name == "" {
			name = r.Header.Get("X-Locus-Filename")
		}
		if name == "" {
			writeErr(w, http.StatusBadRequest, errors.New("upload requires a name (query parameter or X-Locus-Filename header)"))
			return
		}
		targetDir = relRaw2 + name
	} else {
		targetDir = relRaw2
	}
	root, rel, _, err := s.resolve(q.Get("root"), targetDir)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	limit := s.app.Cfg.UploadMaxBytes
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// A short read here means something upstream consumed the body; failing
	// loudly is the only way to keep a truncated upload from being recorded as
	// a complete, valid document.
	if r.ContentLength >= 0 && r.ContentLength <= limit && int64(len(body)) != r.ContentLength {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("incomplete upload: expected %d bytes, read %d", r.ContentLength, len(body)))
		return
	}
	if int64(len(body)) > limit {
		writeErr(w, http.StatusRequestEntityTooLarge, fmt.Errorf("upload exceeds limit of %d bytes", limit))
		return
	}
	sum := sha256.Sum256(body)
	hash := fmt.Sprintf("%x", sum)
	if _, _, err := sync.Install(root, rel, strings.NewReader(string(body)), hash, limit); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	s.audit("api", "upload", fmt.Sprintf("%s/%s (%d bytes)", filepath.Base(root), rel, len(body)))
	// Enter the normal pipeline immediately.
	s.app.NotifyChange(root, rel, false)
	writeJSON(w, http.StatusCreated, map[string]any{"path": "/" + strings.TrimPrefix(rel, "/"), "size": len(body), "hash": hash})
}

func (s *server) deleteFile(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	root, rel, full, err := s.resolve(q.Get("root"), q.Get("path"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// RemoveAll succeeds on a path that does not exist, which would answer
	// 200, audit a delete and queue an unindex for nothing (LOC-28). Lstat,
	// not Stat, so a dangling symlink can still be removed.
	if _, err := os.Lstat(full); errors.Is(err, fs.ErrNotExist) {
		writeErr(w, http.StatusNotFound, errors.New("file not found"))
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if err := os.RemoveAll(full); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	s.audit("api", "delete", root+"/"+rel)
	s.app.NotifyChange(root, rel, true)
	writeJSON(w, http.StatusOK, map[string]string{"deleted": "/" + strings.TrimPrefix(rel, "/")})
}

type moveRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
	Root string `json:"root,omitempty"`
}

func (s *server) moveFile(w http.ResponseWriter, r *http.Request) {
	var req moveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	fromRoot, fromRel, fromFull, err := s.resolve(req.Root, req.From)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	_, toRel, toFull, err := s.resolve(req.Root, req.To)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// Check the source before creating the destination's parents, so a move
	// of a missing file answers 404 and leaves no empty folders behind.
	if _, err := os.Lstat(fromFull); errors.Is(err, fs.ErrNotExist) {
		writeErr(w, http.StatusNotFound, errors.New("source not found"))
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(toFull), 0o755); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if err := os.Rename(fromFull, toFull); errors.Is(err, fs.ErrNotExist) {
		writeErr(w, http.StatusNotFound, errors.New("source not found"))
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	s.audit("api", "move", fromRel+" -> "+toRel)
	s.app.NotifyChange(fromRoot, fromRel, true)
	s.app.NotifyChange(fromRoot, toRel, false)
	writeJSON(w, http.StatusOK, map[string]string{"from": fromRel, "to": toRel})
}

func (s *server) mkdir(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
		Root string `json:"root,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	root, rel, full, err := s.resolve(req.Root, req.Path)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := os.MkdirAll(full, 0o755); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	s.audit("api", "mkdir", root+"/"+rel)
	writeJSON(w, http.StatusCreated, map[string]string{"path": "/" + strings.TrimPrefix(rel, "/")})
}

// ---------- conflicts ----------

func (s *server) conflicts(w http.ResponseWriter, r *http.Request) {
	list, err := s.app.Store.ListConflicts()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if list == nil {
		list = []*store.Conflict{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"conflicts": list})
}

func (s *server) resolveConflict(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.app.Store.ResolveConflict(req.ID); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	s.audit("api", "conflict-resolve", req.ID)
	writeJSON(w, http.StatusOK, map[string]string{"resolved": req.ID})
}

// ---------- PWA ----------

func (s *server) serveUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" || r.URL.Path == "/index.html" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(web.Index())
		return
	}
	if r.URL.Path == "/manifest.webmanifest" {
		w.Header().Set("Content-Type", "application/manifest+json")
		w.Write(web.Manifest())
		return
	}
	if r.URL.Path == "/app.js" {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.Write(web.AppJS())
		return
	}
	if r.URL.Path == "/sw.js" {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.Write(web.ServiceWorker())
		return
	}
	if r.URL.Path == "/style.css" {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Write(web.Stylesheet())
		return
	}
	http.NotFound(w, r)
}

// ---------- helpers ----------

func contentTypeFor(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".pdf":
		return "application/pdf"
	case ".txt", ".md", ".log":
		return "text/plain; charset=utf-8"
	case ".json":
		return "application/json"
	case ".html":
		return "text/html; charset=utf-8"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	default:
		return "application/octet-stream"
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// writeErr answers with {"error": message}. Filesystem errors carry the
// absolute host path (stat /home/you/Documents/x: ...), which is none of the
// client's business, so they are reduced to the operation and its cause
// (LOC-28). Handlers report a missing path with a fixed message instead.
func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": sanitizeErr(err).Error()})
}

// sanitizeErr drops the host path from *fs.PathError and *os.LinkError,
// keeping the operation and the underlying cause.
func sanitizeErr(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return fmt.Errorf("%s: %w", pe.Op, pe.Err)
	}
	var le *os.LinkError
	if errors.As(err, &le) {
		return fmt.Errorf("%s: %w", le.Op, le.Err)
	}
	return err
}

func (s *server) audit(actor, action, detail string) {
	_ = s.app.Store.Audit(actor, action, detail)
}

func logRequests(log *slog.Logger, h http.Handler) http.Handler {
	if log == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			log.Debug("api", "method", r.Method, "path", r.URL.Path, "dur", time.Since(start).String(), "peer", identity.Short(r.Header.Get("X-Locus-Peer")))
		}
	})
}

package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	gosync "sync"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/vvector"
)

// scriptedPeer answers /v1/manifest with a fixed body and /v1/put and
// /v1/delete with another, and counts every request that is not a manifest
// fetch.
type scriptedPeer struct {
	srv      *httptest.Server
	mu       gosync.Mutex
	actions  []string
	manifest string
	apply    string
}

func newScriptedPeer(t *testing.T, manifest, apply string) *scriptedPeer {
	t.Helper()
	p := &scriptedPeer{manifest: manifest, apply: apply}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/manifest" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(p.manifest))
			return
		}
		p.mu.Lock()
		p.actions = append(p.actions, r.URL.Path)
		p.mu.Unlock()
		if r.URL.Path == "/v1/put" || r.URL.Path == "/v1/delete" {
			_, _ = w.Write([]byte(p.apply))
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *scriptedPeer) requests() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.actions...)
}

const remoteWriter = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// LOC-15: a manifest that is malformed in any way fails decoding for that
// root. Before, a corrupt version string parsed as an empty vector, which
// precedes everything, so the planner pushed over (or deleted from) the peer.
// The round must report the error, plan nothing for the root, not record the
// root as cleanly synchronized, and still count the peer as having answered.
func TestRoundRejectsMalformedManifest(t *testing.T) {
	const remoteBody = "the peer's copy\n"
	remoteHash := fswatch.HashBytes([]byte(remoteBody))
	entry := func(fields string) string {
		return `{"peer_id":"` + remoteWriter + `","root_name":"docs","entries":[` + fields + `]}`
	}
	valid := fmt.Sprintf(`{"relpath":"doc.txt","size":%d,"hash":%q,"version":"%s:1","last_writer":%q}`, len(remoteBody), remoteHash, remoteWriter, remoteWriter)
	withField := func(old, new string) string {
		if !strings.Contains(valid, old) {
			t.Fatalf("fixture lacks %q", old)
		}
		return entry(strings.Replace(valid, old, new, 1))
	}
	cases := []struct {
		name, body string
	}{
		{"truncated JSON", `{"peer_id":"` + remoteWriter + `","entries":[`},
		{"entries of the wrong type", `{"peer_id":"x","root_name":"docs","entries":"doc.txt"}`},
		{"size of the wrong type", withField(fmt.Sprintf(`"size":%d`, len(remoteBody)), `"size":"16"`)},
		{"missing entries", `{"peer_id":"` + remoteWriter + `","root_name":"docs"}`},
		{"null entries", `{"peer_id":"` + remoteWriter + `","root_name":"docs","entries":null}`},
		{"missing peer_id", `{"root_name":"docs","entries":[` + valid + `]}`},
		{"unknown root", strings.Replace(entry(valid), `"root_name":"docs"`, `"root_name":"elsewhere"`, 1)},
		{"corrupt version", withField(`"version":"`+remoteWriter+`:1"`, `"version":"garbage"`)},
		{"empty version", withField(`"version":"`+remoteWriter+`:1"`, `"version":""`)},
		{"missing version", withField(`"version":"`+remoteWriter+`:1",`, ``)},
		{"negative counter", withField(`"version":"`+remoteWriter+`:1"`, `"version":"`+remoteWriter+`:-1"`)},
		{"zero counter", withField(`"version":"`+remoteWriter+`:1"`, `"version":"`+remoteWriter+`:0"`)},
		{"peer listed twice", withField(`"version":"`+remoteWriter+`:1"`, `"version":"`+remoteWriter+`:1,`+remoteWriter+`:2"`)},
		{"missing last_writer", withField(`,"last_writer":"`+remoteWriter+`"`, ``)},
		{"missing hash", withField(`"hash":"`+remoteHash+`",`, ``)},
		{"missing relpath", withField(`"relpath":"doc.txt",`, ``)},
		{"traversal relpath", withField(`"relpath":"doc.txt"`, `"relpath":"../doc.txt"`)},
		{"non-canonical relpath", withField(`"relpath":"doc.txt"`, `"relpath":"./doc.txt"`)},
		{"negative size", withField(fmt.Sprintf(`"size":%d`, len(remoteBody)), `"size":-1`)},
		{"duplicate relpath", entry(valid + "," + valid)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngineEnv(t)
			me := e.Identity.PeerID
			// Both files would be pushed if the manifest were trusted: the
			// first is unknown to the peer, the second is ahead of an empty
			// remote vector.
			e.putFile(t, "mine.txt", "only here\n", vvector.New().Increment(me), me)
			e.putFile(t, "doc.txt", "our copy\n", vvector.New().Increment(me), me)
			e.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
			var probe roundProbe
			probe.attach(e.Engine)
			peer := newScriptedPeer(t, tc.body, `{"result":"applied"}`)

			st, err := e.Round(context.Background(), peerAt(peer.srv))
			if err != nil {
				t.Fatal(err)
			}
			if len(st.Errors) != 1 || !strings.Contains(st.Errors[0], "malformed manifest") {
				t.Fatalf("errors = %v, want one malformed-manifest error", st.Errors)
			}
			if got := peer.requests(); len(got) != 0 {
				t.Fatalf("round acted on a malformed manifest: %v", got)
			}
			if st.Pushed+st.Pulled+st.Deleted+st.Conflicted+st.Noop != 0 {
				t.Fatalf("stats = %+v, want nothing planned", st)
			}
			if len(probe.failed) != 0 || len(probe.ok) != 1 {
				t.Fatalf("callbacks failed=%v ok=%v; the peer answered, so no backoff", probe.failed, probe.ok)
			}
			if rounds, err := e.Store.PeerRounds(e.root); err != nil || len(rounds) != 0 {
				t.Fatalf("peer rounds = %v, %v; a failed root must not count as synchronized", rounds, err)
			}
		})
	}

	// The same fixture, well formed, does sync: the cases above fail on
	// the malformation alone.
	t.Run("valid control", func(t *testing.T) {
		e := newEngineEnv(t)
		me := e.Identity.PeerID
		e.putFile(t, "mine.txt", "only here\n", vvector.New().Increment(me), me)
		e.putFile(t, "doc.txt", "our copy\n", vvector.New().Increment(me), me)
		peer := newScriptedPeer(t, entry(valid), `{"result":"applied"}`)
		st, err := e.Round(context.Background(), peerAt(peer.srv))
		if err != nil {
			t.Fatal(err)
		}
		// mine.txt is new to the peer; doc.txt diverged and is pushed for
		// the peer to resolve.
		if len(st.Errors) != 0 || st.Pushed != 2 {
			t.Fatalf("stats = %+v, want both files pushed without errors", st)
		}
	})
}

// LOC-15: PushDelete surfaces every answer that is not an accepted delete.
// A rejection (the LOC-34 case), an undecodable body or an unknown result
// all fail the delete, count nothing as deleted, and keep the round from
// recording the peer as holding our tombstones.
func TestPushDeleteSurfacesRejection(t *testing.T) {
	const body = "soon deleted\n"
	cases := []struct {
		name, answer, want string
	}{
		{"rejected", `{"result":"rejected","error":"disk full"}`, "disk full"},
		{"rejected without reason", `{"result":"rejected"}`, "rejected delete"},
		{"undecodable", `<html>proxy error</html>`, "undecodable"},
		{"unknown result", `{"result":"maybe"}`, "unknown result"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEngineEnv(t)
			me := e.Identity.PeerID
			hash := fswatch.HashBytes([]byte(body))
			e.putFile(t, "gone.txt", body, vvector.New().Increment(me), me)
			remote := e.manifestEntry(t, "gone.txt")
			// Our tombstone is one event ahead of what the peer holds.
			if err := e.record(e.root, "gone.txt", hash, 0, vvector.New().Increment(me).Increment(me), me, true); err != nil {
				t.Fatal(err)
			}
			manifest, _ := jsonManifest(remoteWriter, "docs", remote)
			peer := newScriptedPeer(t, manifest, tc.answer)

			res, err := e.PushDelete(context.Background(), peer.srv.URL, e.root, "gone.txt", ptr(e.manifestEntry(t, "gone.txt")))
			if err == nil || !strings.Contains(err.Error(), tc.want) || res != ResultRejected {
				t.Fatalf("PushDelete = %s, %v; want rejected with %q", res, err, tc.want)
			}

			st, err := e.Round(context.Background(), peerAt(peer.srv))
			if err != nil {
				t.Fatal(err)
			}
			if len(st.Errors) != 1 || !strings.Contains(st.Errors[0], "push delete gone.txt") || st.Deleted != 0 {
				t.Fatalf("stats = %+v, want the rejected delete reported and not counted", st)
			}
			if rounds, err := e.Store.PeerRounds(e.root); err != nil || len(rounds) != 0 {
				t.Fatalf("peer rounds = %v, %v; a rejected delete must not count as delivered", rounds, err)
			}
		})
	}
}

// Push decodes its answer the same way.
func TestPushSurfacesUndecodableAnswer(t *testing.T) {
	e := newEngineEnv(t)
	me := e.Identity.PeerID
	e.putFile(t, "new.txt", "fresh\n", vvector.New().Increment(me), me)
	peer := newScriptedPeer(t, "", `not json`)
	res, err := e.Push(context.Background(), peer.srv.URL, e.root, "new.txt", ptr(e.manifestEntry(t, "new.txt")))
	if err == nil || res != ResultRejected {
		t.Fatalf("Push = %s, %v; want an error for an undecodable answer", res, err)
	}
}

func TestDecodeManifestErrorType(t *testing.T) {
	_, err := DecodeManifest([]byte(`{}`), "docs")
	var me *ManifestError
	if !errors.As(err, &me) || me.Root != "docs" || IsTransport(err) {
		t.Fatalf("err = %v; want a *ManifestError for root docs, not a transport error", err)
	}
	if _, err := DecodeManifest([]byte(`{"peer_id":"p","root_name":"docs","entries":[]}`), "docs"); err != nil {
		t.Fatalf("empty root rejected: %v", err)
	}
}

// Plan never reads an unparsable vector as empty, whichever side it is on.
func TestPlanIgnoresUnparsableVectors(t *testing.T) {
	good := ManifestEntry{RelPath: "a.txt", Hash: "h1", Version: "p:2", LastWriter: "p"}
	bad := ManifestEntry{RelPath: "a.txt", Hash: "h2", Version: "garbage", LastWriter: "q"}
	for _, pair := range [][2]ManifestEntry{{good, bad}, {bad, good}} {
		for _, deleted := range []bool{false, true} {
			l, r := pair[0], pair[1]
			r.Deleted = deleted
			acts := Plan([]ManifestEntry{l}, []ManifestEntry{r})
			if len(acts) != 1 || acts[0].Kind != ActionNoop {
				t.Fatalf("Plan(%+v, %+v) = %+v, want a noop", l, r, acts)
			}
		}
	}
}

func jsonManifest(peerID, rootName string, entries ...ManifestEntry) (string, error) {
	var sb strings.Builder
	err := EncodeJSON(&sb, Manifest{PeerID: peerID, RootName: rootName, Entries: entries})
	return sb.String(), err
}

func ptr[T any](v T) *T { return &v }

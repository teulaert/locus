package update

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// fakeRelease is one release the fake GitHub serves.
type fakeRelease struct {
	Tag        string
	Prerelease bool
	Draft      bool
	Created    time.Time
	Files      map[string][]byte
}

// signedRelease builds a release of commit for channel with one binary per
// platform ("linux/amd64": content), signed with priv. mutate may change
// the manifest before it is signed; tamper may change the files after.
func signedRelease(t *testing.T, priv ed25519.PrivateKey, tag, channel, commit string, commitTime time.Time, bins map[string][]byte, mutate func(*Manifest)) fakeRelease {
	t.Helper()
	m := Manifest{Version: commit[:7], Commit: commit, CommitTime: commitTime, Created: commitTime.Add(time.Minute), Channel: channel}
	files := map[string][]byte{}
	for plat, b := range bins {
		goos, goarch, _ := strings.Cut(plat, "/")
		name := ArtifactName(goos, goarch)
		m.Artifacts = append(m.Artifacts, Artifact{OS: goos, Arch: goarch, File: name, Size: int64(len(b)), SHA256: sum(b)})
		files[name] = b
	}
	if mutate != nil {
		mutate(&m)
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	files[ManifestFile] = data
	files[SignatureFile] = EncodeSignature(ed25519.Sign(priv, data))
	return fakeRelease{Tag: tag, Prerelease: channel == ChannelMain, Created: commitTime.Add(time.Minute), Files: files}
}

// fakeGitHub serves the releases API and asset downloads for Repo.
type fakeGitHub struct {
	srv *httptest.Server

	mu          sync.Mutex
	releases    []fakeRelease
	lists       int // release list requests answered 200
	notModified int // release list requests answered 304
	downloads   map[string]int
	// limit, when set, answers the next list request instead.
	limit func(w http.ResponseWriter) bool
}

func newFakeGitHub(t *testing.T, rels ...fakeRelease) *fakeGitHub {
	t.Helper()
	g := &fakeGitHub{releases: rels, downloads: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/teulaert/locus/releases", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		if r.Header.Get("User-Agent") == "" {
			http.Error(w, "GitHub requires a User-Agent", http.StatusForbidden)
			return
		}
		if g.limit != nil && g.limit(w) {
			g.limit = nil
			return
		}
		var out []map[string]any
		for _, rel := range g.releases {
			var assets []map[string]any
			for _, name := range slices.Sorted(maps.Keys(rel.Files)) {
				b := rel.Files[name]
				assets = append(assets, map[string]any{"name": name, "size": len(b), "browser_download_url": g.srv.URL + "/download/" + rel.Tag + "/" + name})
			}
			out = append(out, map[string]any{"tag_name": rel.Tag, "name": rel.Tag, "draft": rel.Draft, "prerelease": rel.Prerelease, "created_at": rel.Created, "published_at": rel.Created, "assets": assets})
		}
		body, _ := json.Marshal(out)
		etag := `"` + sum(body)[:16] + `"`
		if r.Header.Get("If-None-Match") == etag {
			g.notModified++
			w.WriteHeader(http.StatusNotModified)
			return
		}
		g.lists++
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("GET /download/{tag}/{name}", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		for _, rel := range g.releases {
			if rel.Tag == r.PathValue("tag") {
				if b, ok := rel.Files[r.PathValue("name")]; ok {
					g.downloads[r.PathValue("tag")+"/"+r.PathValue("name")]++
					_, _ = w.Write(b)
					return
				}
			}
		}
		http.NotFound(w, r)
	})
	g.srv = httptest.NewServer(mux)
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGitHub) set(rels ...fakeRelease) {
	g.mu.Lock()
	g.releases = rels
	g.mu.Unlock()
}

func (g *fakeGitHub) count(what string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch what {
	case "lists":
		return g.lists
	case "304":
		return g.notModified
	}
	return g.downloads[what]
}

// installed is a fake installed binary in its own directory.
func installed(t *testing.T, content string) string {
	t.Helper()
	exe := filepath.Join(t.TempDir(), "locus")
	if err := os.WriteFile(exe, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return exe
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const (
	oldCommit = "0123456aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	newCommit = "89abcdebbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// testUpdater is an updater for linux/amd64 running oldCommit (built at
// t0), replacing exe, trusting pub, reading releases from g.
func testUpdater(t *testing.T, g *fakeGitHub, pub ed25519.PublicKey, exe, channel string) *Updater {
	t.Helper()
	u := New(Options{Channel: channel, Interval: time.Hour, URL: g.srv.URL, DataDir: t.TempDir(),
		Running: Build{Version: "0123456", Commit: oldCommit, Time: t0}})
	u.Key, u.Exe, u.GOOS, u.GOARCH = pub, exe, "linux", "amd64"
	return u
}

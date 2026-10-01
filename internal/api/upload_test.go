package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
)

// patternReader yields an endless deterministic byte stream without holding
// it in memory.
type patternReader struct{ i uint64 }

func (p *patternReader) Read(b []byte) (int, error) {
	for k := range b {
		p.i++
		b[k] = byte(p.i*2654435761>>13) ^ byte(p.i)
	}
	return len(b), nil
}

// apiServer serves the API of a fresh app and returns it with its token and
// root directory.
func apiServer(t *testing.T) (*httptest.Server, string, string) {
	t.Helper()
	a := testApp(t, "secret")
	h, err := Handler(a)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	a.Cfg.UploadMaxBytes = 64 << 20
	return srv, "secret", a.Cfg.Roots[0].Path
}

func post(t *testing.T, srv *httptest.Server, token, path string, body io.Reader, length int64) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func assertNoTempFiles(t *testing.T, root string) {
	t.Helper()
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && strings.HasPrefix(d.Name(), fswatch.TempPrefix) {
			t.Errorf("temporary file left behind: %s", p)
		}
		return nil
	})
}

// LOC-13: an upload used to be read into memory twice (io.ReadAll, then a
// string copy) before being installed. It now streams into the temporary
// file, so a 32 MiB upload allocates a small fraction of its size.
func TestUploadStreamsWithBoundedMemory(t *testing.T) {
	const size = 32 << 20
	srv, token, root := apiServer(t)
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(&patternReader{}, size)); err != nil {
		t.Fatal(err)
	}
	want := hex.EncodeToString(h.Sum(nil))

	for _, tc := range []struct {
		name   string
		length int64
	}{{"declared length", size}, {"chunked", -1}} {
		name := strings.ReplaceAll(tc.name, " ", "-") + ".bin"
		var code int
		var out map[string]any
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		code, out = post(t, srv, token, "/api/v1/files?path=/&name="+name, io.LimitReader(&patternReader{}, size), tc.length)
		runtime.ReadMemStats(&after)
		if code != http.StatusCreated || out["hash"] != want || out["size"] != float64(size) {
			t.Fatalf("%s: status %d, reply %v", tc.name, code, out)
		}
		if n := after.TotalAlloc - before.TotalAlloc; n > size/4 {
			t.Errorf("%s: a %d byte upload allocated %d bytes", tc.name, size, n)
		}
		if got, _, _ := fswatch.HashFile(filepath.Join(root, name)); got != want {
			t.Fatalf("%s: installed %s, want %s", tc.name, got, want)
		}
	}
	assertNoTempFiles(t, root)
}

// An upload over upload_max_bytes is refused with 413 whether or not it
// declares its length, and leaves nothing behind.
func TestUploadOverLimitIsRefused(t *testing.T) {
	srv, token, root := apiServer(t)
	const limit = 64 << 20
	for _, length := range []int64{limit + 1, -1} {
		code, out := post(t, srv, token, "/api/v1/files?path=/&name=big.bin", io.LimitReader(&patternReader{}, limit+1), length)
		if code != http.StatusRequestEntityTooLarge {
			t.Errorf("length %d: status %d, reply %v; want 413", length, code, out)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "big.bin")); !os.IsNotExist(err) {
		t.Fatalf("an oversized upload was installed: %v", err)
	}
	assertNoTempFiles(t, root)
}

// LOC-13: the JSON endpoints decoded r.Body without a bound.
func TestJSONBodiesAreBounded(t *testing.T) {
	srv, token, _ := apiServer(t)
	huge := `{"id":"` + strings.Repeat("x", maxJSONBody) + `"}`
	for _, path := range []string{"/api/v1/files/move", "/api/v1/directories", "/api/v1/conflicts/resolve"} {
		if code, out := post(t, srv, token, path, strings.NewReader(huge), int64(len(huge))); code != http.StatusRequestEntityTooLarge {
			t.Errorf("%s: status %d, reply %v; want 413", path, code, out)
		}
	}
}

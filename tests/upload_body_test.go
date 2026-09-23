package tests

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Scenario 12 boundary — an upload is the request body, whatever the client
// chose to call it. curl defaults to application/x-www-form-urlencoded, and a
// form parse of that body would leave nothing for the handler: the file landed
// as a valid, empty document. Uploads are therefore asserted byte-for-byte for
// every content type a client is likely to send.
func TestUploadBodySurvivesEveryContentType(t *testing.T) {
	c := newCluster(t, 1)
	a := c.nodes[0]
	payload := []byte("Invoice 2026-0042 for the solar panels: 6400 EUR, payable within 30 days.\n")

	types := []string{
		"application/x-www-form-urlencoded", // what curl sends by default
		"application/octet-stream",
		"text/plain; charset=utf-8",
		"multipart/form-data; boundary=----x",
		"",
	}
	for i, ct := range types {
		status, body := a.apiWithContentType(t, "POST", "/api/v1/files?path=/uploads/&name="+fmt.Sprint(i)+".txt", a.Token, ct, payload)
		if status != http.StatusCreated {
			t.Fatalf("upload with Content-Type %q: status %d body %s", ct, status, body)
		}
		stats := decode(t, body)
		if stats["size"] != float64(len(payload)) {
			t.Fatalf("Content-Type %q: reported size %v, want %d (an empty file means the body was consumed before the handler)", ct, stats["size"], len(payload))
		}
		full := filepath.Join(a.Root, "uploads", fmt.Sprintf("%d.txt", i))
		got, err := os.ReadFile(full)
		if err != nil {
			t.Fatalf("Content-Type %q: %v", ct, err)
		}
		if string(got) != string(payload) {
			t.Fatalf("Content-Type %q: stored %d bytes (%q), want %d", ct, len(got), truncate(string(got)), len(payload))
		}
		if !strings.Contains(string(got), "solar panels") {
			t.Fatalf("Content-Type %q: content lost", ct)
		}
	}

	// An empty file is still an explicit, legitimate upload.
	status, body := a.apiWithContentType(t, "POST", "/api/v1/files?path=/&name=empty.txt", a.Token, "application/x-www-form-urlencoded", nil)
	if status != http.StatusCreated {
		t.Fatalf("empty upload: status %d body %s", status, body)
	}
	if got, err := os.ReadFile(filepath.Join(a.Root, "empty.txt")); err != nil || len(got) != 0 {
		t.Fatalf("empty upload wrote %q (%v)", got, err)
	}
}

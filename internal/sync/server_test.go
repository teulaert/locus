package sync

import "testing"

func TestServerAuthorized(t *testing.T) {
	known := map[string]bool{"discovered": true}
	s := &Server{Known: func(id string) bool { return known[id] }}

	// Empty allowlist: only peers in the presence table (README: "empty
	// accepts any peer this one has discovered").
	if !s.authorized("discovered") {
		t.Fatal("discovered peer rejected with an empty allowlist")
	}
	if s.authorized("stranger") {
		t.Fatal("undiscovered peer accepted with an empty allowlist")
	}

	// Without a presence source nothing counts as discovered.
	if (&Server{}).authorized("discovered") {
		t.Fatal("nil Known must fail closed")
	}

	// An explicit allowlist is authoritative either way.
	s.AuthorizedPeers = []string{"listed"}
	if !s.authorized("listed") {
		t.Fatal("listed peer rejected")
	}
	if s.authorized("discovered") {
		t.Fatal("discovered but unlisted peer accepted")
	}
}

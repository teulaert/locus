package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenAndFTS(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Index.Exec(`INSERT INTO fts_chunks (rowid, text) VALUES (1, 'solar panel invoice')`); err != nil {
		t.Fatalf("fts5: %v", err)
	}
	var id int
	err = s.Index.QueryRow(`SELECT rowid FROM fts_chunks WHERE fts_chunks MATCH 'solar AND invoice'`).Scan(&id)
	if err != nil || id != 1 {
		t.Fatalf("match: id=%d err=%v", id, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state.db")); err != nil {
		t.Fatal(err)
	}
}

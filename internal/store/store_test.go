package store

import (
	"database/sql"
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

// pending_arrivals lives only in state.db. An index.db migrated by an older
// build still holds an unused copy; reopening drops it (LOC-30).
func TestIndexHasNoPendingArrivals(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	exists := func(db interface {
		QueryRow(string, ...any) *sql.Row
	}) bool {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'pending_arrivals'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	if exists(s.Index) || !exists(s.State) {
		t.Fatal("pending_arrivals must exist in state.db only")
	}
	// Simulate an index.db from before the cleanup migration.
	if _, err := s.Index.Exec(`CREATE TABLE pending_arrivals (root TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Index.Exec(`DELETE FROM schema_version WHERE version = (SELECT MAX(version) FROM schema_version)`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if exists(s.Index) {
		t.Fatal("reopening did not drop the leftover index.db pending_arrivals")
	}
}

// The search connections read index.db alongside the single writer: an open
// write transaction neither blocks them nor shows them uncommitted rows, and
// they cannot write themselves.
func TestReadConnectionsRunBesideWriter(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.IndexDocument("/r", "a.txt", "h1", 1, []*Chunk{{Text: "committed text", Hash: "c1"}}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Index.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO documents (root, relpath, hash, size, indexed_at) VALUES ('/r', 'b.txt', 'h2', 1, 0)`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.Read.QueryRow(`SELECT COUNT(*) FROM documents`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("reader during write transaction: count=%d err=%v; want the 1 committed row", n, err)
	}
	if _, err := s.Read.Exec(`DELETE FROM documents`); err == nil {
		t.Fatal("read connection accepted a write")
	}
}

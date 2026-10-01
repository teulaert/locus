// Package store provides the two local SQLite databases of a Locus peer:
//
//	state.db — synchronization correctness state (documents, versions,
//	         conflicts, audit log). Never disposable.
//	index.db — derived, disposable search state (documents copy, chunks,
//	         FTS5 index, embeddings, job queue). Deleting it is always safe;
//	         `locus index rebuild` recreates it entirely from local files.
package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Store bundles the state and derived-index databases of one peer.
type Store struct {
	Dir   string
	State *sql.DB
	Index *sql.DB
}

// Open opens (creating when necessary) both databases under dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	st := &Store{Dir: dir}
	var err error
	if st.State, err = openFile(filepath.Join(dir, "state.db")); err != nil {
		return nil, fmt.Errorf("open state.db: %w", err)
	}
	if err := migrateState(st.State); err != nil {
		return nil, fmt.Errorf("migrate state.db: %w", err)
	}
	if st.Index, err = openFile(filepath.Join(dir, "index.db")); err != nil {
		return nil, fmt.Errorf("open index.db: %w", err)
	}
	if err := migrateIndex(st.Index); err != nil {
		return nil, fmt.Errorf("migrate index.db: %w", err)
	}
	return st, nil
}

func openFile(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	// SQLite still serializes writers; a single connection avoids SQLITE_BUSY
	// lock upgrades while keeping transactions simple.
	db.SetMaxOpenConns(1)
	return db, nil
}

// Close closes both databases.
func (s *Store) Close() error {
	e1 := s.State.Close()
	e2 := s.Index.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

func migrate(db *sql.DB, name string, migrations []string) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return err
	}
	var cur int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&cur); err != nil {
		return err
	}
	for i := cur; i < len(migrations); i++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[i]); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_version (version) VALUES (?)`, i+1); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func migrateState(db *sql.DB) error {
	return migrate(db, "state", []string{
		`CREATE TABLE docs (
			root          TEXT NOT NULL,
			relpath       TEXT NOT NULL,
			size          INTEGER NOT NULL,
			hash          TEXT NOT NULL,
			version       TEXT NOT NULL,
			last_writer   TEXT NOT NULL,
			deleted       INTEGER NOT NULL DEFAULT 0,
			updated_at    INTEGER NOT NULL,
			PRIMARY KEY (root, relpath)
		)`,
		`CREATE TABLE conflicts (
			id            TEXT PRIMARY KEY,
			root          TEXT NOT NULL,
			relpath       TEXT NOT NULL,
			local_path    TEXT NOT NULL,
			conflict_path TEXT NOT NULL,
			local_hash    TEXT NOT NULL,
			conflict_hash TEXT NOT NULL,
			local_writer  TEXT NOT NULL,
			conflict_writer TEXT NOT NULL,
			created_at    INTEGER NOT NULL,
			resolved_at   INTEGER
		)`,
		`CREATE TABLE audit (
			seq     INTEGER PRIMARY KEY AUTOINCREMENT,
			ts      INTEGER NOT NULL,
			actor   TEXT NOT NULL,
			action  TEXT NOT NULL,
			detail  TEXT NOT NULL
		)`,
		`CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		// Provenance of an incoming transfer, recorded before the file is put
		// in place. If the process dies between the atomic rename and the
		// document record, reconciliation adopts this instead of mistaking a
		// received file for one authored here.
		`CREATE TABLE pending_arrivals (
			root       TEXT NOT NULL,
			relpath    TEXT NOT NULL,
			hash       TEXT NOT NULL,
			version    TEXT NOT NULL,
			writer     TEXT NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (root, relpath)
		)`,
		// The start of the last clean synchronization round with each peer,
		// per root. A tombstone older than every peer's entry has been
		// reconciled against all of them and may be pruned (LOC-34).
		`CREATE TABLE peer_rounds (
			root       TEXT NOT NULL,
			peer_id    TEXT NOT NULL,
			synced_at  INTEGER NOT NULL,
			PRIMARY KEY (root, peer_id)
		)`,
	})
}

func migrateIndex(db *sql.DB) error {
	return migrate(db, "index", []string{
		`CREATE TABLE documents (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			root       TEXT NOT NULL,
			relpath    TEXT NOT NULL,
			hash       TEXT NOT NULL,
			size       INTEGER NOT NULL,
			indexed_at INTEGER NOT NULL,
			UNIQUE (root, relpath)
		)`,
		`CREATE TABLE chunks (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			doc_id    INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
			ordinal   INTEGER NOT NULL,
			text      TEXT NOT NULL,
			page      INTEGER,
			section   TEXT,
			hash      TEXT NOT NULL,
			chunkver  INTEGER NOT NULL
		)`,
		`CREATE INDEX idx_chunks_doc ON chunks (doc_id)`,
		`CREATE VIRTUAL TABLE fts_chunks USING fts5(
			text,
			content='chunks',
			content_rowid='id',
			tokenize='unicode61 remove_diacritics 2'
		)`,
		`CREATE TRIGGER chunks_ai AFTER INSERT ON chunks BEGIN
			INSERT INTO fts_chunks (rowid, text) VALUES (new.id, new.text);
		END`,
		`CREATE TRIGGER chunks_ad AFTER DELETE ON chunks BEGIN
			INSERT INTO fts_chunks (fts_chunks, rowid, text) VALUES ('delete', old.id, old.text);
		END`,
		`CREATE TRIGGER chunks_au AFTER UPDATE ON chunks BEGIN
			INSERT INTO fts_chunks (fts_chunks, rowid, text) VALUES ('delete', old.id, old.text);
			INSERT INTO fts_chunks (rowid, text) VALUES (new.id, new.text);
		END`,
		`CREATE TABLE embeddings (
			chunk_id INTEGER PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
			model    TEXT NOT NULL,
			dims     INTEGER NOT NULL,
			vector   BLOB NOT NULL,
			stale    INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE jobs (
			id           INTEGER PRIMARY KEY AUTOINCREMENT,
			kind         TEXT NOT NULL,
			root         TEXT NOT NULL,
			relpath      TEXT NOT NULL,
			hash         TEXT NOT NULL,
			state        TEXT NOT NULL DEFAULT 'pending',
			attempts     INTEGER NOT NULL DEFAULT 0,
			last_error   TEXT NOT NULL DEFAULT '',
			priority     INTEGER NOT NULL DEFAULT 0,
			available_at INTEGER NOT NULL DEFAULT 0,
			updated_at   INTEGER NOT NULL,
			UNIQUE (kind, root, relpath)
		)`,
		`CREATE INDEX idx_jobs_state ON jobs (state, priority DESC, id)`,
		`CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		// Provenance of an incoming transfer, recorded before the file is put
		// in place. If the process dies between the atomic rename and the
		// document record, reconciliation adopts this instead of mistaking a
		// received file for one authored here.
		`CREATE TABLE pending_arrivals (
			root       TEXT NOT NULL,
			relpath    TEXT NOT NULL,
			hash       TEXT NOT NULL,
			version    TEXT NOT NULL,
			writer     TEXT NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (root, relpath)
		)`,
		// generation identifies one claim of a job; rerun marks a running job
		// that was re-enqueued and must be processed again when it finishes.
		`ALTER TABLE jobs ADD COLUMN generation INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE jobs ADD COLUMN rerun INTEGER NOT NULL DEFAULT 0`,
		// no_text records why a document has no derived text (no extractor,
		// extraction failed, empty text). Such a row marks the file as
		// processed at its hash so it is not re-extracted on every pass.
		`ALTER TABLE documents ADD COLUMN no_text TEXT NOT NULL DEFAULT ''`,
		// chunkver is the ChunkVersion a document's chunks were cut with, so
		// NeedsIndex can tell a chunker change apart from "already indexed".
		// Existing rows take the oldest version among their chunks; rows
		// without chunks get 0, which only matters for documents with text.
		`ALTER TABLE documents ADD COLUMN chunkver INTEGER NOT NULL DEFAULT 0`,
		`UPDATE documents SET chunkver = COALESCE((SELECT MIN(c.chunkver) FROM chunks c WHERE c.doc_id = documents.id), 0)`,
	})
}

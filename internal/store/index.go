package store

import (
	"database/sql"
	"errors"
	"time"
)

// Chunk is one derived, searchable passage of a document.
type Chunk struct {
	ID      int64
	DocID   int64
	Ordinal int
	Text    string
	Page    int
	HasPage bool
	Section string
	Hash    string
}

// IndexDocument upserts the derived document row and replaces its chunks.
// It returns the index document ID.
func (s *Store) IndexDocument(root, relpath, hash string, size int64, chunks []*Chunk) (int64, error) {
	tx, err := s.Index.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var id int64
	err = tx.QueryRow(`INSERT INTO documents (root, relpath, hash, size, indexed_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (root, relpath) DO UPDATE SET hash = excluded.hash, size = excluded.size, indexed_at = excluded.indexed_at
		RETURNING id`, root, relpath, hash, size, time.Now().Unix()).Scan(&id)
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`DELETE FROM chunks WHERE doc_id = ?`, id); err != nil {
		return 0, err
	}
	for _, c := range chunks {
		var page any
		if c.HasPage {
			page = c.Page
		}
		res, err := tx.Exec(`INSERT INTO chunks (doc_id, ordinal, text, page, section, hash, chunkver) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			id, c.Ordinal, c.Text, page, c.Section, c.Hash, ChunkVersion)
		if err != nil {
			return 0, err
		}
		c.ID, _ = res.LastInsertId()
		c.DocID = id
	}
	return id, tx.Commit()
}

// ChunkVersion is bumped whenever the chunking algorithm changes, marking
// derived chunks stale for regeneration.
const ChunkVersion = 1

// DeleteIndexDocument removes a document and cascades chunks/embeddings.
func (s *Store) DeleteIndexDocument(root, relpath string) error {
	_, err := s.Index.Exec(`DELETE FROM documents WHERE root = ? AND relpath = ?`, root, relpath)
	return err
}

// NeedsIndex reports whether the derived copy is missing or outdated.
func (s *Store) NeedsIndex(root, relpath, hash string) (bool, error) {
	var cur string
	err := s.Index.QueryRow(`SELECT hash FROM documents WHERE root = ? AND relpath = ?`, root, relpath).Scan(&cur)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return cur != hash, nil
}

// IndexStats summarizes derived index state.
type IndexStats struct {
	Documents  int    `json:"documents"`
	Chunks     int    `json:"chunks"`
	Embeddings int    `json:"embeddings"`
	Stale      int    `json:"stale"`
	Model      string `json:"model"`
	ChunkVer   int    `json:"chunk_ver"`
}

// GetIndexStats returns derived index counters.
func (s *Store) GetIndexStats(model string) (*IndexStats, error) {
	st := &IndexStats{Model: model, ChunkVer: ChunkVersion}
	if err := s.Index.QueryRow(`SELECT COUNT(*) FROM documents`).Scan(&st.Documents); err != nil {
		return nil, err
	}
	if err := s.Index.QueryRow(`SELECT COUNT(*) FROM chunks`).Scan(&st.Chunks); err != nil {
		return nil, err
	}
	if err := s.Index.QueryRow(`SELECT COUNT(*) FROM embeddings`).Scan(&st.Embeddings); err != nil {
		return nil, err
	}
	if err := s.Index.QueryRow(`SELECT COUNT(*) FROM embeddings WHERE stale = 1 OR model <> ?`, model).Scan(&st.Stale); err != nil {
		return nil, err
	}
	return st, nil
}

// DropDerived clears all derived search state. Synchronization state is
// untouched; rebuilding is always safe.
func (s *Store) DropDerived() error {
	// Deleting documents cascades to chunks and embeddings; the FTS triggers
	// keep the external-content index consistent.
	if _, err := s.Index.Exec(`DELETE FROM embeddings`); err != nil {
		return err
	}
	if _, err := s.Index.Exec(`DELETE FROM documents`); err != nil {
		return err
	}
	if _, err := s.Index.Exec(`INSERT INTO fts_chunks (fts_chunks) VALUES ('integrity-check')`); err != nil {
		return err
	}
	return nil
}

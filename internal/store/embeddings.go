package store

import (
	"database/sql"
	"encoding/binary"
	"math"
)

// PutEmbedding stores a chunk vector for a model.
func (s *Store) PutEmbedding(chunkID int64, model string, vector []float32) error {
	return PutEmbeddingOn(s.Index, chunkID, model, vector)
}

// PutEmbeddingOn stores a vector on an arbitrary connection/transaction.
func PutEmbeddingOn(e execer, chunkID int64, model string, vector []float32) error {
	buf := make([]byte, 4*len(vector))
	for i, f := range vector {
		binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(f))
	}
	_, err := e.Exec(`INSERT INTO embeddings (chunk_id, model, dims, vector, stale)
		VALUES (?, ?, ?, ?, 0)
		ON CONFLICT (chunk_id) DO UPDATE SET model = excluded.model, dims = excluded.dims, vector = excluded.vector, stale = 0`,
		chunkID, model, len(vector), buf)
	return err
}

// execer is satisfied by *sql.DB and *sql.Tx.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// DecodeVector converts a stored blob back to float32s.
func DecodeVector(buf []byte) []float32 {
	out := make([]float32, len(buf)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
	}
	return out
}

// MarkEmbeddingsStale marks every embedding not matching model as stale.
func (s *Store) MarkEmbeddingsStale(model string) (int, error) {
	res, err := s.Index.Exec(`UPDATE embeddings SET stale = 1 WHERE model <> ?`, model)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ChunkIDsNeedingEmbedding returns chunk IDs of the given document that lack
// a fresh embedding for model.
func (s *Store) ChunkIDsNeedingEmbedding(docID int64, model string) ([]int64, error) {
	rows, err := s.Index.Query(`SELECT c.id FROM chunks c LEFT JOIN embeddings e ON e.chunk_id = c.id
		WHERE c.doc_id = ? AND (e.chunk_id IS NULL OR e.stale = 1 OR e.model <> ?)`, docID, model)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ChunkText returns the text of one chunk.
func (s *Store) ChunkText(chunkID int64) (string, error) {
	var t string
	err := s.Index.QueryRow(`SELECT text FROM chunks WHERE id = ?`, chunkID).Scan(&t)
	return t, err
}

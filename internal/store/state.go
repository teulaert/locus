package store

import (
	"database/sql"
	"errors"
	"time"

	"github.com/teulaert/locus/internal/vvector"
)

// Doc is a synchronized file's correctness record.
type Doc struct {
	Root       string         `json:"root"`
	RelPath    string         `json:"path"`
	Size       int64          `json:"size"`
	Hash       string         `json:"hash"`
	Version    vvector.Vector `json:"version"`
	LastWriter string         `json:"last_writer"`
	Deleted    bool           `json:"deleted"`
	UpdatedAt  int64          `json:"updated_at"`

	// MtimeNS, CtimeNS and Inode are the stat fingerprint (with Size) the
	// file had when Hash was last computed from its bytes. Zero means "not
	// known": reconciliation must hash the file. They are local cache
	// state, never synchronized.
	MtimeNS int64 `json:"-"`
	CtimeNS int64 `json:"-"`
	Inode   int64 `json:"-"`
}

// UpsertDoc inserts or replaces a document record.
func (s *Store) UpsertDoc(d *Doc) error {
	if d.Version == nil {
		d.Version = vvector.New()
	}
	if d.UpdatedAt == 0 {
		d.UpdatedAt = time.Now().Unix()
	}
	_, err := s.State.Exec(`INSERT INTO docs (root, relpath, size, hash, version, last_writer, deleted, updated_at, mtime_ns, ctime_ns, inode)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (root, relpath) DO UPDATE SET
			size = excluded.size,
			hash = excluded.hash,
			version = excluded.version,
			last_writer = excluded.last_writer,
			deleted = excluded.deleted,
			updated_at = excluded.updated_at,
			mtime_ns = excluded.mtime_ns,
			ctime_ns = excluded.ctime_ns,
			inode = excluded.inode`,
		d.Root, d.RelPath, d.Size, d.Hash, d.Version.String(), d.LastWriter, boolInt(d.Deleted), d.UpdatedAt, d.MtimeNS, d.CtimeNS, d.Inode)
	return err
}

// SetDocStat records the stat fingerprint of a live document whose bytes
// were just hashed to hash, without touching its version. A record whose
// hash no longer matches (it changed in the meantime) is left alone. Zero
// mtime clears the fingerprint so the next pass hashes again.
func (s *Store) SetDocStat(root, relpath, hash string, size, mtimeNS, ctimeNS, inode int64) error {
	_, err := s.State.Exec(`UPDATE docs SET size = ?, mtime_ns = ?, ctime_ns = ?, inode = ?
		WHERE root = ? AND relpath = ? AND hash = ? AND deleted = 0`,
		size, mtimeNS, ctimeNS, inode, root, relpath, hash)
	return err
}

// GetDoc returns the record for root/relpath, or sql.ErrNoRows.
func (s *Store) GetDoc(root, relpath string) (*Doc, error) {
	return scanDoc(s.State.QueryRow(`SELECT root, relpath, size, hash, version, last_writer, deleted, updated_at, mtime_ns, ctime_ns, inode
		FROM docs WHERE root = ? AND relpath = ?`, root, relpath))
}

// ListDocs returns all records for a root; includeDeleted also returns tombstones.
func (s *Store) ListDocs(root string, includeDeleted bool) ([]*Doc, error) {
	q := `SELECT root, relpath, size, hash, version, last_writer, deleted, updated_at, mtime_ns, ctime_ns, inode FROM docs WHERE root = ?`
	if !includeDeleted {
		q += ` AND deleted = 0`
	}
	q += ` ORDER BY relpath`
	rows, err := s.State.Query(q, root)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Doc
	for rows.Next() {
		d, err := scanDoc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// CountDocs returns live and tombstone counts for a root.
func (s *Store) CountDocs(root string) (live, deleted int, err error) {
	err = s.State.QueryRow(`SELECT
		COALESCE(SUM(CASE WHEN deleted = 0 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN deleted = 1 THEN 1 ELSE 0 END), 0)
		FROM docs WHERE root = ?`, root).Scan(&live, &deleted)
	return
}

type rowScanner interface{ Scan(dest ...any) error }

func scanDoc(r rowScanner) (*Doc, error) {
	var d Doc
	var version string
	var deleted int
	if err := r.Scan(&d.Root, &d.RelPath, &d.Size, &d.Hash, &version, &d.LastWriter, &deleted, &d.UpdatedAt, &d.MtimeNS, &d.CtimeNS, &d.Inode); err != nil {
		return nil, err
	}
	d.Deleted = deleted != 0
	v, err := vvector.Parse(version)
	if err != nil {
		return nil, err
	}
	d.Version = v
	return &d, nil
}

// Conflict is a locally recorded divergence between two peers. Despite the
// names, the Local* fields describe the version that kept the normal path
// (the deterministic winner, possibly written by another peer) and the
// Conflict* fields the other version; ConflictPath is its sibling file, or
// empty when none was written (a delete concurrent with an edit).
type Conflict struct {
	ID             string `json:"id"`
	Root           string `json:"root"`
	RelPath        string `json:"relpath"`
	LocalPath      string `json:"local_path"`
	ConflictPath   string `json:"conflict_path"`
	LocalHash      string `json:"local_hash"`
	ConflictHash   string `json:"conflict_hash"`
	LocalWriter    string `json:"local_writer"`
	ConflictWriter string `json:"conflict_writer"`
	CreatedAt      int64  `json:"created_at"`
	ResolvedAt     *int64 `json:"resolved_at"`
}

// PutConflict records a conflict.
func (s *Store) PutConflict(c *Conflict) error {
	_, err := s.State.Exec(`INSERT INTO conflicts (id, root, relpath, local_path, conflict_path, local_hash, conflict_hash, local_writer, conflict_writer, created_at, resolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET resolved_at = excluded.resolved_at`,
		c.ID, c.Root, c.RelPath, c.LocalPath, c.ConflictPath, c.LocalHash, c.ConflictHash, c.LocalWriter, c.ConflictWriter, c.CreatedAt, c.ResolvedAt)
	return err
}

// ListConflicts returns unresolved conflicts (resolved_at IS NULL).
func (s *Store) ListConflicts() ([]*Conflict, error) {
	rows, err := s.State.Query(conflictSelect + ` WHERE resolved_at IS NULL ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	return scanConflicts(rows)
}

// GetConflict returns one conflict by ID.
func (s *Store) GetConflict(id string) (*Conflict, error) {
	row := s.State.QueryRow(conflictSelect+` WHERE id = ?`, id)
	c, err := scanConflict(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return c, err
}

// GetConflictFor returns the most recent unresolved conflict for one path.
func (s *Store) GetConflictFor(root, relpath string) (*Conflict, error) {
	row := s.State.QueryRow(conflictSelect+` WHERE root = ? AND relpath = ? AND resolved_at IS NULL ORDER BY created_at DESC, id DESC LIMIT 1`, root, relpath)
	c, err := scanConflict(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return c, err
}

// ResolveConflict marks a conflict resolved.
func (s *Store) ResolveConflict(id string) error {
	res, err := s.State.Exec(`UPDATE conflicts SET resolved_at = ? WHERE id = ? AND resolved_at IS NULL`,
		time.Now().Unix(), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// ListAllConflicts returns every conflict including resolved ones.
func (s *Store) ListAllConflicts() ([]*Conflict, error) {
	rows, err := s.State.Query(conflictSelect + ` ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	return scanConflicts(rows)
}

const conflictSelect = `SELECT id, root, relpath, local_path, conflict_path, local_hash, conflict_hash, local_writer, conflict_writer, created_at, resolved_at FROM conflicts`

// scanConflicts drains and always closes rows. Closing matters: state.db has a
// single connection, so a cursor left open after a scan error would block every
// later query on it.
func scanConflicts(rows *sql.Rows) ([]*Conflict, error) {
	defer rows.Close()
	var out []*Conflict
	for rows.Next() {
		c, err := scanConflict(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func scanConflict(r rowScanner) (*Conflict, error) {
	var c Conflict
	var resolved *int64
	if err := r.Scan(&c.ID, &c.Root, &c.RelPath, &c.LocalPath, &c.ConflictPath, &c.LocalHash, &c.ConflictHash,
		&c.LocalWriter, &c.ConflictWriter, &c.CreatedAt, &resolved); err != nil {
		return nil, err
	}
	c.ResolvedAt = resolved
	return &c, nil
}

// Audit appends a security/conflict event to the local audit log.
func (s *Store) Audit(actor, action, detail string) error {
	_, err := s.State.Exec(`INSERT INTO audit (ts, actor, action, detail) VALUES (?, ?, ?, ?)`,
		time.Now().Unix(), actor, action, detail)
	return err
}

// AuditEntry is one audit log row.
type AuditEntry struct {
	Seq    int64
	TS     int64
	Actor  string
	Action string
	Detail string
}

// ListAudit returns the most recent audit entries.
func (s *Store) ListAudit(limit int) ([]*AuditEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.State.Query(`SELECT seq, ts, actor, action, detail FROM audit ORDER BY seq DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.Seq, &e.TS, &e.Actor, &e.Action, &e.Detail); err != nil {
			return nil, err
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}

// MetaGet/MetaSet store small pieces of peer metadata (presence snapshots,
// model identifiers, tuning counters).
func (s *Store) MetaGet(db *sql.DB, key string) (string, error) {
	var v string
	err := db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) MetaSet(db *sql.DB, key, value string) error {
	_, err := db.Exec(`INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// ErrNotFound signals a missing record.
var ErrNotFound = errors.New("store: not found")

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// PendingArrival is the provenance of an incoming transfer that has been
// written to disk but not yet recorded as a document version. It exists so a
// crash between the atomic rename and the record cannot make a received file
// look as if this peer authored it.
type PendingArrival struct {
	Root      string `json:"root"`
	RelPath   string `json:"path"`
	Hash      string `json:"hash"`
	Version   string `json:"version"`
	Writer    string `json:"writer"`
	UpdatedAt int64  `json:"updated_at"`
}

// PutPendingArrival records provenance before the file is moved into place.
func (s *Store) PutPendingArrival(p *PendingArrival) error {
	if p.UpdatedAt == 0 {
		p.UpdatedAt = time.Now().Unix()
	}
	_, err := s.State.Exec(`INSERT INTO pending_arrivals (root, relpath, hash, version, writer, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (root, relpath) DO UPDATE SET
			hash = excluded.hash,
			version = excluded.version,
			writer = excluded.writer,
			updated_at = excluded.updated_at`,
		p.Root, p.RelPath, p.Hash, p.Version, p.Writer, p.UpdatedAt)
	return err
}

// GetPendingArrival returns the pending provenance for one path.
func (s *Store) GetPendingArrival(root, relpath string) (*PendingArrival, error) {
	row := s.State.QueryRow(`SELECT root, relpath, hash, version, writer, updated_at FROM pending_arrivals WHERE root = ? AND relpath = ?`, root, relpath)
	var p PendingArrival
	err := row.Scan(&p.Root, &p.RelPath, &p.Hash, &p.Version, &p.Writer, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// ClearPendingArrival removes provenance once the document record is durable.
func (s *Store) ClearPendingArrival(root, relpath string) error {
	_, err := s.State.Exec(`DELETE FROM pending_arrivals WHERE root = ? AND relpath = ?`, root, relpath)
	return err
}

// RecordPeerRound notes that a synchronization round with peerID over root,
// started at startedAt, completed without errors. Every tombstone recorded
// before startedAt was in the manifest that round reconciled against the
// peer, so the peer now holds the deletion (or a newer version this peer
// has adopted). The stored time only moves forward.
func (s *Store) RecordPeerRound(root, peerID string, startedAt time.Time) error {
	_, err := s.State.Exec(`INSERT INTO peer_rounds (root, peer_id, synced_at) VALUES (?, ?, ?)
		ON CONFLICT (root, peer_id) DO UPDATE SET synced_at = MAX(synced_at, excluded.synced_at)`,
		root, peerID, startedAt.Unix())
	return err
}

// PeerRounds returns, per peer ID, the start of the last clean round over
// root.
func (s *Store) PeerRounds(root string) (map[string]time.Time, error) {
	rows, err := s.State.Query(`SELECT peer_id, synced_at FROM peer_rounds WHERE root = ?`, root)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var id string
		var at int64
		if err := rows.Scan(&id, &at); err != nil {
			return nil, err
		}
		out[id] = time.Unix(at, 0)
	}
	return out, rows.Err()
}

// PruneTombstones forgets deletion records under root that are older than
// cutoff and that every peer ever synchronized over root has seen, i.e.
// that predate the start of each such peer's last clean round. A peer that
// has been away since before the deletion keeps the tombstone alive: if it
// were forgotten, the peer's stale copy would look like a new document and
// resurrect the file everywhere (LOC-34). It returns the number pruned.
func (s *Store) PruneTombstones(root string, cutoff time.Time) (int64, error) {
	horizon := cutoff.Unix()
	var oldest sql.NullInt64
	if err := s.State.QueryRow(`SELECT MIN(synced_at) FROM peer_rounds WHERE root = ?`, root).Scan(&oldest); err != nil {
		return 0, err
	}
	if oldest.Valid && oldest.Int64 < horizon {
		horizon = oldest.Int64
	}
	res, err := s.State.Exec(`DELETE FROM docs WHERE root = ? AND deleted = 1 AND updated_at < ?`, root, horizon)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

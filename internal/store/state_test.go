package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/vvector"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestDocCRUD(t *testing.T) {
	st := openStore(t)
	root := "/root"
	d := &Doc{Root: root, RelPath: "a/b.txt", Size: 10, Hash: "h1", Version: vvector.New().Increment("p1"), LastWriter: "p1"}
	if err := st.UpsertDoc(d); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetDoc(root, "a/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash != "h1" || got.Version.String() != "p1:1" || got.Deleted {
		t.Fatalf("round trip: %+v", got)
	}
	// Replace.
	got.Hash, got.Size = "h2", 20
	if err := st.UpsertDoc(got); err != nil {
		t.Fatal(err)
	}
	again, err := st.GetDoc(root, "a/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if again.Hash != "h2" || again.Size != 20 {
		t.Fatalf("upsert did not replace: %+v", again)
	}
	if _, err := st.GetDoc(root, "missing.txt"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected ErrNoRows, got %v", err)
	}
}

func TestDocStatFingerprint(t *testing.T) {
	st := openStore(t)
	root := "/root"
	ver := vvector.New().Increment("p1")
	if err := st.UpsertDoc(&Doc{Root: root, RelPath: "a", Size: 3, Hash: "h1", Version: ver, LastWriter: "p1", MtimeNS: 10, CtimeNS: 11, Inode: 12}); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetDoc(root, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.MtimeNS != 10 || got.CtimeNS != 11 || got.Inode != 12 {
		t.Fatalf("fingerprint round trip: %+v", got)
	}
	// Recording a fingerprint for bytes the record no longer describes is
	// a no-op: the hash guard keeps a stale scan from vouching for them.
	if err := st.SetDocStat(root, "a", "other", 3, 20, 21, 22); err != nil {
		t.Fatal(err)
	}
	if got, _ = st.GetDoc(root, "a"); got.MtimeNS != 10 {
		t.Fatalf("fingerprint set under a different hash: %+v", got)
	}
	if err := st.SetDocStat(root, "a", "h1", 3, 20, 21, 22); err != nil {
		t.Fatal(err)
	}
	if got, _ = st.GetDoc(root, "a"); got.MtimeNS != 20 || got.CtimeNS != 21 || got.Inode != 22 || got.Version.String() != ver.String() {
		t.Fatalf("SetDocStat: %+v", got)
	}
	// A writer that does not know the fingerprint (synchronization) clears
	// it, so the next reconciliation hashes the file.
	if err := st.UpsertDoc(&Doc{Root: root, RelPath: "a", Size: 4, Hash: "h2", Version: ver.Increment("p2"), LastWriter: "p2"}); err != nil {
		t.Fatal(err)
	}
	if got, _ = st.GetDoc(root, "a"); got.MtimeNS != 0 || got.CtimeNS != 0 || got.Inode != 0 {
		t.Fatalf("upsert without fingerprint kept a stale one: %+v", got)
	}
}

func TestTombstonesAndCounts(t *testing.T) {
	st := openStore(t)
	root := "/root"
	for _, name := range []string{"keep.txt", "gone.txt"} {
		if err := st.UpsertDoc(&Doc{Root: root, RelPath: name, Hash: "h", Version: vvector.New().Increment("p"), LastWriter: "p"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.UpsertDoc(&Doc{Root: root, RelPath: "gone.txt", Hash: "h", Version: vvector.New().Increment("p"), LastWriter: "p", Deleted: true}); err != nil {
		t.Fatal(err)
	}
	live, deleted, err := st.CountDocs(root)
	if err != nil {
		t.Fatal(err)
	}
	if live != 1 || deleted != 1 {
		t.Fatalf("counts = live %d deleted %d", live, deleted)
	}
	all, err := st.ListDocs(root, true)
	if err != nil || len(all) != 2 {
		t.Fatalf("ListDocs(includeDeleted) = %d (%v)", len(all), err)
	}
	onlyLive, err := st.ListDocs(root, false)
	if err != nil || len(onlyLive) != 1 {
		t.Fatalf("ListDocs(live) = %d (%v)", len(onlyLive), err)
	}
}

func TestConflictsLifecycle(t *testing.T) {
	st := openStore(t)
	c := &Conflict{ID: "c1", Root: "/r", RelPath: "x.txt", LocalPath: "x.txt", ConflictPath: "x.conflict-y.txt",
		LocalHash: "a", ConflictHash: "b", LocalWriter: "p1", ConflictWriter: "p2", CreatedAt: 100}
	if _, err := st.PutConflict(c); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListConflicts()
	if err != nil || len(list) != 1 {
		t.Fatalf("unresolved list = %v (%v)", list, err)
	}
	if _, err := st.GetConflict("c1"); err != nil {
		t.Fatal(err)
	}
	if err := st.ResolveConflict("c1"); err != nil {
		t.Fatal(err)
	}
	if list, err := st.ListConflicts(); err != nil || len(list) != 0 {
		t.Fatalf("after resolve = %v (%v)", list, err)
	}
	all, err := st.ListAllConflicts()
	if err != nil || len(all) != 1 || all[0].ResolvedAt == nil {
		t.Fatalf("resolved history = %+v (%v)", all, err)
	}
	if err := st.ResolveConflict("nope"); errors.Is(err, sql.ErrNoRows) || err == nil {
		t.Fatal("expected error resolving unknown conflict")
	}
}

// A row that fails to scan mid-iteration must not leak the *sql.Rows: state.db
// has a single connection (SetMaxOpenConns(1)), so a leaked cursor would block
// every later query on it forever (LOC-8).
func TestListHelpersReleaseConnectionOnScanError(t *testing.T) {
	cases := []struct {
		name string
		seed []string
		list func(*Store) error
	}{
		{
			name: "ListConflicts",
			seed: []string{
				`INSERT INTO conflicts VALUES ('c1','/r','a','a','a.c','h1','h2','p1','p2',100,NULL)`,
				`INSERT INTO conflicts VALUES ('c2','/r','b','b','b.c','h1','h2','p1','p2','not-a-number',NULL)`,
			},
			list: func(s *Store) error { _, err := s.ListConflicts(); return err },
		},
		{
			name: "ListAllConflicts",
			seed: []string{
				`INSERT INTO conflicts VALUES ('c1','/r','a','a','a.c','h1','h2','p1','p2',100,200)`,
				`INSERT INTO conflicts VALUES ('c2','/r','b','b','b.c','h1','h2','p1','p2','not-a-number',NULL)`,
			},
			list: func(s *Store) error { _, err := s.ListAllConflicts(); return err },
		},
		{
			name: "ListDocs",
			seed: []string{
				`INSERT INTO docs (root, relpath, size, hash, version, last_writer, deleted, updated_at) VALUES ('/r','a',1,'h','{}','p1',0,100)`,
				`INSERT INTO docs (root, relpath, size, hash, version, last_writer, deleted, updated_at) VALUES ('/r','b',1,'h','{}','p1',0,'not-a-number')`,
			},
			list: func(s *Store) error { _, err := s.ListDocs("/r", true); return err },
		},
		{
			name: "ListAudit",
			seed: []string{
				`INSERT INTO audit (ts, actor, action, detail) VALUES ('not-a-number','api','x','')`,
				`INSERT INTO audit (ts, actor, action, detail) VALUES (100,'api','x','')`,
			},
			list: func(s *Store) error { _, err := s.ListAudit(10); return err },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := openStore(t)
			for _, q := range tc.seed {
				if _, err := st.State.Exec(q); err != nil {
					t.Fatalf("seed %q: %v", q, err)
				}
			}
			if err := tc.list(st); err == nil {
				t.Fatal("expected a scan error from the corrupt row")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var n int
			if err := st.State.QueryRowContext(ctx, `SELECT 1`).Scan(&n); err != nil {
				t.Fatalf("state.db unusable after scan error (rows leaked?): %v", err)
			}
		})
	}
}

func TestAuditLog(t *testing.T) {
	st := openStore(t)
	for i := 0; i < 3; i++ {
		if err := st.Audit("api", "upload", "detail"); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := st.ListAudit(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("limit not honoured: %d", len(entries))
	}
	if entries[0].Seq < entries[1].Seq {
		t.Fatal("audit must be newest-first")
	}
}

func TestIndexDocumentAndChunks(t *testing.T) {
	st := openStore(t)
	chunks := []*Chunk{
		{Ordinal: 0, Text: "alpha content", Hash: "h0", Section: "S1"},
		{Ordinal: 1, Text: "beta content", Hash: "h1", Page: 3, HasPage: true},
	}
	id, err := st.IndexDocument("/r", "doc.txt", "hash1", 100, chunks)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected index document id")
	}
	var n int
	if err := st.Index.QueryRow(`SELECT COUNT(*) FROM chunks WHERE doc_id = ?`, id).Scan(&n); err != nil || n != 2 {
		t.Fatalf("chunks = %d (%v)", n, err)
	}
	if chunks[0].ID == 0 {
		t.Fatal("chunk ids must be assigned")
	}
	needs, err := st.NeedsIndex("/r", "doc.txt", "hash1")
	if err != nil || needs {
		t.Fatalf("fresh document reports needs index: %v (%v)", needs, err)
	}
	needs, err = st.NeedsIndex("/r", "doc.txt", "hash2")
	if err != nil || !needs {
		t.Fatalf("changed hash must need reindex: %v (%v)", needs, err)
	}
	// Replacing must not leave the old chunks behind.
	if _, err := st.IndexDocument("/r", "doc.txt", "hash2", 120, []*Chunk{{Ordinal: 0, Text: "only one", Hash: "h9"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Index.QueryRow(`SELECT COUNT(*) FROM chunks WHERE doc_id = ?`, id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("replace left %d chunks (%v)", n, err)
	}
	// The FTS index stays consistent with the content table.
	var hits int
	err = st.Index.QueryRow(`SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'only AND content'`).Scan(&hits)
	if err == nil && hits != 0 {
		t.Fatalf("stale fts rows survived replacement: %d", hits)
	}
	if err := st.Index.QueryRow(`SELECT COUNT(*) FROM fts_chunks WHERE fts_chunks MATCH 'only'`).Scan(&hits); err != nil || hits != 1 {
		t.Fatalf("fts out of sync: %d (%v)", hits, err)
	}
}

func TestDropDerivedIsSafeAndLeavesSyncState(t *testing.T) {
	st := openStore(t)
	root := "/r"
	if err := st.UpsertDoc(&Doc{Root: root, RelPath: "a.txt", Hash: "h", Version: vvector.New().Increment("p"), LastWriter: "p"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.IndexDocument(root, "a.txt", "h", 3, []*Chunk{{Ordinal: 0, Text: "hello", Hash: "x"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.DropDerived(); err != nil {
		t.Fatalf("dropping derived state failed: %v", err)
	}
	stats, err := st.GetIndexStats("model")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 0 || stats.Chunks != 0 {
		t.Fatalf("derived state not cleared: %+v", stats)
	}
	// Correctness state survives a derived-state wipe.
	if _, err := st.GetDoc(root, "a.txt"); err != nil {
		t.Fatalf("sync state lost after DropDerived: %v", err)
	}
	// Re-indexing after a drop works.
	if _, err := st.IndexDocument(root, "a.txt", "h", 3, []*Chunk{{Ordinal: 0, Text: "hello again", Hash: "y"}}); err != nil {
		t.Fatalf("re-index after drop failed: %v", err)
	}
}

func TestEmbeddingStorageAndStaleness(t *testing.T) {
	st := openStore(t)
	if _, err := st.IndexDocument("/r", "a.txt", "h", 1, []*Chunk{
		{Ordinal: 0, Text: "one", Hash: "c0"},
		{Ordinal: 1, Text: "two", Hash: "c1"},
	}); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	rows, err := st.Index.Query(`SELECT id FROM chunks ORDER BY ordinal`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	if err := st.PutEmbedding(ids[0], "model-a", []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	missing, err := st.ChunkIDsNeedingEmbedding(1, "model-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != ids[1] {
		t.Fatalf("missing embeddings = %v", missing)
	}
	n, err := st.MarkEmbeddingsStale("model-b")
	if err != nil || n != 1 {
		t.Fatalf("stale marking: %d (%v)", n, err)
	}
	after, err := st.ChunkIDsNeedingEmbedding(1, "model-b")
	if err != nil || len(after) != 2 {
		t.Fatalf("stale embeddings must be regenerated: %v (%v)", after, err)
	}
	stats, err := st.GetIndexStats("model-b")
	if err != nil || stats.Stale != 1 {
		t.Fatalf("stats stale = %+v (%v)", stats, err)
	}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	st1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st1.Close()
	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopening an existing store failed: %v", err)
	}
	defer st2.Close()
	var v int
	if err := st2.State.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&v); err != nil || v == 0 {
		t.Fatalf("schema version = %d (%v)", v, err)
	}
	if _, err := st2.GetDoc("/r", "x"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("tables missing after reopen")
	}
}

func TestMeta(t *testing.T) {
	st := openStore(t)
	if err := st.MetaSet(st.State, "k", "v1"); err != nil {
		t.Fatal(err)
	}
	v, err := st.MetaGet(st.State, "k")
	if err != nil || v != "v1" {
		t.Fatalf("meta = %q (%v)", v, err)
	}
	if err := st.MetaSet(st.State, "k", "v2"); err != nil {
		t.Fatal(err)
	}
	if v, _ := st.MetaGet(st.State, "k"); v != "v2" {
		t.Fatalf("meta not replaced: %q", v)
	}
	if v, err := st.MetaGet(st.State, "absent"); err != nil || v != "" {
		t.Fatalf("absent key = %q (%v)", v, err)
	}
}

func TestPendingArrivalRoundTrip(t *testing.T) {
	st := openStore(t)
	defer st.Close()
	p := &PendingArrival{Root: "/r", RelPath: "a.txt", Hash: "sha256:aa", Version: "1:peer-a", Writer: "peer-a"}
	if err := st.PutPendingArrival(p); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetPendingArrival("/r", "a.txt"); err != nil || got.Hash != "sha256:aa" || got.Writer != "peer-a" || got.UpdatedAt == 0 {
		t.Fatalf("get = %+v, %v", got, err)
	}
	// A later arrival for the same path replaces the earlier provenance.
	if err := st.PutPendingArrival(&PendingArrival{Root: "/r", RelPath: "a.txt", Hash: "sha256:bb", Version: "2:peer-a", Writer: "peer-a"}); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetPendingArrival("/r", "a.txt"); err != nil || got.Hash != "sha256:bb" {
		t.Fatalf("after replace = %+v, %v", got, err)
	}
	if err := st.ClearPendingArrival("/r", "a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetPendingArrival("/r", "a.txt"); err != ErrNotFound {
		t.Fatalf("after clear err = %v", err)
	}
	if _, err := st.GetPendingArrival("/r", "never.txt"); err != ErrNotFound {
		t.Fatalf("missing err = %v", err)
	}
}

// A tombstone may only be forgotten once it is old enough and every peer
// ever synchronized over the root has completed a clean round since the
// deletion. Otherwise a peer that has been away longer than the tombstone
// age presents its stale copy as a new document and resurrects it (LOC-34).
func TestPruneTombstonesWaitsForEveryKnownPeer(t *testing.T) {
	st := openStore(t)
	root, other := "/root", "/other"
	now := time.Now()
	day := 24 * time.Hour
	deletedAt := now.Add(-40 * day)
	tomb := func(root, rel string, at time.Time) {
		t.Helper()
		if err := st.UpsertDoc(&Doc{Root: root, RelPath: rel, Hash: "h", Version: vvector.New().Increment("p1"), LastWriter: "p1", Deleted: true, UpdatedAt: at.Unix()}); err != nil {
			t.Fatal(err)
		}
	}
	exists := func(root, rel string) bool {
		t.Helper()
		_, err := st.GetDoc(root, rel)
		if errors.Is(err, sql.ErrNoRows) {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		return true
	}
	prune := func(root string) int64 {
		t.Helper()
		n, err := st.PruneTombstones(root, now.Add(-30*day))
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	tomb(root, "old.txt", deletedAt)
	tomb(root, "young.txt", now.Add(-time.Hour))
	if err := st.UpsertDoc(&Doc{Root: root, RelPath: "live.txt", Hash: "h", Version: vvector.New().Increment("p1"), LastWriter: "p1", UpdatedAt: deletedAt.Unix()}); err != nil {
		t.Fatal(err)
	}

	// Peer A synchronized after the deletion; peer B was last seen before
	// it and has been offline ever since.
	if err := st.RecordPeerRound(root, "A", now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordPeerRound(root, "B", deletedAt.Add(-day)); err != nil {
		t.Fatal(err)
	}
	if n := prune(root); n != 0 || !exists(root, "old.txt") {
		t.Fatalf("pruned %d; a tombstone peer B never saw must survive past the age", n)
	}

	// Recording an older round never moves the acknowledgement backwards.
	if err := st.RecordPeerRound(root, "A", deletedAt.Add(-2*day)); err != nil {
		t.Fatal(err)
	}
	if rounds, err := st.PeerRounds(root); err != nil || rounds["A"].Unix() != now.Add(-time.Minute).Unix() {
		t.Fatalf("peer rounds = %v, %v", rounds, err)
	}

	// B returns and completes a clean round: now the old tombstone goes,
	// the young one stays for the age, and live records are never touched.
	if err := st.RecordPeerRound(root, "B", now); err != nil {
		t.Fatal(err)
	}
	if n := prune(root); n != 1 {
		t.Fatalf("pruned %d, want 1", n)
	}
	if exists(root, "old.txt") || !exists(root, "young.txt") || !exists(root, "live.txt") {
		t.Fatal("wrong records pruned")
	}

	// Acknowledgements are per root: a peer that never shared a root does
	// not block it, and with no known peers age alone decides.
	tomb(other, "gone.txt", deletedAt)
	if n := prune(other); n != 1 || exists(other, "gone.txt") {
		t.Fatalf("pruned %d in a root without known peers, want 1", n)
	}

	// A tombstone recorded in the same second a round started may not have
	// been in that round's manifest, so it is kept.
	if err := st.RecordPeerRound(other, "C", deletedAt); err != nil {
		t.Fatal(err)
	}
	tomb(other, "edge.txt", deletedAt)
	if n := prune(other); n != 0 || !exists(other, "edge.txt") {
		t.Fatalf("pruned %d tombstones not strictly older than the last round", n)
	}
}

func TestLearnedPeersRoundTrip(t *testing.T) {
	st := openStore(t)
	if got, err := st.ListLearnedPeers(); err != nil || len(got) != 0 {
		t.Fatalf("fresh store learned peers = %v, %v", got, err)
	}
	want := []LearnedPeer{
		{PeerID: "aa", DiscoveryAddr: "10.42.0.2:8911", SyncAddr: "10.42.0.2:8909", Hostname: "galaxy", LastSeen: 100, LearnedAt: 100},
		{PeerID: "bb", DiscoveryAddr: "10.42.0.3:8911", Via: "aa", LearnedAt: 90},
	}
	if err := st.ReplaceLearnedPeers(want); err != nil {
		t.Fatal(err)
	}
	got, err := st.ListLearnedPeers()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
	// Replacing drops entries no longer present.
	if err := st.ReplaceLearnedPeers(want[1:]); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.ListLearnedPeers(); len(got) != 1 || got[0].PeerID != "bb" {
		t.Fatalf("after replace = %+v", got)
	}
}

// A conflict is its root, path and two hashes: recording it again under
// another ID (an earlier build derived IDs from the local root path) keeps
// one record, under its original ID, and only reopens it (LOC-72).
func TestPutConflictFoldsSameConflictUnderAnotherID(t *testing.T) {
	st := openStore(t)
	first := &Conflict{ID: "/home/a/docs:x.txt:aaaa:bbbb", Root: "/r", RelPath: "x.txt", LocalPath: "x.txt",
		LocalHash: "aaaa", ConflictHash: "bbbb", LocalWriter: "p1", ConflictWriter: "p2", CreatedAt: 100}
	if _, err := st.PutConflict(first); err != nil {
		t.Fatal(err)
	}
	if err := st.ResolveConflict(first.ID); err != nil {
		t.Fatal(err)
	}
	again := *first
	again.ID, again.CreatedAt = "docs:x.txt:aaaa:bbbb", 200
	stored, err := st.PutConflict(&again)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ID != first.ID || stored.CreatedAt != 100 || stored.ResolvedAt != nil {
		t.Fatalf("stored = %+v, want the first record reopened", stored)
	}
	all, err := st.ListAllConflicts()
	if err != nil || len(all) != 1 {
		t.Fatalf("records = %+v (%v), want one", all, err)
	}
	other := again
	other.ID, other.ConflictHash = "docs:x.txt:aaaa:cccc", "cccc"
	if _, err := st.PutConflict(&other); err != nil {
		t.Fatal(err)
	}
	if found, err := st.FindConflict("/r", "x.txt", "aaaa", "cccc"); err != nil || found.ID != other.ID {
		t.Fatalf("FindConflict = %+v (%v)", found, err)
	}
	if _, err := st.FindConflict("/r", "x.txt", "bbbb", "aaaa"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FindConflict with swapped hashes = %v, want ErrNotFound", err)
	}
}

// Opening a state.db that holds the same conflict under several IDs folds
// them into the oldest record, open while any copy was (LOC-72).
func TestMigrationFoldsDuplicateConflicts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Roll state.db back to before the fold: no unique index, and the
	// three migrations that make it not yet applied.
	if _, err := s.State.Exec(`DROP INDEX idx_conflicts_key`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.State.Exec(`DELETE FROM schema_version WHERE version > (SELECT MAX(version) - 3 FROM schema_version)`); err != nil {
		t.Fatal(err)
	}
	ins := func(id, root, rel, keep, other string, created int64, resolved any) {
		t.Helper()
		if _, err := s.State.Exec(`INSERT INTO conflicts (id, root, relpath, local_path, conflict_path, local_hash, conflict_hash, local_writer, conflict_writer, created_at, resolved_at)
			VALUES (?, ?, ?, ?, '', ?, ?, 'p1', 'p2', ?, ?)`, id, root, rel, rel, keep, other, created, resolved); err != nil {
			t.Fatal(err)
		}
	}
	// One conflict listed twice: resolved here, still open as adopted.
	ins("/home/me/docs:a.txt:aaaa:bbbb", "/home/me/docs", "a.txt", "aaaa", "bbbb", 100, 150)
	ins("/srv/docs:a.txt:aaaa:bbbb", "/home/me/docs", "a.txt", "aaaa", "bbbb", 110, nil)
	// One conflict resolved under both IDs.
	ins("/home/me/docs:b.txt:aaaa:bbbb", "/home/me/docs", "b.txt", "aaaa", "bbbb", 100, 150)
	ins("/srv/docs:b.txt:aaaa:bbbb", "/home/me/docs", "b.txt", "aaaa", "bbbb", 110, 160)
	// A different conflict on the same path stays.
	ins("/home/me/docs:a.txt:aaaa:cccc", "/home/me/docs", "a.txt", "aaaa", "cccc", 120, nil)
	s.Close()

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	all, err := s.ListAllConflicts()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, c := range all {
		got[c.ID] = c.ResolvedAt == nil
	}
	want := map[string]bool{
		"/home/me/docs:a.txt:aaaa:bbbb": true,
		"/home/me/docs:b.txt:aaaa:bbbb": false,
		"/home/me/docs:a.txt:aaaa:cccc": true,
	}
	if len(got) != len(want) {
		t.Fatalf("records after migration = %v, want %v", got, want)
	}
	for id, open := range want {
		if o, ok := got[id]; !ok || o != open {
			t.Fatalf("records after migration = %v, want %v", got, want)
		}
	}
}

// LOC-83: state.db reads go through a pool of query-only connections, so
// they never queue behind a write transaction on the writer connection,
// and a read made after a write returned sees it.
func TestStateReadsDoNotWaitForTheWriter(t *testing.T) {
	st := openStore(t)
	if err := st.UpsertDoc(&Doc{Root: "/r", RelPath: "a.txt", Hash: "h1", LastWriter: "p"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StateRead.Exec(`DELETE FROM docs`); err == nil {
		t.Fatal("the state read pool accepted a write")
	}

	// A write transaction holds the writer connection.
	tx, err := st.State.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE docs SET hash = 'uncommitted' WHERE relpath = 'a.txt'`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		d, err := st.GetDoc("/r", "a.txt")
		if err == nil && d.Hash != "h1" {
			err = errors.New("read an uncommitted write: " + d.Hash)
		}
		if err == nil {
			_, err = st.ListDocs("/r", true)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a record read waited for the writer's transaction")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Read after write, through different connections, every time.
	for i := 0; i < 200; i++ {
		h := fmt.Sprintf("hash-%d", i)
		if err := st.UpsertDoc(&Doc{Root: "/r", RelPath: "a.txt", Hash: h, LastWriter: "p"}); err != nil {
			t.Fatal(err)
		}
		if d, err := st.GetDoc("/r", "a.txt"); err != nil || d.Hash != h {
			t.Fatalf("read after write %d: %+v, %v; want hash %s", i, d, err, h)
		}
	}
}

// LOC-83: a received file's record and the removal of its pending
// provenance are one commit.
func TestRecordArrivalClearsPending(t *testing.T) {
	st := openStore(t)
	if err := st.PutPendingArrival(&PendingArrival{Root: "/r", RelPath: "a.txt", Hash: "h", Version: "p:1", Writer: "p"}); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordArrival(&Doc{Root: "/r", RelPath: "a.txt", Size: 3, Hash: "h", Version: vvector.New().Increment("p"), LastWriter: "p"}); err != nil {
		t.Fatal(err)
	}
	d, err := st.GetDoc("/r", "a.txt")
	if err != nil || d.Hash != "h" || d.LastWriter != "p" || d.Size != 3 || d.UpdatedAt == 0 {
		t.Fatalf("record %+v, %v", d, err)
	}
	if _, err := st.GetPendingArrival("/r", "a.txt"); err != ErrNotFound {
		t.Fatalf("pending arrival left behind: %v", err)
	}
	// Without a pending arrival it is a plain record write.
	if err := st.RecordArrival(&Doc{Root: "/r", RelPath: "b.txt", Hash: "h2", LastWriter: "p"}); err != nil {
		t.Fatal(err)
	}
	if d, err := st.GetDoc("/r", "b.txt"); err != nil || d.Hash != "h2" {
		t.Fatalf("record %+v, %v", d, err)
	}
}

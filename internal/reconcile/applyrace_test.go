package reconcile

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/teulaert/locus/internal/fswatch"
	"github.com/teulaert/locus/internal/identity"
	syncpkg "github.com/teulaert/locus/internal/sync"
	"github.com/teulaert/locus/internal/vvector"
)

// raceEnv is a reconciler and a sync engine sharing one store, as in the
// daemon, with a second identity standing in for the pushing peer.
type raceEnv struct {
	*env
	eng          *syncpkg.Engine
	self, remote string
}

func newRaceEnv(t *testing.T) *raceEnv {
	t.Helper()
	e := newEnv(t)
	remote, err := identity.Load(filepath.Join(t.TempDir(), "remote"))
	if err != nil {
		t.Fatal(err)
	}
	return &raceEnv{
		env:    e,
		eng:    &syncpkg.Engine{Store: e.st, Identity: e.rec.Identity, Roots: []string{e.root}},
		self:   e.rec.Identity.PeerID,
		remote: remote.PeerID,
	}
}

func (r *raceEnv) pass(t *testing.T) *Report {
	t.Helper()
	rep, err := r.rec.PassRoot(r.ctx, r.root)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// apply delivers a put (content non-nil) or a delete from the remote peer.
func (r *raceEnv) apply(t *testing.T, rel string, content *string, oldContent string) {
	t.Helper()
	ver := vvector.New()
	if d, err := r.st.GetDoc(r.root, rel); err == nil {
		ver = d.Version
	}
	meta := syncpkg.Meta{Version: ver.Increment(r.remote), LastWriter: r.remote}
	var res syncpkg.Result
	var err error
	if content == nil {
		meta.Deleted, meta.Hash = true, fswatch.HashBytes([]byte(oldContent))
		res, err = r.eng.Apply(r.root, rel, meta, nil, r.remote)
	} else {
		meta.Hash, meta.Size = fswatch.HashBytes([]byte(*content)), int64(len(*content))
		res, err = r.eng.Apply(r.root, rel, meta, strings.NewReader(*content), r.remote)
	}
	if err != nil || res != syncpkg.ResultApplied {
		t.Fatalf("apply %s: %v %v", rel, res, err)
	}
}

// record describes rel's record for comparison.
func (r *raceEnv) record(t *testing.T, rel string) string {
	t.Helper()
	d, err := r.st.GetDoc(r.root, rel)
	if err != nil {
		t.Fatal(err)
	}
	return d.Version.String() + " writer=" + identity.Short(d.LastWriter) + " hash=" + d.Hash[:8] + " deleted=" + map[bool]string{true: "yes", false: "no"}[d.Deleted]
}

// A file a peer pushes is installed and recorded by the sync engine's Apply.
// If that happens after a pass has read the records but before it scans the
// files, the scan finds bytes its stale records do not describe. The pass
// must not record them again as this peer's own version: same content, a
// vector concurrent with the sender's, so the sender's next change (a
// delete, in TestSyncServerEndToEndOverPeerPort) comes back as a conflict
// (LOC-58). Each case runs its step from the afterList hook, so the
// interleaving is deterministic. A genuine local edit in the same window is
// still recorded as this peer's new version.
func TestPassDoesNotReversionFileAppliedDuringPass(t *testing.T) {
	str := func(s string) *string { return &s }
	cases := []struct {
		name     string
		existing string // content recorded before the pass, "" for none
		race     func(t *testing.T, r *raceEnv)
		// the record expected after the pass, and the report counter
		// that must stay zero (or be one, for the local edit)
		wantVer    func(r *raceEnv) vvector.Vector
		wantWriter func(r *raceEnv) string
		wantDel    bool
		counter    func(rep *Report) int
		wantCount  int
	}{
		{
			name:       "pushed new file",
			race:       func(t *testing.T, r *raceEnv) { r.apply(t, "doc.txt", str("pushed by b\n"), "") },
			wantVer:    func(r *raceEnv) vvector.Vector { return vvector.New().Increment(r.remote) },
			wantWriter: func(r *raceEnv) string { return r.remote },
			counter:    func(rep *Report) int { return rep.Created },
		},
		{
			name:       "pushed update",
			existing:   "one\n",
			race:       func(t *testing.T, r *raceEnv) { r.apply(t, "doc.txt", str("two\n"), "") },
			wantVer:    func(r *raceEnv) vvector.Vector { return vvector.New().Increment(r.self).Increment(r.remote) },
			wantWriter: func(r *raceEnv) string { return r.remote },
			counter:    func(rep *Report) int { return rep.Modified },
		},
		{
			name:       "pushed delete",
			existing:   "one\n",
			race:       func(t *testing.T, r *raceEnv) { r.apply(t, "doc.txt", nil, "one\n") },
			wantVer:    func(r *raceEnv) vvector.Vector { return vvector.New().Increment(r.self).Increment(r.remote) },
			wantWriter: func(r *raceEnv) string { return r.remote },
			wantDel:    true,
			counter:    func(rep *Report) int { return rep.Deleted },
		},
		{
			name:       "local edit",
			existing:   "one\n",
			race:       func(t *testing.T, r *raceEnv) { r.write(t, "doc.txt", "edited here\n") },
			wantVer:    func(r *raceEnv) vvector.Vector { return vvector.New().Increment(r.self).Increment(r.self) },
			wantWriter: func(r *raceEnv) string { return r.self },
			counter:    func(rep *Report) int { return rep.Modified },
			wantCount:  1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRaceEnv(t)
			if tc.existing != "" {
				r.write(t, "doc.txt", tc.existing)
				r.pass(t)
			}
			r.rec.afterList = func(string) {
				r.rec.afterList = nil
				tc.race(t, r)
			}
			rep := r.pass(t)
			d, err := r.st.GetDoc(r.root, "doc.txt")
			if err != nil {
				t.Fatal(err)
			}
			if vvector.Compare(d.Version, tc.wantVer(r)) != vvector.Equal || d.LastWriter != tc.wantWriter(r) || d.Deleted != tc.wantDel {
				t.Errorf("record after the pass: %s; want version %s writer=%s deleted=%v",
					r.record(t, "doc.txt"), tc.wantVer(r), identity.Short(tc.wantWriter(r)), tc.wantDel)
			}
			if n := tc.counter(rep); n != tc.wantCount {
				t.Errorf("pass counted %d, want %d: %+v", n, tc.wantCount, rep)
			}
			// The next pass agrees with the record and leaves it alone.
			before := r.record(t, "doc.txt")
			if rep := r.pass(t); rep.Created+rep.Modified+rep.Deleted+rep.Concurrent != 0 {
				t.Errorf("follow-up pass changed records: %+v", rep)
			}
			if after := r.record(t, "doc.txt"); after != before {
				t.Errorf("follow-up pass rewrote the record: %s -> %s", before, after)
			}
		})
	}
}

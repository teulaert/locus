package reconcile

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/fswatch"
)

// age gives a file a settled mtime (well outside fswatch.RacyWindow), which
// is what a file that has not been touched recently looks like.
func (e *env) age(t *testing.T, rel string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(filepath.Join(e.root, filepath.FromSlash(rel)), when, when); err != nil {
		t.Fatal(err)
	}
}

func (e *env) pass(t *testing.T) *Report {
	t.Helper()
	reports, err := e.rec.Pass(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return reports[0]
}

func TestUnchangedFilesAreNotRehashed(t *testing.T) {
	e := newEnv(t)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	e.write(t, "a.txt", "alpha")
	e.write(t, "sub/b.txt", "bravo")
	e.age(t, "a.txt", old)
	e.age(t, "sub/b.txt", old)

	if rep := e.pass(t); rep.Created != 2 || rep.Hashed != 2 {
		t.Fatalf("first pass must hash every new file: %+v", rep)
	}
	e.drain(t)

	// Nothing changed on disk: the stat fingerprints vouch for both files,
	// so no byte is read and the root is in sync.
	for i := 0; i < 2; i++ {
		rep := e.pass(t)
		if rep.Hashed != 0 || rep.Unchanged != 2 || !rep.InSync {
			t.Fatalf("pass %d over an unchanged tree re-hashed: %+v", i+2, rep)
		}
	}

	// A touched file (same bytes, new mtime) is hashed again, found
	// unchanged, and not treated as a new version.
	before, err := e.st.GetDoc(e.root, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	e.age(t, "a.txt", old.Add(time.Minute))
	if rep := e.pass(t); rep.Hashed != 1 || rep.Modified != 0 || !rep.InSync {
		t.Fatalf("touched file: %+v", rep)
	}
	after, err := e.st.GetDoc(e.root, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if after.Version.String() != before.Version.String() {
		t.Fatalf("touch bumped the version: %s -> %s", before.Version, after.Version)
	}
	// ...and its new fingerprint is remembered.
	if rep := e.pass(t); rep.Hashed != 0 {
		t.Fatalf("touched file hashed again on the following pass: %+v", rep)
	}

	// A real edit is hashed and becomes a new version.
	e.write(t, "sub/b.txt", "bravo, edited")
	e.age(t, "sub/b.txt", old.Add(2*time.Minute))
	if rep := e.pass(t); rep.Hashed != 1 || rep.Modified != 1 {
		t.Fatalf("modified file: %+v", rep)
	}
}

func TestEditPreservingSizeAndMtimeIsDetected(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("ctime is only part of the fingerprint on linux and darwin")
	}
	e := newEnv(t)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	e.write(t, "a.txt", "alpha")
	e.age(t, "a.txt", old)
	e.pass(t)
	if rep := e.pass(t); rep.Hashed != 0 {
		t.Fatalf("unchanged file re-hashed: %+v", rep)
	}
	// Same size, mtime restored (as cp -p, rsync -t or some editors do):
	// only the ctime betrays the write, and it must be enough.
	e.write(t, "a.txt", "ALPHA")
	e.age(t, "a.txt", old)
	if rep := e.pass(t); rep.Hashed != 1 || rep.Modified != 1 {
		t.Fatalf("same-size edit with restored mtime missed: %+v", rep)
	}
}

func TestRacilyCleanFilesAreHashedUntilSettled(t *testing.T) {
	e := newEnv(t)
	// Written just now: a further write within the same timestamp tick
	// would keep mtime and size, so the fingerprint must not be trusted.
	e.write(t, "fresh.txt", "new")
	if rep := e.pass(t); rep.Hashed != 1 {
		t.Fatalf("first pass: %+v", rep)
	}
	if rep := e.pass(t); rep.Hashed != 1 {
		t.Fatalf("racily clean file was skipped on the next pass: %+v", rep)
	}
	d, err := e.st.GetDoc(e.root, "fresh.txt")
	if err != nil {
		t.Fatal(err)
	}
	if d.MtimeNS != 0 {
		t.Fatalf("fingerprint recorded for a file inside the racy window: %+v", d)
	}
	// Once the mtime is settled the fingerprint is recorded and used.
	e.age(t, "fresh.txt", time.Now().Add(-2*fswatch.RacyWindow))
	e.pass(t)
	if rep := e.pass(t); rep.Hashed != 0 {
		t.Fatalf("settled file still re-hashed: %+v", rep)
	}
}

func TestPeriodicFullVerifyIgnoresFingerprints(t *testing.T) {
	e := newEnv(t)
	e.rec.VerifyEvery = time.Hour
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	e.write(t, "a.txt", "alpha")
	e.write(t, "b.txt", "bravo")
	e.age(t, "a.txt", old)
	e.age(t, "b.txt", old)

	if rep := e.pass(t); !rep.FullVerify || rep.Hashed != 2 {
		t.Fatalf("a never-verified root is verified on its first pass: %+v", rep)
	}
	if rep := e.pass(t); rep.FullVerify || rep.Hashed != 0 {
		t.Fatalf("verification repeated before it was due: %+v", rep)
	}
	// Pretend the last verification was long ago.
	stale := strconv.FormatInt(time.Now().Add(-2*time.Hour).Unix(), 10)
	if err := e.st.MetaSet(e.st.State, verifyKey(e.root), stale); err != nil {
		t.Fatal(err)
	}
	if rep := e.pass(t); !rep.FullVerify || rep.Hashed != 2 {
		t.Fatalf("due verification did not hash every file: %+v", rep)
	}
	if rep := e.pass(t); rep.FullVerify || rep.Hashed != 0 {
		t.Fatalf("verification time not recorded: %+v", rep)
	}
}

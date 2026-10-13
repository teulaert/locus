package reconcile

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/store"
)

// LOC-83: a reconciliation pass over thousands of new files writes each
// record in a short transaction of its own, so it never holds state.db's
// writer for long: a record write from elsewhere (a received file) gets
// in within milliseconds while the pass runs, and a record read never
// waits for the writer at all.
func TestPassKeepsWriterTransactionsShort(t *testing.T) {
	e := newEnv(t)
	files := 3000
	if testing.Short() {
		files = 1000
	}
	for i := 0; i < files; i++ {
		e.write(t, fmt.Sprintf("d%02d/f%04d.txt", i%30, i), fmt.Sprintf("file %d\n", i))
	}
	type probe struct{ read, write time.Duration }
	var probes []probe
	stop := make(chan struct{})
	probed := make(chan struct{})
	go func() {
		defer close(probed)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			start := time.Now()
			_, _ = e.st.GetDoc(e.root, fmt.Sprintf("d%02d/f%04d.txt", i%30, i%files))
			read := time.Since(start)
			start = time.Now()
			if err := e.st.UpsertDoc(&store.Doc{Root: "/elsewhere", RelPath: fmt.Sprintf("p%d", i%100), Hash: "h", LastWriter: "p"}); err != nil {
				t.Error(err)
				return
			}
			probes = append(probes, probe{read, time.Since(start)})
		}
	}()
	waits, waited := e.st.WriterWait()
	started := time.Now()
	reports, err := e.rec.Pass(e.ctx)
	took := time.Since(started)
	close(stop)
	<-probed
	if err != nil {
		t.Fatal(err)
	}
	if reports[0].Created != files {
		t.Fatalf("created %d; want %d", reports[0].Created, files)
	}
	n, d := e.st.WriterWait()
	reads := make([]time.Duration, len(probes))
	writes := make([]time.Duration, len(probes))
	for i, p := range probes {
		reads[i], writes[i] = p.read, p.write
	}
	slices.Sort(reads)
	slices.Sort(writes)
	if len(probes) == 0 {
		t.Fatal("no probes ran during the pass")
	}
	q := func(s []time.Duration, f float64) time.Duration { return s[int(f*float64(len(s)-1))] }
	t.Logf("pass over %d new files took %v; %d probes: record read p50 %v max %v, record write p50 %v max %v; writer waits %d totalling %v",
		files, took.Round(time.Millisecond), len(probes), q(reads, .5), q(reads, 1), q(writes, .5), q(writes, 1), n-waits, (d - waited).Round(time.Millisecond))
	// The typical probe is what the read pool and short write transactions
	// buy; the worst case is bounded loosely, because a shared CI runner
	// stalls for hundreds of milliseconds on its own (a GitHub-hosted runner
	// showed a 854 ms write wait with a 448 µs median).
	if p50 := q(writes, .5); p50 > 20*time.Millisecond {
		t.Errorf("record writes typically waited %v for the writer during the pass", p50)
	}
	if p50 := q(reads, .5); p50 > 10*time.Millisecond {
		t.Errorf("record reads typically took %v during the pass", p50)
	}
	if max := q(writes, 1); max > 1500*time.Millisecond {
		t.Errorf("a record write waited %v for the writer during the pass", max)
	}
	if max := q(reads, 1); max > 1500*time.Millisecond {
		t.Errorf("a record read took %v during the pass", max)
	}
}

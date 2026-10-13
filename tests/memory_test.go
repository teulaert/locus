package tests

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/extract"
	"github.com/teulaert/locus/internal/testutil/synthpdf"
)

// heapSampler records the peak of runtime.MemStats.HeapInuse while it runs.
// With LOCUS_HEAP_PROFILE_DIR set, it also writes a heap profile each time
// the peak grows by a tenth, so the last one shows what was live at the peak.
type heapSampler struct {
	mu      sync.Mutex
	base    uint64
	peak    uint64
	rss     uint64 // peak resident set, where /proc/self/statm exists
	stop    chan struct{}
	done    chan struct{}
	profDir string
	dumped  uint64
	dumps   int
}

func startHeapSampler() *heapSampler {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s := &heapSampler{base: ms.HeapInuse, peak: ms.HeapInuse, stop: make(chan struct{}), done: make(chan struct{}), profDir: os.Getenv("LOCUS_HEAP_PROFILE_DIR")}
	go s.run()
	return s
}

func (s *heapSampler) run() {
	defer close(s.done)
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	var ms runtime.MemStats
	for {
		runtime.ReadMemStats(&ms)
		rss := residentBytes()
		s.mu.Lock()
		if ms.HeapInuse > s.peak {
			s.peak = ms.HeapInuse
		}
		s.rss = max(s.rss, rss)
		dump := s.profDir != "" && s.peak > s.dumped+s.dumped/10 && s.peak > s.base+(16<<20)
		if dump {
			s.dumped = s.peak
			s.dumps++
		}
		s.mu.Unlock()
		if dump {
			if f, err := os.Create(filepath.Join(s.profDir, "heap-peak.pprof")); err == nil {
				_ = pprof.Lookup("heap").WriteTo(f, 0)
				f.Close()
			}
		}
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
	}
}

// Stop ends sampling and returns the baseline and peak of the heap in use,
// and the peak resident set (0 where unknown).
func (s *heapSampler) Stop() (base, peak, rss uint64) {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base, s.peak, s.rss
}

// residentBytes is the process's resident set from /proc/self/statm (Linux),
// 0 where that does not exist.
func residentBytes() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	var size, resident uint64
	if _, err := fmt.Sscan(string(b), &size, &resident); err != nil {
		return 0
	}
	return resident * uint64(os.Getpagesize())
}

// bulkRoot fills dir with a root shaped like the one that showed the
// LOC-84 peaks: many small markdown notes, photos, a large CSV export and
// several large PDFs, text-heavy (a report of thousands of pages) and
// scanned (pages that are one image each, with no text). It returns the
// number of files and the size of the largest.
func bulkRoot(t *testing.T, dir string) (files int, largest int64) {
	t.Helper()
	rng := rand.New(rand.NewPCG(84, 84))
	write := func(rel string, b []byte) {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, b, 0o644); err != nil {
			t.Fatal(err)
		}
		files++
		largest = max(largest, int64(len(b)))
	}
	pdf := func(rel string, spec synthpdf.Spec) {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		n, err := synthpdf.WriteFile(full, spec)
		if err != nil {
			t.Fatal(err)
		}
		files++
		largest = max(largest, n)
	}
	note := strings.Repeat("Field notes on the pump station, valves and the river basin.\n", 60)
	for i := 0; i < 440; i++ {
		write(fmt.Sprintf("notes/%02d/note%03d.md", i%20, i), []byte(fmt.Sprintf("# Note %d\n\n%s", i, note)))
	}
	for i := 0; i < 40; i++ {
		b := make([]byte, 256<<10+rng.IntN(768<<10))
		for k := range b {
			b[k] = byte(rng.Uint32())
		}
		write(fmt.Sprintf("photos/img%02d.jpg", i), b)
	}
	// A data export larger than the text limit: indexed to its first
	// extract.MaxTextBytes.
	csv := strings.Repeat("2026-10-13,station-7,valve-3,pressure,4.71,ok,inspected by the night shift\n", 800000)
	write("exports/readings.csv", []byte(csv)) // ~60 MB of text
	for i := 0; i < 3; i++ {
		pdf(fmt.Sprintf("reports/report%d.pdf", i), synthpdf.Spec{Pages: 1500, LinesPerPage: 60, Seed: uint64(i + 1)}) // ~11 MB, ~9 MB of text
	}
	for i := 0; i < 3; i++ {
		pdf(fmt.Sprintf("scans/scan%d.pdf", i), synthpdf.Spec{Pages: 40, ImageBytes: 1 << 20, Seed: uint64(i + 10)}) // ~40 MB, no text
	}
	for i := 0; i < 2; i++ {
		pdf(fmt.Sprintf("manuals/manual%d.pdf", i), synthpdf.Spec{Pages: 300, LinesPerPage: 40, ImageBytes: 100 << 10, Seed: uint64(i + 20)}) // ~30 MB, ~1 MB of text
	}
	return files, largest
}

// LOC-84: a peer joining with an empty root pulls ~500 files, several of
// them large PDFs and a large CSV, and indexes them as they land. The Go
// heap in use must stay within a bound set by the largest document's text,
// not grow with the amount transferred or indexed.
//
// The heap is what Locus controls. A service manager's "memory peak" (the
// cgroup's) also counts the page cache of every file written and read: run
// under systemd-run, this scenario peaks at about 190 MB of process memory
// (the test's own reading of every file to compare the trees included) and
// 850 MB of page cache (LOC-84, LOCUS_DESIGN.md "Memory").
func TestBulkPullAndIndexHeapStaysBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("bulk transfer and indexing of ~300 MB")
	}
	c := newCluster(t, 2)
	c.dumpOnFailure()
	seed, joiner := c.nodes[0], c.nodes[1]
	joiner.stopServing(t)

	files, largest := bulkRoot(t, seed.Root)
	ctx := context.Background()
	// Every file is processed once its index row exists, with text or
	// recorded as text-free, and no work is left queued.
	indexed := func(nd *node) bool {
		st, err := nd.App.IndexStatus(ctx)
		if err != nil {
			return false
		}
		n, err := nd.App.Queue.PendingCount(ctx)
		return err == nil && n == 0 && st.Index.Documents+st.Index.NoText == files
	}
	waitFor(t, 5*time.Minute, func() bool { return indexed(seed) }, "the seed to index its root")

	sampler := startHeapSampler()
	start := time.Now()
	joiner.restart(t)
	waitFor(t, 10*time.Minute, func() bool { return indexed(joiner) }, "the joiner to pull and index every file")
	base, peak, rss := sampler.Stop()
	if !treesMatch(t, seed, joiner) {
		t.Fatal("trees differ after the pull")
	}

	grew := int64(peak) - int64(base)
	// What an index worker holds for one document is bounded by the text
	// it yields, at most extract.MaxTextBytes whatever the file's size: a
	// 40 MB scan holds almost nothing, a 60 MB export only its first 16 MB.
	// At its peak a worker holds two copies of that text (the text and
	// the chunks cut from it, or the chunks and their reload for
	// embedding); there are two workers (the harness default); and the
	// GC lets the heap reach twice what is live before collecting
	// (GOGC=100). That is 2 x 2 x 2 = 8 times the largest text, plus
	// 48 MB for transfer buffers, manifests, database caches and both
	// peers' runtimes. Main before LOC-84 peaked at +258..290 MB here
	// (each worker read a text file whole with io.ReadAll and copied it
	// to a string, up to 64 MB); with the fixes the peak is about +85 MB.
	textCost := min(largest, extract.MaxTextBytes)
	bound := 8*textCost + 48<<20
	t.Logf("%d files, largest %d MB, pulled and indexed in %v; heap in use %d MB at start, peak %d MB (+%d MB), bound +%d MB; resident peak %d MB",
		files, largest>>20, time.Since(start).Round(time.Second), base>>20, peak>>20, grew>>20, bound>>20, rss>>20)
	if grew > bound {
		t.Fatalf("heap in use grew by %d MB during the pull, more than the %d MB bound", grew>>20, bound>>20)
	}
}

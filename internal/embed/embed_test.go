package embed

import (
	"context"
	"math"
	"sync"
	"testing"
)

func TestDeterministicAndNormalized(t *testing.T) {
	e := &Hashing{}
	if e.ID() != ModelID || e.Dims() != Dims {
		t.Fatalf("identity mismatch: %s %d", e.ID(), e.Dims())
	}
	in := []string{"solar panel invoice for the roof work", "insurance terms and exclusions"}
	a, err := e.Embed(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.Embed(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	for i := range a {
		if len(a[i]) != Dims {
			t.Fatalf("dims = %d", len(a[i]))
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				t.Fatal("embeddings must be deterministic")
			}
		}
		var norm float64
		for _, v := range a[i] {
			norm += float64(v) * float64(v)
		}
		if math.Abs(math.Sqrt(norm)-1) > 1e-3 {
			t.Fatalf("vectors must be L2 normalized, got norm %v", math.Sqrt(norm))
		}
	}
}

func TestCancellation(t *testing.T) {
	e := &Hashing{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Embed(ctx, []string{"a", "b"}); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestSimilarityOrdersSensibly(t *testing.T) {
	e := &Hashing{}
	texts := []string{
		"invoice for solar panel installation on the roof",
		"solar panels quoted for the rooftop installation",
		"recipe for garlic soup with bread",
	}
	vecs, err := e.Embed(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	near := Cosine(vecs[0], vecs[1])
	far := Cosine(vecs[0], vecs[2])
	if !(near > far) {
		t.Fatalf("expected related texts to score higher: near=%v far=%v", near, far)
	}
}

func TestEmptyAndUnknownWords(t *testing.T) {
	e := &Hashing{}
	vecs, err := e.Embed(context.Background(), []string{"", "?????", "the a of"})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vecs {
		if len(v) != Dims {
			t.Fatal("dimensionality must be constant")
		}
	}
	if Cosine(vecs[0], vecs[2]) != 0 {
		t.Fatal("cosine with a zero vector must be zero")
	}
}

func TestCosineDimensionMismatch(t *testing.T) {
	if Cosine([]float32{1}, []float32{1, 2}) != 0 {
		t.Fatal("mismatched dimensions must score zero")
	}
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	c := []float32{1, 0, 0}
	if math.Abs(Cosine(a, c)-1) > 1e-6 {
		t.Fatal("identical vectors must score 1")
	}
	if math.Abs(Cosine(a, b)) > 1e-6 {
		t.Fatal("orthogonal vectors must score 0")
	}
}

func TestResolveDevice(t *testing.T) {
	_, d := Resolve("")
	if d.Requested != "auto" || d.Used != "cpu" {
		t.Fatalf("auto resolution: %+v", d)
	}
	_, d = Resolve("cpu")
	if d.Used != "cpu" {
		t.Fatalf("cpu request: %+v", d)
	}
	// A machine without an accelerated runtime must fall back cleanly rather
	// than fail: a CPU-only peer has to work.
	em, d := Resolve("gpu")
	if d.Requested != "gpu" {
		t.Fatalf("gpu request recorded wrong: %+v", d)
	}
	if !d.GPUFound && d.Used != "cpu" {
		t.Fatalf("gpu request without hardware must fall back to cpu: %+v", d)
	}
	if em == nil || em.ID() == "" {
		t.Fatal("resolution must always yield a working embedder")
	}
}

func TestConcurrentEmbedIsStable(t *testing.T) {
	e := &Hashing{}
	texts := make([]string, 50)
	for i := range texts {
		texts[i] = "document " + string(rune('a'+i%26)) + " about distributed systems and reconciliation"
	}
	results := make([][][]float32, 4)
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			results[k], errs[k] = e.Embed(context.Background(), texts)
		}(k)
	}
	wg.Wait()
	want, err := e.Embed(context.Background(), texts)
	if err != nil {
		t.Fatal(err)
	}
	for k := range results {
		if errs[k] != nil {
			t.Fatal(errs[k])
		}
		for i := range want {
			for j := range want[i] {
				if want[i][j] != results[k][i][j] {
					t.Fatal("embedding not reproducible under concurrency")
				}
			}
		}
	}
}

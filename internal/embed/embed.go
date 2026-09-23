// Package embed defines the pluggable embedding interface and the default
// fully local runtime. No cloud API is involved; search APIs depend only on
// the interface so the inference implementation can be replaced.
package embed

import (
	"context"
	"hash/fnv"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// ModelID identifies the embedding implementation and version. Changing it
// marks stored embeddings stale so they regenerate.
const ModelID = "locus-hashing-v1"

// Dims is the vector dimensionality produced by the default runtime.
const Dims = 256

// Embedder produces vectors for chunk texts.
type Embedder interface {
	// ID is the stable model identifier stored with each vector.
	ID() string
	// Dims is the vector dimensionality.
	Dims() int
	// Embed returns one L2-normalized vector per input text.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Device describes the resolved inference device.
type Device struct {
	Requested string `json:"requested"`
	Used      string `json:"used"`
	GPUFound  bool   `json:"gpu_found"`
	Note      string `json:"note,omitempty"`
}

// Detect reports whether hardware acceleration is available. The default
// runtime is pure Go and always runs correctly on CPU; detection exists so a
// native runtime attached to this interface can opt in.
func Detect() Device {
	gpu := GPUAvailable()
	d := Device{Requested: "auto", Used: "cpu", GPUFound: gpu}
	if gpu {
		d.Note = "hardware detected; default pure-Go runtime still uses CPU, native runtime can opt in"
	}
	return d
}

// GPUAvailable checks the common Linux GPU markers without requiring drivers.
func GPUAvailable() bool {
	if st, err := os.Stat("/proc/driver/nvidia"); err == nil && st.IsDir() {
		return true
	}
	if _, err := exec.LookPath("nvidia-smi"); err == nil {
		return true
	}
	if drivers, _ := filepath.Glob("/sys/bus/pci/devices/*/driver"); len(drivers) > 0 {
		for _, d := range drivers {
			resolved, err := filepath.EvalSymlinks(d)
			if err != nil {
				continue
			}
			if v, err := os.ReadFile(filepath.Join(filepath.Dir(d), "class")); err == nil {
				class := strings.TrimSpace(string(v))
				driver := strings.ToLower(filepath.Base(resolved))
				if strings.HasPrefix(class, "0x03") && (strings.Contains(driver, "nvidia") || strings.Contains(driver, "amdgpu")) {
					return true
				}
			}
		}
	}
	return false
}

// Resolve returns an embedder honoring the requested device (auto|cpu|gpu).
// GPU falls back cleanly to CPU when no accelerated runtime is present.
func Resolve(requested string) (Embedder, Device) {
	if requested == "" {
		requested = "auto"
	}
	d := Detect()
	d.Requested = requested
	switch requested {
	case "gpu":
		if d.GPUFound {
			d.Used = "gpu"
		} else {
			d.Used = "cpu"
			d.Note = "gpu requested but no accelerated runtime available; using CPU"
		}
	default:
		d.Used = "cpu"
	}
	return &Hashing{}, d
}

// Hashing is the default local embedder: a deterministic hashed
// bag-of-words model with L2 normalization. It is CPU-only, dependency-free
// and produces stable vectors; semantic/neural runtimes implement Embedder
// and can replace it without touching search.
type Hashing struct{}

// ID implements Embedder.
func (h *Hashing) ID() string { return ModelID }

// Dims implements Embedder.
func (h *Hashing) Dims() int { return Dims }

// Embed implements Embedder.
func (h *Hashing) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out[i] = embedOne(t)
	}
	return out, nil
}

var (
	tokenizeOnce sync.Once
	stopwords    map[string]bool
)

func init() {
	tokenizeOnce.Do(func() {
		stopwords = map[string]bool{}
		for _, w := range strings.Fields("a an and are as at be but by for if in is it of on or so that the this to was with") {
			stopwords[w] = true
		}
	})
}

func tokens(s string) []string {
	fields := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !(r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 0xc0)
	})
	out := fields[:0]
	for _, f := range fields {
		if len(f) > 1 && !stopwords[f] {
			out = append(out, f)
		}
	}
	return out
}

func embedOne(text string) []float32 {
	v := make([]float32, Dims)
	for _, tok := range tokens(text) {
		h := fnv.New64a()
		h.Write([]byte(tok))
		idx := int(h.Sum64() % uint64(Dims))
		sign := float32(1.0)
		if h.Sum64()&(1<<32) != 0 {
			sign = -1.0
		}
		v[idx] += sign
		// Character trigrams add robustness for partial words.
		for i := 0; i+3 <= len(tok); i++ {
			h2 := fnv.New64a()
			h2.Write([]byte(tok[i : i+3]))
			idx2 := int(h2.Sum64() % uint64(Dims))
			v[idx2] += sign * 0.5
		}
	}
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	norm = math.Sqrt(norm)
	if norm == 0 {
		return v
	}
	for i := range v {
		v[i] = float32(float64(v[i]) / norm)
	}
	return v
}

// Cosine returns the cosine similarity of two vectors.
func Cosine(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

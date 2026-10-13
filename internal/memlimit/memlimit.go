// Package memlimit resolves the daemon's soft memory limit (LOC-84): the
// memory_limit setting, by default a quarter of the machine's memory, handed
// to the Go runtime with debug.SetMemoryLimit. Below the limit the garbage
// collector paces itself as usual (GOGC); approaching it, it collects more
// often and returns freed memory to the system sooner, so a burst of work
// does not leave the heap at twice its live size. It is a soft limit: the
// process is never killed or refused memory for reaching it.
package memlimit

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	// DefaultPercent is the share of the machine's memory used when
	// memory_limit is not set.
	DefaultPercent = 25
	// Floor is the least a percentage (the default included) resolves to,
	// so a small machine or container still leaves the daemon room for its
	// index workers.
	Floor = 256 << 20
	// Minimum is the least an explicit size may be.
	Minimum = 64 << 20
)

// Setting is a parsed memory_limit value.
type Setting struct {
	// Off disables the limit ("off").
	Off bool
	// Percent, when positive, is a share of the machine's memory ("25%").
	Percent float64
	// Bytes, when positive, is an absolute limit ("1GiB", "512MB").
	Bytes int64
}

var units = []struct {
	suffix string
	mult   float64
}{
	// Longest suffixes first, so "MiB" is not read as "B".
	{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
	{"kb", 1e3}, {"mb", 1e6}, {"gb", 1e9}, {"tb", 1e12},
	{"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}, {"t", 1 << 40},
	{"b", 1},
}

// Parse reads a memory_limit value: "" (the default), "off", a percentage
// of the machine's memory such as "25%", or a size such as "1GiB", "512MB",
// "768m" or a plain number of bytes. K, M, G and T without "B" are binary
// units, as in GOMEMLIMIT's KiB/MiB/GiB.
func Parse(s string) (Setting, error) {
	v := strings.ToLower(strings.TrimSpace(s))
	switch v {
	case "":
		return Setting{Percent: DefaultPercent}, nil
	case "off", "none":
		return Setting{Off: true}, nil
	}
	if num, ok := strings.CutSuffix(v, "%"); ok {
		p, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
		if err != nil || p <= 0 || p > 100 || math.IsNaN(p) {
			return Setting{}, fmt.Errorf("memory_limit %q: a percentage must be above 0 and at most 100", s)
		}
		return Setting{Percent: p}, nil
	}
	mult := 1.0
	for _, u := range units {
		if num, ok := strings.CutSuffix(v, u.suffix); ok {
			v, mult = strings.TrimSpace(num), u.mult
			break
		}
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil || n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) || n*mult > math.MaxInt64/2 {
		return Setting{}, fmt.Errorf("memory_limit %q: want \"off\", a percentage like \"25%%\" or a size like \"1GiB\"", s)
	}
	b := int64(n * mult)
	if b < Minimum {
		return Setting{}, fmt.Errorf("memory_limit %q is below the %d MiB minimum", s, Minimum>>20)
	}
	return Setting{Bytes: b}, nil
}

// Resolve turns a memory_limit value into a limit in bytes for a machine
// with total bytes of memory (0 when unknown). It returns 0 when no limit
// applies: the setting is "off", or it is a percentage and total is unknown.
func Resolve(s string, total uint64) (int64, error) {
	set, err := Parse(s)
	if err != nil {
		return 0, err
	}
	switch {
	case set.Off:
		return 0, nil
	case set.Bytes > 0:
		return set.Bytes, nil
	case total == 0:
		return 0, nil
	}
	limit := int64(float64(total) * set.Percent / 100)
	return max(limit, Floor), nil
}

// EnvVar is the Go runtime's own soft memory limit variable. When it is set
// the runtime has already applied it, and it takes precedence over the
// configuration.
const EnvVar = "GOMEMLIMIT"

// Apply sets the process's soft memory limit from the memory_limit value s,
// unless GOMEMLIMIT is set, and logs what applies. It returns the limit in
// effect (math.MaxInt64 when there is none).
func Apply(s string, log *slog.Logger) int64 {
	if log == nil {
		log = slog.Default()
	}
	if env := os.Getenv(EnvVar); env != "" {
		limit := debug.SetMemoryLimit(-1)
		log.Info("memory limit from environment", "GOMEMLIMIT", env, "limit_mib", limit>>20)
		return limit
	}
	total, terr := SystemMemory()
	limit, err := Resolve(s, total)
	switch {
	case err != nil:
		// Validate rejects a bad value before the daemon starts; this is
		// only reached by a configuration built in code.
		log.Warn("memory limit not applied", "memory_limit", s, "err", err)
		return debug.SetMemoryLimit(-1)
	case limit == 0 && terr != nil && !isOff(s):
		log.Info("memory limit not applied: machine memory unknown", "err", terr)
		return debug.SetMemoryLimit(-1)
	case limit == 0:
		log.Info("memory limit off")
		debug.SetMemoryLimit(math.MaxInt64)
		return math.MaxInt64
	}
	debug.SetMemoryLimit(limit)
	log.Info("memory limit", "limit_mib", limit>>20, "system_mib", total>>20, "memory_limit", settingName(s))
	return limit
}

func isOff(s string) bool {
	set, err := Parse(s)
	return err == nil && set.Off
}

func settingName(s string) string {
	if strings.TrimSpace(s) == "" {
		return fmt.Sprintf("default (%d%%)", DefaultPercent)
	}
	return s
}

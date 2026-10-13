package memlimit

import (
	"bytes"
	"log/slog"
	"math"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Setting
	}{
		{"", Setting{Percent: DefaultPercent}},
		{"  ", Setting{Percent: DefaultPercent}},
		{"off", Setting{Off: true}},
		{"OFF", Setting{Off: true}},
		{"25%", Setting{Percent: 25}},
		{"12.5 %", Setting{Percent: 12.5}},
		{"100%", Setting{Percent: 100}},
		{"1GiB", Setting{Bytes: 1 << 30}},
		{"1gib", Setting{Bytes: 1 << 30}},
		{"1.5GiB", Setting{Bytes: 3 << 29}},
		{"512MiB", Setting{Bytes: 512 << 20}},
		{"512 MB", Setting{Bytes: 512e6}},
		{"2G", Setting{Bytes: 2 << 30}},
		{"768m", Setting{Bytes: 768 << 20}},
		{"2GB", Setting{Bytes: 2e9}},
		{"134217728", Setting{Bytes: 128 << 20}},
		{"134217728B", Setting{Bytes: 128 << 20}},
	} {
		got, err := Parse(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("Parse(%q) = %+v, %v; want %+v", tc.in, got, err, tc.want)
		}
	}
	for _, bad := range []string{"0", "0%", "101%", "-5%", "-1GiB", "lots", "1XB", "GiB", "NaN", "1e400", "63MiB", "1000"} {
		if got, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) = %+v; want an error", bad, got)
		}
	}
}

func TestResolve(t *testing.T) {
	const gib = 1 << 30
	for _, tc := range []struct {
		name    string
		setting string
		total   uint64
		want    int64
	}{
		{"default is a quarter of memory", "", 16 * gib, 4 * gib},
		{"default on a small machine is the floor", "", 512 << 20, Floor},
		{"default with memory unknown is no limit", "", 0, 0},
		{"percentage", "50%", 8 * gib, 4 * gib},
		{"percentage below the floor", "1%", 8 * gib, Floor},
		{"explicit size ignores memory", "768MiB", 64 * gib, 768 << 20},
		{"explicit size below the floor is kept", "128MiB", 64 * gib, 128 << 20},
		{"explicit size with memory unknown", "1GiB", 0, gib},
		{"off", "off", 16 * gib, 0},
	} {
		got, err := Resolve(tc.setting, tc.total)
		if err != nil || got != tc.want {
			t.Errorf("%s: Resolve(%q, %d) = %d, %v; want %d", tc.name, tc.setting, tc.total, got, err, tc.want)
		}
	}
	if _, err := Resolve("lots", 16*gib); err == nil {
		t.Error("Resolve of a bad setting must fail")
	}
}

func TestSystemMemoryIsKnownWhereSupported(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("machine memory unknown on this platform")
	}
	total, err := SystemMemory()
	if err != nil || total < 64<<20 {
		t.Fatalf("SystemMemory() = %d, %v; want the machine's memory", total, err)
	}
}

// Apply sets the runtime's limit from the setting, and leaves it alone when
// GOMEMLIMIT is set.
func TestApply(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))

	t.Setenv(EnvVar, "")
	if got := Apply("768MiB", log); got != 768<<20 || debug.SetMemoryLimit(-1) != 768<<20 {
		t.Fatalf("Apply(768MiB) = %d, runtime limit %d", got, debug.SetMemoryLimit(-1))
	}
	if got := Apply("off", log); got != math.MaxInt64 {
		t.Fatalf("Apply(off) = %d; want no limit", got)
	}

	debug.SetMemoryLimit(3 << 30)
	t.Setenv(EnvVar, "3GiB")
	if got := Apply("768MiB", log); got != 3<<30 {
		t.Fatalf("with GOMEMLIMIT set, Apply = %d; want the environment's 3 GiB kept", got)
	}
	if !strings.Contains(logs.String(), "GOMEMLIMIT") {
		t.Fatalf("the log does not say the limit came from GOMEMLIMIT:\n%s", logs.String())
	}
}

package memlimit

import (
	"strings"
	"testing"
)

func TestParseMeminfo(t *testing.T) {
	got, err := parseMeminfo(strings.NewReader("MemTotal:       65703424 kB\nMemFree:         4096 kB\n"))
	if err != nil || got != 65703424<<10 {
		t.Fatalf("parseMeminfo = %d, %v", got, err)
	}
	if _, err := parseMeminfo(strings.NewReader("MemFree: 1 kB\n")); err == nil {
		t.Fatal("meminfo without MemTotal must fail")
	}
}

func TestCgroupMemoryMax(t *testing.T) {
	if p, ok := cgroupV2Path("0::/system.slice/locus.service\n"); !ok || p != "/system.slice/locus.service" {
		t.Fatalf("cgroupV2Path = %q, %v", p, ok)
	}
	if _, ok := cgroupV2Path("12:memory:/x\n"); ok {
		t.Fatal("a cgroup v1 line is not the unified hierarchy")
	}
	if n, ok := parseCgroupMax("1073741824\n"); !ok || n != 1<<30 {
		t.Fatalf("parseCgroupMax(1 GiB) = %d, %v", n, ok)
	}
	if _, ok := parseCgroupMax("max\n"); ok {
		t.Fatal(`"max" is no limit`)
	}
}

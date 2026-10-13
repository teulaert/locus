package memlimit

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// SystemMemory returns the memory available to this process: the machine's
// (MemTotal in /proc/meminfo), or its cgroup's memory.max when that is
// lower, as in a container or a service with MemoryMax.
func SystemMemory() (uint64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	total, err := parseMeminfo(f)
	if err != nil {
		return 0, err
	}
	if max, ok := cgroupMax(); ok && max < total {
		total = max
	}
	return total, nil
}

// parseMeminfo returns MemTotal, in bytes, from /proc/meminfo content.
func parseMeminfo(r io.Reader) (uint64, error) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		rest, ok := strings.CutPrefix(sc.Text(), "MemTotal:")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			break
		}
		n, err := strconv.ParseUint(f[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("memlimit: MemTotal: %w", err)
		}
		if len(f) > 1 && strings.EqualFold(f[1], "kB") {
			n <<= 10
		}
		return n, nil
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("memlimit: no MemTotal in /proc/meminfo")
}

// cgroupMax returns the cgroup v2 memory.max of this process's cgroup, when
// it has a numeric one.
func cgroupMax() (uint64, bool) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0, false
	}
	path, ok := cgroupV2Path(string(b))
	if !ok {
		return 0, false
	}
	v, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", filepath.Clean("/"+path), "memory.max"))
	if err != nil {
		return 0, false
	}
	return parseCgroupMax(string(v))
}

// cgroupV2Path returns the unified hierarchy's path from /proc/self/cgroup
// content (its "0::/path" line).
func cgroupV2Path(s string) (string, bool) {
	for _, line := range strings.Split(s, "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			return p, true
		}
	}
	return "", false
}

// parseCgroupMax reads a memory.max value: a number of bytes, or "max" for
// none.
func parseCgroupMax(s string) (uint64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "max" {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 {
		return 0, false
	}
	return n, true
}

package memlimit

import "golang.org/x/sys/unix"

// SystemMemory returns the machine's memory (sysctl hw.memsize).
func SystemMemory() (uint64, error) {
	return unix.SysctlUint64("hw.memsize")
}

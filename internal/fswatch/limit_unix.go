//go:build unix

package fswatch

import "syscall"

// descriptorLimit is the process's soft RLIMIT_NOFILE, 0 when unknown. The Go
// runtime raises the soft limit to the hard one at startup (on macOS to
// what kern.maxfilesperproc allows), so this is what the process really has.
func descriptorLimit() uint64 {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return 0
	}
	return uint64(rl.Cur)
}

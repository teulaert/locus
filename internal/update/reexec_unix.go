//go:build unix

package update

import (
	"os"
	"syscall"
)

// Reexec replaces the running process with exe, keeping its arguments,
// environment and pid, so systemd and launchd see the same service process
// carry on with the new binary. It only returns on failure.
//
// Every descriptor Go opens is close-on-exec; the caller has already shut
// the daemon down (listeners, databases and the daemon lock released).
func Reexec(exe string) error {
	return syscall.Exec(exe, os.Args, os.Environ())
}

// CanReexec reports whether Reexec is supported here.
const CanReexec = true

//go:build dragonfly || freebsd || netbsd || openbsd

package fswatch

import "golang.org/x/sys/unix"

// dirOpenMode opens a directory read-only for kqueue; the BSDs have no
// O_EVTONLY.
const dirOpenMode = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC

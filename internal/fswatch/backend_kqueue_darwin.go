package fswatch

import "golang.org/x/sys/unix"

// dirOpenMode opens a directory for event notification only: O_EVTONLY does
// not count as a use of the volume, so watching does not keep an external
// disk from being ejected.
const dirOpenMode = unix.O_EVTONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC

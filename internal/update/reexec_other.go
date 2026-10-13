//go:build !unix

package update

import "errors"

// Reexec is not supported here: the caller exits and the service manager
// starts the new binary.
func Reexec(string) error { return errors.New("re-exec is not supported on this platform") }

// CanReexec reports whether Reexec is supported here.
const CanReexec = false

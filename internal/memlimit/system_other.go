//go:build !linux && !darwin

package memlimit

import "errors"

// SystemMemory is unknown on this platform; the default limit then does not
// apply, and memory_limit or GOMEMLIMIT can set one.
func SystemMemory() (uint64, error) {
	return 0, errUnknown
}

// errUnknown is SystemMemory's error where the platform has no way to ask.
var errUnknown = errors.New("memlimit: machine memory unknown on this platform")

//go:build !unix

package fswatch

// descriptorLimit is unknown where there is no RLIMIT_NOFILE.
func descriptorLimit() uint64 { return 0 }

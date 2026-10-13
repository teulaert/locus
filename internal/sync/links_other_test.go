//go:build !unix

package sync

import "testing"

// links cannot count hard links here; every file reads as having one.
func links(*testing.T, string) uint64 { return 1 }

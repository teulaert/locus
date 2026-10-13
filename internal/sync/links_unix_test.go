//go:build unix

package sync

import (
	"os"
	"syscall"
	"testing"
)

// links reports the hard-link count of the file at path.
func links(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Nlink)
	}
	return 1
}

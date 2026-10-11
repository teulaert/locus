//go:build unix

package main

import (
	"os"
	"syscall"
)

// owns reports whether path belongs to the service user.
func (s *serviceUser) owns(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == s.uid
}

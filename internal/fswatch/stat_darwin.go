package fswatch

import (
	"io/fs"
	"syscall"
)

func sysStat(info fs.FileInfo) (inode, ctimeNS int64) {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(st.Ino), int64(st.Ctimespec.Sec)*1e9 + int64(st.Ctimespec.Nsec)
	}
	return 0, 0
}

package fswatch

import (
	"io/fs"
	"time"
)

// RacyWindow is how old a file's modification time must be before its stat
// fingerprint may stand in for its content hash. Filesystem timestamps are
// coarse (one or two seconds on some filesystems, a clock tick on others),
// so a file written again within the same tick as the scan that hashed it
// keeps its mtime and size; recording the fingerprint of such a "racily
// clean" file could hide that write forever. Fresh files are therefore
// re-hashed until their mtime is safely in the past.
const RacyWindow = 2 * time.Second

// Stat is the cheap change fingerprint of a regular file: size, modification
// time and, where the platform exposes them, inode and status-change time.
// Two equal settled fingerprints mean the file's bytes are presumed
// unchanged, so its previously computed hash may be reused. The ctime also
// changes when an editor or copy tool restores an old mtime after writing,
// which mtime and size alone would miss; the inode changes when a file is
// replaced by rename.
type Stat struct {
	Size    int64
	MtimeNS int64
	CtimeNS int64
	Inode   int64
}

// StatOf returns the fingerprint of info.
func StatOf(info fs.FileInfo) Stat {
	st := Stat{Size: info.Size(), MtimeNS: info.ModTime().UnixNano()}
	st.Inode, st.CtimeNS = sysStat(info)
	return st
}

// Known reports whether the fingerprint carries any information. A zero
// fingerprint (never recorded, or recorded while the file was racy) never
// stands in for a hash.
func (s Stat) Known() bool { return s.MtimeNS != 0 }

// Settled reports whether the file's mtime lies far enough in the past that
// the fingerprint can be trusted on a later pass (see RacyWindow). An mtime
// in the future (clock skew) is never settled.
func (s Stat) Settled(now time.Time) bool {
	return s.Known() && now.UnixNano()-s.MtimeNS >= int64(RacyWindow)
}

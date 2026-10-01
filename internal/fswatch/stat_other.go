//go:build !linux && !darwin

package fswatch

import "io/fs"

// sysStat has no portable inode or ctime here; the fingerprint falls back to
// size and mtime, and the reconciler's periodic full verification covers
// edits that preserve both.
func sysStat(fs.FileInfo) (inode, ctimeNS int64) { return 0, 0 }

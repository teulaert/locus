package fswatch

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"

	"github.com/teulaert/locus/internal/pathguard"
)

// RemoveEmptyDir removes the directory root/rel if it is a real directory
// (not a symlink) with no entries at all, and reports whether it did. It
// uses os.Remove, which refuses a directory that holds anything, a hidden
// file such as .DS_Store included, so a non-empty directory can never be
// removed and no file is ever deleted. The root itself is never removed:
// rel must name something below it.
//
// A directory that does not exist yields an error matching fs.ErrNotExist,
// so a caller walking upward can tell "already gone" from "kept". Any other
// reason to keep the directory (it has entries, it is not a directory, it is
// reached through a symlink leading out of the root, the remove is not
// permitted) yields false with a nil error.
func RemoveEmptyDir(root, rel string) (bool, error) {
	if rel == "" || rel == "." {
		return false, nil
	}
	rel, err := pathguard.CleanRel(rel)
	if err != nil {
		return false, err
	}
	// The common case on a reconcile pass is a directory long gone; a
	// plain lstat answers that before the symlink checks walk the path.
	if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(rel))); errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	full, err := pathguard.Join(root, rel)
	if errors.Is(err, pathguard.ErrEscapesRoot) {
		// Reached through a symlink that leads out of the root: not ours.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	st, err := os.Lstat(full)
	if err != nil {
		return false, err
	}
	if !st.IsDir() {
		// A symlink to a directory would be removed as a link; a file is
		// never ours to remove.
		return false, nil
	}
	if err := os.Remove(full); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// PruneEmptyParents removes the directories above rel (a slash-separated path
// below root) that are left with no entries, walking upward one level at a
// time and stopping at the first directory that is kept, or at the root,
// which is never removed. A directory already gone is passed over, so a
// removal finishes the shell an interrupted earlier one left. It returns the
// relative paths it removed, deepest first.
func PruneEmptyParents(root, rel string) ([]string, error) {
	var removed []string
	for dir := path.Dir(rel); dir != "." && dir != "/" && dir != ""; dir = path.Dir(dir) {
		ok, err := RemoveEmptyDir(root, dir)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return removed, err
		}
		if !ok {
			break
		}
		removed = append(removed, dir)
	}
	return removed, nil
}

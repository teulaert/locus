// Package pathguard validates caller-supplied paths against configured
// synchronization roots. It is the single choke point for `..`, absolute and
// symlink escape attempts.
package pathguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrEscapesRoot is returned when a path resolves outside every root.
var ErrEscapesRoot = errors.New("pathguard: path escapes configured root")

// ErrBadPath is returned for malformed relative paths.
var ErrBadPath = errors.New("pathguard: invalid path")

// CleanRel normalizes a slash-separated relative path and rejects absolute
// paths, traversal components and drive-ish prefixes.
func CleanRel(rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("%w: empty", ErrBadPath)
	}
	if strings.HasPrefix(rel, "/") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: absolute path", ErrBadPath)
	}
	if len(rel) > 1 && rel[1] == ':' {
		return "", fmt.Errorf("%w: drive prefix", ErrBadPath)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(rel)))
	if clean == "." {
		return "", fmt.Errorf("%w: root itself", ErrBadPath)
	}
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: traversal", ErrBadPath)
	}
	for _, seg := range strings.Split(clean, "/") {
		if seg == ".." {
			return "", fmt.Errorf("%w: traversal", ErrBadPath)
		}
		if strings.ContainsRune(seg, 0) {
			return "", fmt.Errorf("%w: NUL byte", ErrBadPath)
		}
	}
	return clean, nil
}

// Join resolves rel under root and verifies both the lexical result and every
// existing symlink component stay inside the root.
func Join(root, rel string) (string, error) {
	rel, err := CleanRel(rel)
	if err != nil {
		return "", err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return "", err
	}
	root = filepath.Clean(root)
	full := filepath.Join(root, filepath.FromSlash(rel))
	if !within(root, full) {
		return "", ErrEscapesRoot
	}
	// Verify no existing component symlinks out of the root.
	if err := checkSymlinks(root, full); err != nil {
		return "", err
	}
	return full, nil
}

// Contains reports whether absPath is inside root.
func Contains(root, absPath string) bool {
	root = filepath.Clean(root)
	absPath = filepath.Clean(absPath)
	return within(root, absPath)
}

func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// checkSymlinks walks the existing prefix of path and rejects any symlink
// that resolves outside the root. Non-existent trailing components are fine:
// the file is created inside the root.
func checkSymlinks(root, path string) error {
	st, err := os.Lstat(path)
	if err == nil {
		if st.Mode()&os.ModeSymlink != 0 {
			target, err := filepath.EvalSymlinks(path)
			if err != nil {
				// The target may not exist yet: fall back to the lexical
				// link target, which is what a subsequent create would use.
				link, lerr := os.Readlink(path)
				if lerr != nil {
					return err
				}
				if !filepath.IsAbs(link) {
					link = filepath.Join(filepath.Dir(path), link)
				}
				if !within(root, filepath.Clean(link)) {
					return ErrEscapesRoot
				}
				return nil
			}
			if !within(root, target) {
				return ErrEscapesRoot
			}
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return nil
	}
	if within(root, parent) {
		return checkSymlinks(root, parent)
	}
	return ErrEscapesRoot
}

// RootsContains reports whether p is inside any of roots.
func RootsContains(roots []string, p string) bool {
	for _, r := range roots {
		if Contains(r, p) {
			return true
		}
	}
	return false
}

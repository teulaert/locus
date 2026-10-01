// Package pathguard validates caller-supplied paths against configured
// synchronization roots. It is the single choke point for `..`, absolute and
// symlink escape attempts.
package pathguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// ErrEscapesRoot is returned when a path resolves outside every root.
var ErrEscapesRoot = errors.New("pathguard: path escapes configured root")

// ErrBadPath is returned for malformed relative paths.
var ErrBadPath = errors.New("pathguard: invalid path")

// CleanRel normalizes a slash-separated relative path and rejects absolute
// paths, traversal components and drive prefixes.
//
// Relpaths travel between peers, so a Windows drive root (`C:`, `C:/x`,
// `C:\x`) is refused on every platform: a peer on Windows would treat it as
// absolute. Other names containing ':' (`a:b.txt`, `1:30 meeting.md`) are
// ordinary filenames on Unix and are accepted there; on Windows ':' can only
// mean a drive or an NTFS alternate data stream, so it is refused anywhere.
func CleanRel(rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("%w: empty", ErrBadPath)
	}
	if strings.HasPrefix(rel, "/") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: absolute path", ErrBadPath)
	}
	if hasDriveRoot(rel) || filepath.VolumeName(filepath.FromSlash(rel)) != "" {
		return "", fmt.Errorf("%w: drive prefix", ErrBadPath)
	}
	if runtime.GOOS == "windows" && strings.ContainsRune(rel, ':') {
		return "", fmt.Errorf("%w: ':' in name", ErrBadPath)
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

// hasDriveRoot reports whether rel starts with a Windows drive letter that
// names a drive root or an absolute path on it: `C:`, `C:/...` or `C:\...`.
// It is platform independent on purpose; see CleanRel.
func hasDriveRoot(rel string) bool {
	if len(rel) < 2 || rel[1] != ':' {
		return false
	}
	c := rel[0]
	if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z') {
		return false
	}
	return len(rel) == 2 || rel[2] == '/' || rel[2] == '\\'
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

// maxSymlinkHops bounds how many symlinks resolvePath follows, so a link loop
// cannot spin forever.
const maxSymlinkHops = 40

// checkSymlinks rejects path if any existing component of it, intermediate
// directories included, is a symlink that leads outside the root.
// Non-existent trailing components are fine: they are created under whatever
// the existing prefix resolves to, which is what gets checked.
//
// Inspecting only the leaf is not enough: os.Lstat follows intermediate
// symlinks, so root/linkdir/secret.txt with linkdir -> /outside looks like an
// ordinary file. Instead every component of the path is resolved and the
// result is compared against the resolved root.
func checkSymlinks(root, path string) error {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// Nothing under a missing root exists yet, so nothing can link out.
		return nil
	}
	resolved, err := resolvePath(path)
	if err != nil {
		return err
	}
	if !within(realRoot, resolved) {
		return ErrEscapesRoot
	}
	return nil
}

// resolvePath returns the absolute path with every symlink in it resolved,
// walking one component at a time from the filesystem root, the way the
// kernel does. Components from the first missing one onward are appended
// unchanged: that is where a subsequent create would land. Dangling links are
// followed by their link text, so a link to a not-yet-existing target is
// judged by where the target would be created.
func resolvePath(path string) (string, error) {
	vol := filepath.VolumeName(path)
	cur := vol + string(filepath.Separator)
	todo := splitPath(path[len(vol):])
	hops := 0
	for len(todo) > 0 {
		comp := todo[0]
		todo = todo[1:]
		switch comp {
		case ".":
			continue
		case "..":
			cur = filepath.Dir(cur)
			continue
		}
		next := filepath.Join(cur, comp)
		st, err := os.Lstat(next)
		if errors.Is(err, os.ErrNotExist) {
			// The rest does not exist yet; nothing below can be a link.
			return filepath.Join(append([]string{next}, todo...)...), nil
		}
		if err != nil {
			return "", err
		}
		if st.Mode()&os.ModeSymlink == 0 {
			cur = next
			continue
		}
		if hops++; hops > maxSymlinkHops {
			return "", fmt.Errorf("%w: too many symlinks", ErrEscapesRoot)
		}
		link, err := os.Readlink(next)
		if err != nil {
			return "", err
		}
		if filepath.IsAbs(link) {
			lvol := filepath.VolumeName(link)
			cur = lvol + string(filepath.Separator)
			link = link[len(lvol):]
		}
		todo = append(splitPath(link), todo...)
	}
	return cur, nil
}

func splitPath(p string) []string {
	var out []string
	for _, seg := range strings.Split(filepath.ToSlash(p), "/") {
		if seg != "" {
			out = append(out, seg)
		}
	}
	return out
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

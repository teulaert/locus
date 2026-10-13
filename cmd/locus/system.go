package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/teulaert/locus/internal/update"
)

// Defaults of `locus init --system`, matching contrib/systemd/locus.service.
const (
	systemConfigPath   = "/etc/locus/config.json"
	systemDataDir      = "/var/lib/locus"
	defaultServiceUser = "locus"
	// systemServiceBinary is the binary contrib/systemd/locus.service runs.
	// It lives in the service's own state directory, owned by the service
	// user, so the daemon can replace it when it updates itself (LOC-86).
	systemServiceBinary = "/var/lib/locus/bin/locus"
)

// serviceUser is the account a system install runs `locus serve` as.
type serviceUser struct {
	name     string
	uid, gid int
}

func lookupServiceUser(name string) (*serviceUser, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return nil, fmt.Errorf("service user %q not found (%v); create it first, e.g. useradd --system --home-dir %s --shell /usr/sbin/nologin %s",
			name, err, systemDataDir, name)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return nil, fmt.Errorf("service user %q: uid %q is not numeric", name, u.Uid)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		return nil, fmt.Errorf("service user %q: gid %q is not numeric", name, u.Gid)
	}
	return &serviceUser{name: name, uid: uid, gid: gid}, nil
}

// chownTree gives dir and everything below it to the service user, so files
// init (running as root) created there — the identity, a stale daemon.lock —
// do not lock the daemon out (LOC-64).
func (s *serviceUser) chownTree(dir string) error {
	return filepath.WalkDir(dir, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Lchown(path, s.uid, s.gid)
	})
}

// installBinary copies the running binary to dst, owned by the service
// user, unless dst already is the running binary.
func (s *serviceUser) installBinary(dst string) error {
	src, err := update.ResolveExecutable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if fi, err := os.Stat(dst); err == nil {
		if si, err := os.Stat(src); err == nil && os.SameFile(fi, si) {
			return os.Chown(dst, s.uid, s.gid)
		}
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Chown(filepath.Dir(dst), s.uid, s.gid); err != nil {
		return err
	}
	return os.Chown(dst, s.uid, s.gid)
}

package main

import (
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
)

// Defaults of `locus init --system`, matching contrib/systemd/locus.service.
const (
	systemConfigPath   = "/etc/locus/config.json"
	systemDataDir      = "/var/lib/locus"
	defaultServiceUser = "locus"
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

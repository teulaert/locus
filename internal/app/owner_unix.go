//go:build unix

package app

import (
	"os"
	"os/user"
	"strconv"
	"syscall"
)

// geteuid is a variable so tests can pose as another user.
var geteuid = os.Geteuid

// fileOwner reports the owner of path; false when it cannot be read.
func fileOwner(path string) (Account, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		return Account{}, false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return Account{}, false
	}
	return accountOf(int(st.Uid)), true
}

func currentAccount() Account { return accountOf(geteuid()) }

func accountOf(uid int) Account {
	a := Account{UID: uid}
	if u, err := user.LookupId(strconv.Itoa(uid)); err == nil {
		a.Name = u.Username
	}
	return a
}

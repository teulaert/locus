package app

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrForeignDataDir is matched (errors.Is) by a *DataDirOwnerError.
var ErrForeignDataDir = errors.New("data directory belongs to another user")

// Account names an operating-system user.
type Account struct {
	UID  int
	Name string // empty when the uid has no entry in the user database
}

func (a Account) String() string {
	if a.Name == "" {
		return fmt.Sprintf("uid %d", a.UID)
	}
	return fmt.Sprintf("%s (uid %d)", a.Name, a.UID)
}

// runAs is how the error messages tell the reader to switch user.
func (a Account) runAs() string {
	if a.Name == "" {
		return "sudo -u '#" + strconv.Itoa(a.UID) + "' locus ..."
	}
	return "runuser -u " + a.Name + " -- locus ..."
}

// DataDirOwnerError refuses to open a data directory owned by another user
// (LOC-64). Every file the process would create in it — the databases and
// their WAL files, daemon.lock, api_token, identity.json — would belong to
// the wrong user, and a daemon running as the directory's owner could then
// not open them.
type DataDirOwnerError struct {
	Dir   string
	Owner Account
	Self  Account
}

func (e *DataDirOwnerError) Error() string {
	return fmt.Sprintf("data directory %s belongs to user %s, but this command runs as %s; "+
		"run it as that user (for example: %s) so it does not leave files there the daemon cannot open",
		e.Dir, e.Owner, e.Self, e.Owner.runAs())
}

// Is makes errors.Is(err, ErrForeignDataDir) hold.
func (e *DataDirOwnerError) Is(target error) bool { return target == ErrForeignDataDir }

// CheckDataDirOwner returns a *DataDirOwnerError when dir exists and belongs
// to a user other than the one this process runs as (root included: root can
// open the files, but what it creates the daemon cannot). A missing dir is
// fine: whoever creates it owns it. Platforms without Unix ownership never
// refuse.
func CheckDataDirOwner(dir string) error {
	owner, ok := fileOwner(dir)
	if !ok {
		return nil
	}
	self := currentAccount()
	if owner.UID == self.UID {
		return nil
	}
	return &DataDirOwnerError{Dir: dir, Owner: owner, Self: self}
}

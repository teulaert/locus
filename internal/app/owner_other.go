//go:build !unix

package app

// Without Unix file ownership there is nothing to compare.
func fileOwner(string) (Account, bool) { return Account{}, false }

func currentAccount() Account { return Account{UID: -1} }

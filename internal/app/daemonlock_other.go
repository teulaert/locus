//go:build !unix && !windows

package app

import "os"

// Platforms without advisory file locks get no daemon exclusion.
func tryLockFile(*os.File) (bool, error) { return true, nil }

func unlockFile(*os.File) error { return nil }

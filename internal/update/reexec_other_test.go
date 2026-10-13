//go:build !unix

package update

const reexecHelperEnv = "LOCUS_TEST_REEXEC_HELPER"

func reexecHelper() {}

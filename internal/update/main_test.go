package update

import (
	"os"
	"testing"

	"github.com/teulaert/locus/internal/netguard"
)

// TestMain runs the package under the network guard (LOC-80): the updater's
// HTTP client then dials loopback test servers only, never GitHub.
func TestMain(m *testing.M) {
	_ = os.Setenv(netguard.EnvVar, "1")
	if os.Getenv(reexecHelperEnv) != "" {
		reexecHelper()
		return
	}
	os.Exit(m.Run())
}

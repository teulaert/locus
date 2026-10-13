package api

import (
	"os"
	"testing"

	"github.com/teulaert/locus/internal/netguard"
)

// TestMain runs the package under the network guard (LOC-80) even when the
// tests are started without make: nothing here may reach the real network or
// the daemon of the machine running the tests.
func TestMain(m *testing.M) {
	_ = os.Setenv(netguard.EnvVar, "1")
	os.Exit(m.Run())
}

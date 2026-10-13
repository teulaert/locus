package discovery

import (
	"net/netip"
	"os"
	"testing"

	"github.com/teulaert/locus/internal/netguard"
)

// TestMain runs the package under the network guard (LOC-80) even when the
// tests are started without make, and gives books no own addresses: the
// fixed addresses the tests use (10.42.0.x) may be the test machine's own.
// Tests of either rule set them explicitly.
func TestMain(m *testing.M) {
	_ = os.Setenv(netguard.EnvVar, "1")
	systemOwnAddrs = func() []netip.Addr { return nil }
	os.Exit(m.Run())
}

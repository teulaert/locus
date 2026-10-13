// Package netguard keeps test runs off the real network (LOC-80).
//
// A test suite run on a machine whose own Locus daemon is running must never
// reach that daemon or the peers it talks to: test peers with fresh
// identities that probe the real discovery port, or the hub found through
// the real routing table, end up in the learned peers of every daemon on the
// network. Under EnvVar (set by every test target of the Makefile and by the
// test helpers that start the binary) nothing is derived from the routing
// table or `wg show`, and only loopback addresses on ports other than the
// default Locus ports are probed or listened on.
package netguard

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"

	"github.com/teulaert/locus/internal/config"
)

// EnvVar turns the guard on when set to anything but "" or "0".
const EnvVar = "LOCUS_TEST_NO_NETWORK"

// ErrBlocked is returned for an address or port the guard refuses.
var ErrBlocked = errors.New("refused under " + EnvVar)

// Active reports whether the guard is on.
func Active() bool {
	v := os.Getenv(EnvVar)
	return v != "" && v != "0"
}

// reservedPort reports whether port is one of the default Locus ports, where
// a real daemon on this machine listens.
func reservedPort(port int) bool {
	return port == config.DefaultDiscoveryPort || port == config.DefaultPeerPort || port == config.DefaultAPIPort
}

// CheckDial returns ErrBlocked when the guard is on and addr is not a
// loopback IP literal on a non-default port.
func CheckDial(addr string) error {
	if !Active() {
		return nil
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: %w", addr, ErrBlocked)
	}
	ip, err := netip.ParseAddr(host)
	port, perr := strconv.Atoi(portStr)
	if err != nil || perr != nil || !ip.Unmap().IsLoopback() || reservedPort(port) {
		return fmt.Errorf("%s: %w", addr, ErrBlocked)
	}
	return nil
}

// CheckListen returns ErrBlocked when the guard is on and port is one of the
// default Locus ports.
func CheckListen(port int) error {
	if Active() && reservedPort(port) {
		return fmt.Errorf("port %d: %w", port, ErrBlocked)
	}
	return nil
}

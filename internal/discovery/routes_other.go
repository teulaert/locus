//go:build !linux && !darwin

package discovery

import "errors"

// readSystemRoutes is unsupported here: no gateway-derived targets, and
// discovery.peers is required.
func readSystemRoutes() ([]Route, error) {
	return nil, errors.New("discovery: reading the routing table is not supported on this platform")
}

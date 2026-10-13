package discovery

import "os"

// readSystemRoutes reads the IPv4 routing table from /proc/net/route, which
// every user may read.
func readSystemRoutes() ([]Route, error) {
	b, err := os.ReadFile("/proc/net/route")
	if err != nil {
		return nil, err
	}
	return parseProcNetRoute(string(b)), nil
}

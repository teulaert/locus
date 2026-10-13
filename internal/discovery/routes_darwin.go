package discovery

import (
	"context"
	"os/exec"
	"time"
)

// readSystemRoutes reads the IPv4 routing table with `netstat -rn -f inet`,
// which needs no privileges.
func readSystemRoutes() ([]Route, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "/usr/sbin/netstat", "-rn", "-f", "inet").Output()
	if err != nil {
		return nil, err
	}
	return parseNetstat(string(out)), nil
}

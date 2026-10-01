package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/teulaert/locus/internal/app"
)

// daemonClient asks the `locus serve` running on the CLI's data directory for
// its live state over the HTTP API (LOC-57). A CLI-built App only sees its own
// process, so without this the status commands could never show a round in
// progress or the daemon's last round.
type daemonClient struct {
	base  string
	token string
	hc    *http.Client
}

// errDaemonAPI wraps transport failures: the daemon holds the lock but its API
// cannot be reached (another api.addr/port, or it is still starting).
var errDaemonAPI = errors.New("daemon API unreachable")

// daemonFor returns a client for the daemon serving a's data directory, or
// nil when none is running and the local snapshot is all there is.
func daemonFor(a *app.App) *daemonClient {
	if !app.DaemonRunning(a.DataDir()) {
		return nil
	}
	tok, err := a.APIToken()
	if err != nil {
		fmt.Fprintln(os.Stderr, "locus: a daemon is running but its API token is unavailable:", err)
		return nil
	}
	host := a.Cfg.API.Addr
	// A wildcard listen address is reached on loopback.
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
		if ip != nil && ip.To4() == nil {
			host = "::1"
		}
	}
	return &daemonClient{
		base:  "http://" + net.JoinHostPort(host, strconv.Itoa(a.Cfg.API.Port)),
		token: tok,
		hc:    &http.Client{},
	}
}

// do sends a request and decodes a 200 answer into out. Transport failures
// wrap errDaemonAPI; an error answer carries the daemon's message.
func (d *daemonClient) do(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, d.base+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	resp, err := d.hc.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", errDaemonAPI, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%w: %v", errDaemonAPI, err)
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return fmt.Errorf("daemon: %s", e.Error)
		}
		return fmt.Errorf("daemon: %s %s: %s", method, path, strings.TrimSpace(resp.Status))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("daemon: decode %s: %w", path, err)
	}
	return nil
}

// live fetches path from the daemon into out when one is running. It reports
// false, after a warning, when there is no daemon or it cannot be reached, and
// the caller then uses its own snapshot.
func live(ctx context.Context, a *app.App, path string, out any) bool {
	d := daemonFor(a)
	if d == nil {
		return false
	}
	if err := d.do(ctx, http.MethodGet, path, out); err != nil {
		fmt.Fprintf(os.Stderr, "locus: %v; showing this process's snapshot instead\n", err)
		return false
	}
	return true
}

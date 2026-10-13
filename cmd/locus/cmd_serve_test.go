package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/app"
)

// A shutdown cut short at its deadline is not a failure: systemd must not
// record every restart under load as one (LOC-81). Real failures still are.
func TestServeExitCodes(t *testing.T) {
	cut := &app.ShutdownError{Timeout: 15 * time.Second, Pending: []string{"requests on 0.0.0.0:8909"}}
	for _, tc := range []struct {
		err  error
		want int
	}{
		{nil, exitOK},
		{cut, exitOK},
		{fmt.Errorf("serve: %w", cut), exitOK},
		{app.ErrDaemonRunning, exitError},
		{errors.New("listen peer port: address already in use"), exitError},
	} {
		if got := serveExit(tc.err); got != tc.want {
			t.Errorf("serveExit(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

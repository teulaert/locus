package sync

import (
	"context"
	"errors"
)

// ErrDraining ends a round's work with a peer once its context has been
// marked as draining: the transfer in progress was allowed to finish, but
// no new one starts.
var ErrDraining = errors.New("sync: shutting down; remaining transfers left for the next round")

type drainKey struct{}

// WithDrain returns a context for Round that, once drain is closed, lets
// the transfer in progress finish but starts no other. A shutdown closes
// drain, waits a bounded time for the round to return and only then
// cancels the context, so a nearly finished put is not thrown away
// (LOC-67).
func WithDrain(ctx context.Context, drain <-chan struct{}) context.Context {
	return context.WithValue(ctx, drainKey{}, drain)
}

// draining reports whether ctx's drain channel (WithDrain) is closed.
func draining(ctx context.Context) bool {
	ch, _ := ctx.Value(drainKey{}).(<-chan struct{})
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Package peer keeps the in-memory presence table of other Locus peers.
package peer

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// State is the presence state of a peer.
type State string

const (
	Online        State = "online"
	Offline       State = "offline"
	Synchronizing State = "synchronizing"
	Incompatible  State = "incompatible"
)

// Info describes a peer as announced in discovery handshakes.
type Info struct {
	PeerID       string   `json:"peer_id"`
	Hostname     string   `json:"hostname"`
	Version      string   `json:"version"`
	SyncAddr     string   `json:"sync_addr"`
	Capabilities []string `json:"capabilities,omitempty"`
}

// Entry is one row of the presence table.
type Entry struct {
	Info
	State     State  `json:"state"`
	Addr      string `json:"addr"`
	FirstSeen int64  `json:"first_seen"`
	LastSeen  int64  `json:"last_seen"`
	LastError string `json:"last_error,omitempty"`
	// Failures counts consecutive synchronization rounds in which this peer
	// could not be reached for transfer, even though it may answer discovery
	// probes. A peer that only answers probes cannot actually sync.
	Failures int `json:"failures"`
	// Reachable is false while transport failures are outstanding.
	Reachable bool      `json:"reachable"`
	Protocol  int       `json:"protocol"`
	Since     time.Time `json:"-"`
}

// Table is a concurrency-safe presence table.
type Table struct {
	mu      sync.Mutex
	entries map[string]*Entry
	timeout time.Duration
	version string
}

// NewTable creates a presence table; entries older than timeout without a
// successful probe become offline.
func NewTable(timeout time.Duration) *Table {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Table{entries: map[string]*Entry{}, timeout: timeout}
}

// Observe records a successful handshake with a peer.
func (t *Table) Observe(addr string, info Info, protocol int, compatible bool, version string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[info.PeerID]
	if !ok {
		e = &Entry{FirstSeen: time.Now().Unix(), Reachable: true}
		t.entries[info.PeerID] = e
	}
	e.Info = info
	e.Addr = addr
	e.Protocol = protocol
	e.LastSeen = time.Now().Unix()
	if e.Failures == 0 {
		// Keep the transport failure visible: probes succeeding while
		// transfers fail is the outage worth reporting.
		e.LastError = ""
	}
	e.Since = time.Now()
	switch {
	case !compatible:
		e.State = Incompatible
	default:
		if e.State != Synchronizing {
			e.State = Online
		}
	}
	_ = version
}

// ObserveFailure records a probe failure for a peer ID seen at addr.
func (t *Table) ObserveFailure(addr, peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if peerID != "" {
		if e, ok := t.entries[peerID]; ok {
			e.State = Offline
			e.LastError = "probe failed"
		}
		return
	}
	for _, e := range t.entries {
		if e.Addr == addr {
			e.State = Offline
			e.LastError = "probe failed"
		}
	}
}

// SetState updates the state of one peer (e.g. synchronizing).
func (t *Table) SetState(peerID string, state State) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[peerID]; ok {
		e.State = state
	}
}

// MarkTransportFailure records that a synchronization round could not be
// completed with this peer, and returns how many consecutive failures that is.
// A successful probe does not clear it: a peer behind a one-directional
// firewall answers probes and still cannot be dialed.
func (t *Table) MarkTransportFailure(peerID, msg string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[peerID]
	if !ok {
		return 1
	}
	e.Failures++
	e.LastError = msg
	e.Reachable = false
	return e.Failures
}

// ClearTransportFailures notes a completed round with this peer.
func (t *Table) ClearTransportFailures(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.entries[peerID]; ok {
		e.Failures = 0
		e.LastError = ""
		e.Reachable = true
	}
}

// Expire marks peers offline whose last probe is older than the timeout.
func (t *Table) Expire() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range t.entries {
		if e.State != Incompatible && !e.Since.IsZero() && time.Since(e.Since) > t.timeout {
			e.State = Offline
		}
	}
}

// Snapshot returns presence entries sorted by peer ID.
func (t *Table) Snapshot() []*Entry {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*Entry, 0, len(t.entries))
	for _, e := range t.entries {
		c := *e
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerID < out[j].PeerID })
	return out
}

// Known reports whether peerID has been discovered, whatever its state.
func (t *Table) Known(peerID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.entries[peerID]
	return ok
}

// Online returns peers eligible for synchronization.
func (t *Table) Online() []*Entry {
	var out []*Entry
	for _, e := range t.Snapshot() {
		if e.State == Online || e.State == Synchronizing {
			out = append(out, e)
		}
	}
	return out
}

// JSON renders the snapshot for CLI/API consumption.
func (t *Table) JSON() ([]byte, error) { return json.Marshal(t.Snapshot()) }

package app

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/teulaert/locus/internal/discovery"
	"github.com/teulaert/locus/internal/peer"
)

// ErrRendezvous refuses an operation that needs roots on a rendezvous peer.
var ErrRendezvous = errors.New("rendezvous peer: no roots")

// systemGatewayTargets derives probe targets from the routing table (LOC-76):
// one address for every route through a WireGuard interface. An interface
// named by LOCUS_WG_INTERFACE or peer.interface counts as WireGuard whatever
// it looks like. discovery.gateways=false turns it off.
func (a *App) systemGatewayTargets() []string {
	if !a.Cfg.GatewayDiscovery() {
		return nil
	}
	return discovery.GatewayTargets(a.Cfg.Discovery.Port, os.Getenv("LOCUS_WG_INTERFACE"), a.Cfg.Peer.Interface)
}

// rendezvousCapabilities is a rendezvous peer's announcement: no "sync" (so
// other peers never pick it for a round), but peer exchange and the
// discovery port, which is all it is for.
func (a *App) rendezvousCapabilities() []string {
	return []string{
		peer.RendezvousCapability,
		peer.PeerExchangeCapability,
		peer.DiscoveryPortCapability(a.Cfg.Discovery.Port),
	}
}

// rendezvousLoop is the main loop of a rendezvous peer, whose discovery
// responder, prober and API Serve has already started: it persists presence
// and the address book (so a restart keeps the learned peers) and maintains
// the audit log, until ctx ends. Nothing synchronizes, indexes or watches.
func (a *App) rendezvousLoop(ctx context.Context) error {
	a.Log.Info("rendezvous peer: answering discovery and sharing learned peers; no roots, no sync")
	presenceTicker := time.NewTicker(5 * time.Second)
	defer presenceTicker.Stop()
	a.MaintainAudit()
	auditTicker := time.NewTicker(a.Cfg.Audit.Interval.D())
	defer auditTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.persistPresence()
			a.persistLearnedPeers(false)
			return nil
		case <-auditTicker.C:
			a.MaintainAudit()
		case <-presenceTicker.C:
			a.persistPresence()
			a.persistLearnedPeers(false)
		}
	}
}

# Server peers and discovery

A peer that never sleeps helps the others converge, and a rendezvous peer on
the WireGuard hub lets clients find each other with nothing configured. The
addresses on this page (`10.42.0.0/16` for the tunnel, `10.42.0.1` for the
hub, `10.42.2.30` for the server) are examples from a private range;
substitute your own.

## How peers find each other

Peers find each other over WireGuard with nothing configured. Each probes one
address for every route through a WireGuard interface (the route's gateway,
or the first address of its prefix: a route to `10.42.0.0/16` probes
`10.42.0.1`, the hub), the tunnel addresses of its WireGuard peers where the
`wg` tool can list them, its `discovery.peers`, and every peer it has learned
about. A peer answering a probe lists the peers it has verified itself (peer
exchange), and a peer that is probed learns the prober, so on a
hub-and-spoke network with a [rendezvous peer on the
hub](#zero-config-discovery-on-a-hub-and-spoke-tunnel) every client needs
only:

```bash
locus init --root ~/Documents   # on every client
```

`locus init` lists the addresses it derived (`discover: 10.42.0.1:8911`, or
`gateway_targets` with `--json`). A node with no WireGuard interface, such as
a container on the hub, probes its private default gateway instead, which
on the hub is the rendezvous peer. Pass `--discovery-peer <host>` only where no
WireGuard route leads to an always-on peer: a container or VM that reaches
the tunnel through its host's routing and has no WireGuard interface of its
own, a full-mesh network whose peers `wg` cannot list (it needs root), or a
hub that runs no Locus. The clients meet through the hub, then probe and
sync with each other directly. Learned peers are kept in `state.db`, so two
clients keep finding each other while the always-on peer is down and after
they restart. Peer lists are signed with the rest of the reply, only accepted
from peers in `authorized_peers` (when set) and about such peers, and only
name IP addresses: nothing is resolved, scanned or probed that a verified
peer did not report or the configuration does not list. They must be able to
reach each other in **both** directions:

```bash
sudo ufw allow in on wg0 to any port 8909 proto tcp  # sync
sudo ufw allow in on wg0 to any port 8911 proto udp  # discovery
```

Edits made while a peer is offline are exchanged when it returns; there is
nothing to click.

## Running an always-on server peer

Peers are equal, but one that never sleeps makes the others converge sooner:
a laptop that edits offline meets the server instead of waiting for the other
laptop. A server peer is an ordinary peer run as a system service. The worked
example below is an Incus container on the WireGuard network at `10.42.2.30`
syncing `/backup/Documents`, with its data on the same persistent volume.

**1. A host with persistent storage.** Use a container or VM whose sync roots
*and* data directory live on a persistent volume, so a rebuilt container keeps
its identity (`identity.json`) and sync state; losing the data directory makes
the peer a new device that re-adopts every file. The peer must be on the
WireGuard (overlay) network, and its firewall must allow the sync port (TCP
8909) and discovery port (UDP 8911) inbound on that interface.

**2. Install a service user, the binary and the unit.**

```bash
make build                       # as yourself
sudo useradd --system --home-dir /var/lib/locus --shell /usr/sbin/nologin locus
sudo make install-system         # /var/lib/locus/bin/locus (owned by locus),
                                 # /usr/local/bin/locus -> it, /etc/systemd/system/locus.service,
                                 # /etc/locus/config.json.example
```

The service runs `/var/lib/locus/bin/locus`, a binary the `locus` user
owns, so that it can [update itself](updates.md#keeping-peers-up-to-date);
`/usr/local/bin/locus` is a symlink to it, so the CLI is always the same
build as the daemon. `make install-system` does not build (so `sudo` needs
no Go toolchain) and never replaces an existing `config.json`. `DESTDIR`,
`SERVICE_BIN_DIR`, `SERVICE_USER`, `SYSTEM_BIN_DIR`, `SYSTEMD_UNIT_DIR` and
`SYSTEM_CONFIG_DIR` override where things go.

**3. Create the configuration and identity.**

```bash
sudo locus --data-dir /backup/locus init --system --root /backup/Documents
```

`init --system` writes `/etc/locus/config.json` (or `--config`), creates the
data directory (default `/var/lib/locus`, or `--data-dir`) and the peer
identity, hands both to the service user (`--user`, default `locus`), and
prints the full peer id. Roots it creates belong to the service user too; an
existing root is left alone, with a warning if the service user does not own
it. Copying `config.json.example` and running `locus init` as the service user
achieves the same by hand.

**4. Point it at the overlay.** Edit `/etc/locus/config.json`:

```json
{
  "roots": ["/backup/Documents"],
  "data_dir": "/backup/locus",
  "peer": { "addr": "10.42.2.30", "port": 8909 },
  "discovery": { "peers": ["10.42.2.10", "10.42.2.11"] },
  "authorized_peers": ["<laptop peer id>", "<desktop peer id>"]
}
```

`peer.addr` binds the sync listener to the WireGuard address only.
`discovery.peers` is only needed because this container has no WireGuard
interface of its own to derive the hub from: with a [rendezvous
peer](#zero-config-discovery-on-a-hub-and-spoke-tunnel) on the hub,
`["10.42.0.1"]` is enough. Without one, list the clients' tunnel addresses:
the hardened unit has no `CAP_NET_ADMIN`, so the `wg` tool cannot list the
WireGuard peers for it. `authorized_peers` (optional) pins which peer ids may
sync with it.

**5. Let systemd write the data directory and roots, and start it.** The unit
uses `ProtectSystem=strict` and `ProtectHome=yes`, so only `/var/lib/locus` is
writable unless a drop-in adds more (`sudo systemctl edit locus`):

```ini
[Service]
ReadWritePaths=/backup/locus /backup/Documents
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now locus
journalctl -u locus -f
```

**6. Add a client.** On each client, add the server to `discovery.peers`
(`"10.42.2.30"`) and its peer id to `authorized_peers`, then restart the
client's `locus serve`. On the server, add the client's peer id (from
`locus status --json | jq -r .peer_id` on the client) to `authorized_peers`
and its tunnel address to `discovery.peers`, then
`sudo systemctl restart locus`. List each other on both sides: either side
can then start contact, and each side checks the other's id against its own
`authorized_peers` (an empty list admits any peer). `locus peers` on either
side shows the other as `online` once they meet.

**Run the CLI as the service user, never as root.** Every command opens the
data directory, and what root creates there (databases, WAL files,
`daemon.lock`, `api_token`) the service cannot open afterwards: `locus serve`
then fails with `open daemon lock: permission denied` and systemd restarts it
in a loop. Locus refuses such a command (exit 4, `data directory ... belongs
to user locus`), so use:

```bash
sudo runuser -u locus -- locus --config /etc/locus/config.json status
```

A small wrapper saves typing; install it as `/usr/local/sbin/locus` so it
shadows the binary for root:

```bash
#!/bin/sh
exec runuser -u locus -- /usr/local/bin/locus --config /etc/locus/config.json "$@"
```

With `/usr/local/bin/locus` a symlink to the service's
`/var/lib/locus/bin/locus`, the wrapper always runs the build the daemon
runs, also after it updated itself. (`/var/lib/locus` is private to the
`locus` user, so only root and `locus` can follow the symlink.)

If an older build already left root-owned files behind, give them back:
`sudo chown -R locus:locus /backup/locus` (the error names the owner).

## Zero-config discovery on a hub-and-spoke tunnel

On a hub-and-spoke WireGuard network every client has one WireGuard peer,
the hub, whose allowed-ips is the whole tunnel (`10.42.0.0/16`), so the
WireGuard configuration names no other client to probe; and reading it
(`wg show`) needs root. Locus reads the routing table instead, which needs
no privileges: every route through a WireGuard interface into a private
range (RFC 1918 or `100.64.0.0/10`) yields **one** address, the route's
gateway or else the first address of its prefix. A client with
`10.42.0.3/24` and a route `10.42.0.0/16 dev wg0` probes `10.42.0.1` and
nothing else; this is never a scan. An interface counts as WireGuard when it
is named `wg*` or `utun*` (macOS), when it is point-to-point with no
link-layer address (as WireGuard interfaces are on Linux, whatever their
name), or when `peer.interface` or `LOCUS_WG_INTERFACE` names it. The
targets are refreshed every probe round, so a tunnel that comes up later is
found. A node without any WireGuard interface, such as a container running
on the hub itself, probes its default gateway when that is a private
address; for such a container the gateway is the hub's bridge address, where
the rendezvous peer answers. A public default gateway is never probed, and
on a network whose router is not a Locus peer the probe simply goes
unanswered.

Run a **rendezvous peer** on the hub: a discovery-only Locus with no roots.
It answers probes, learns every client that probes it, hands each client the
others (and keeps the list in `state.db` across restarts), and runs no
synchronization, indexing or file watching. Clients show it in `locus peers`
as `rendezvous` and never sync with it. On an Ubuntu hub with `wg0`:

```bash
make build                                   # on a build machine, then copy bin/locus,
                                             # or download a release binary
sudo install -m 0755 locus /usr/local/bin/locus
sudo install -m 0644 contrib/systemd/locus.service /etc/systemd/system/locus.service
sudo useradd --system --home-dir /var/lib/locus --shell /usr/sbin/nologin locus
sudo locus init --system --rendezvous        # also copies the binary to /var/lib/locus/bin/locus
sudo ufw allow in on wg0 to any port 8911 proto udp   # discovery only; no sync port
sudo systemctl daemon-reload && sudo systemctl enable --now locus
sudo runuser -u locus -- locus --config /etc/locus/config.json peers
```

Then on every client `locus init --root ~/Documents` and `locus serve`. Once
two clients have probed the hub they appear in each other's `locus peers` as
`online` and sync directly, so they must reach each other on the sync port
(TCP 8909) and discovery port (UDP 8911): the hub must forward between
clients (`AllowedIPs` covering the tunnel on each client, IP forwarding on
the hub) and the clients' firewalls must allow both ports in on the tunnel.
`locus status`, `peers`, `sync status` and `index status` work on the
rendezvous peer; `sync now`, `index rebuild`, `search` and `conflicts` exit 4
with `rendezvous peer: no roots`.

A peer whose routes do not lead to the hub still needs `--discovery-peer`
(for instance a container that reaches the tunnel through its host), and a
network without a hub running Locus keeps working the old way: clients list
the always-on peer in `discovery.peers`. Rendezvous peers announce no `sync`
capability; peers on builds older than this one do not know that and try to
sync with it, failing with `connection refused` and backing off, so upgrade
the clients along with the hub.

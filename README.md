# Locus

Locus keeps plain files on your own filesystems in sync between your own
machines, and makes them searchable — offline, without a server, and without a
database you have to trust with your documents.

It is a single static Go binary. `locus serve` runs the peer: it watches your
sync roots, replicates changes to the other peers it can see, extracts and
indexes text, serves hybrid (lexical + semantic) search, and exposes a local
HTTP API and browser UI. Your documents stay ordinary files in ordinary
directories; nothing about Locus is on their path to being readable.

## Design invariants

* **Files are canonical.** The filesystem is the source of truth. The two
  SQLite databases hold only metadata: `state.db` (documents, version vectors,
  tombstones, conflicts — losing it means re-adoption) and `index.db` (chunks,
  embeddings and the indexing queue — derived, disposable, rebuildable with
  `locus index rebuild`).
* **Peers are equal.** There is no primary, no coordinator and no relay. Any
  number of peers can be up or down in any combination; they converge when they
  meet.
* **Causality, not clocks.** Versions are version vectors. Two peers editing
  the same file offline produce a *conflict*: both versions are kept, one on the
  normal path and one as a `<name>.conflict-<peer8>-<hash8>.<ext>` sibling, and
  the winner is chosen deterministically from content hashes so every peer
  agrees: the greater content hash takes the normal path, and the writer's
  peer ID only breaks a tie between identical contents. An edit concurrent
  with a delete always survives.
* **Transfers are atomic.** Content is streamed to a `.locus-tmp-*` file,
  hash-verified, fsynced, then renamed into place (and the directory fsynced)
  before the hash is recorded. A killed peer or a power loss never leaves a
  partial document, and leftovers from a crash are swept by later passes.
* **Local-first network.** Discovery and sync assume a trusted WireGuard
  network. There is no NAT traversal, relay or public endpoint.
* **Hidden paths stay local.** Any file or directory whose name starts with
  `.` (`.git`, `.obsidian`, `.DS_Store`, `.Trash-1000`, `.venv`, ...) is
  per-machine state, not a document: it is not inventoried, indexed or
  synchronized, and refused when a peer sends it. A root can opt in with
  `sync_hidden` (see [Configuration](#configuration)).

## Install

Go 1.25 or newer is the only requirement; everything else is pure Go
(`CGO_ENABLED=0`), including the SQLite driver and the embedder.

```bash
make install      # → ~/.local/bin/locus
make build        # → ./bin/locus
```

`make install` builds a version-stamped binary and copies it into
`$HOME/.local/bin`, printing a note if that directory is not on your `PATH`.
Choose somewhere else with `make install INSTALL_DIR=~/bin`. To run it as a
service see [Running Locus as a service](#running-locus-as-a-service), and for
a server see [Running an always-on server peer](#running-an-always-on-server-peer).

## Quick start

On each machine, point Locus at the directories you want replicated:

```bash
locus init --root ~/Documents --root ~/Notes
locus serve
```

Machines pair roots by name, which defaults to the folder name, so
`~/Documents` here syncs with `Documents` there. Use `--root docs=~/Sync/Docs`
to name a root explicitly when the folders are called differently (see
[Configuration](#configuration)).

`init` writes `~/.config/locus/config.json` (it will not replace an existing
one unless given `--force`); `serve` starts discovery, sync, indexing and the
API. Check what it sees:

```bash
locus status          # identity, roots, index, watch mode, peers, conflicts
locus peers           # presence of the other peers
```

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

## Searching

```bash
locus search "grid tariff"                  # hybrid lexical + semantic
locus search "grid tariff" --json           # stable JSON with provenance
locus search "grid tariff" --context        # compact passages for an LLM prompt
locus search "invoice" --path Finance/ --type pdf --limit 5
```

`--budget <chars>` bounds `--context` output (default 8000). Exit code 3 means
*no evidence found*, not failure. `locus skill` prints the agent-facing
documentation embedded in the binary, including every JSON shape.

## HTTP API and browser UI

`locus serve` listens on `api.addr`:`api.port` (default `127.0.0.1:8910`) and
serves a browser UI at `/`. All `/api/v1` routes require the bearer token: the
configured `api.token`, or the auto-generated one in `<data_dir>/api_token`.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/healthz` | liveness, no auth |
| GET | `/api/v1/status` | same shape as `locus status --json` |
| GET | `/api/v1/peers` | same shape as `locus peers --json` |
| GET | `/api/v1/search?q=` | as `locus search --json` (`&path=`, `&type=`, `&limit=`, `&root=`; `&context=1&budget=` as `--context`) |
| GET | `/api/v1/files?path=` | list a directory |
| GET | `/api/v1/files/content?path=` | download a file |
| POST | `/api/v1/files?path=` | upload; the body is the content (`path` ending in `/` takes `&name=`) |
| POST | `/api/v1/files/move` | rename/move, `{"from", "to"}` |
| DELETE | `/api/v1/files?path=` | delete |
| POST | `/api/v1/directories` | create a directory, `{"path"}` |
| GET | `/api/v1/conflicts` | unresolved conflicts |
| POST | `/api/v1/conflicts/resolve` | accept the normal-path version, `{"id"}` |
| GET | `/api/v1/sync` | running rounds per peer and root, as `locus sync status --json` |
| POST | `/api/v1/sync` | reconcile and sync now, as `locus sync now --json` |
| GET | `/api/v1/index` | as `locus index status --json` |
| GET | `/debug/pprof/` | Go runtime profiles (`heap`, `allocs`, `goroutine`, `profile`, `trace`); loopback clients only |

File routes act on the first configured root unless given `root=` (or
`"root"` in a JSON body) with another root's absolute path. By default the
API is loopback-only and not reachable from other machines; to use the PWA
from a phone over WireGuard, set `api.addr` to this peer's WireGuard address.

```bash
TOKEN=$(cat ~/.local/share/locus/api_token)
curl -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:8910/api/v1/search?q=grid"
```

The profiles under `/debug/pprof/` need the token too, and are served only
to a client on the same machine (loopback), whatever `api.addr` is. To see
what a running daemon holds:

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8910/debug/pprof/heap > heap.pprof
go tool pprof -sample_index=inuse_space -top heap.pprof
```

## Configuration

`~/.config/locus/config.json` (override with `--config` or `LOCUS_CONFIG_DIR`):

```json
{
  "roots": [
    "/home/you/Documents",
    { "path": "/home/you/Sync/Work", "name": "work" },
    { "path": "/home/you/Sync/Dotnotes", "name": "dotnotes", "sync_hidden": true }
  ],
  "data_dir": "",
  "peer":      { "addr": "0.0.0.0", "port": 8909, "interface": "" },
  "api":       { "addr": "127.0.0.1", "port": 8910, "token": "" },
  "discovery": { "peers": [], "port": 8911, "interval": "10s", "gateways": true },
  "sync":              { "interval": "10s" },
  "reconcile_interval": "30s",
  "workers": 2,
  "embedding": { "device": "auto" },
  "upload_max_bytes": 1073741824,
  "memory_limit": "",
  "tombstone_age": "720h",
  "authorized_peers": [],
  "audit": { "max_entries": 10000, "max_age": "2160h", "interval": "1m" },
  "update": { "auto": false, "channel": "main", "interval": "30m", "url": "" }
}
```

Only `roots` is required (a rendezvous peer, `"rendezvous": true`, has none;
see [Zero-config discovery](#zero-config-discovery-on-a-hub-and-spoke-tunnel));
roots may not be `/`, repeat or lie inside one another. A missing or zero value takes the default shown; ports must be
1–65535, and intervals, `tombstone_age`, `audit.max_age` and
`audit.max_entries` may not be negative (`workers` and `upload_max_bytes`
below 1 also mean the default). `discovery.peers` supplements the
WireGuard-derived peer list with explicit `host:port` entries (a bare host,
like every WireGuard-derived peer, is probed on `discovery.port`);
`discovery.gateways: false` stops probing the gateways of WireGuard routes;
and
`authorized_peers` restricts which peer IDs this peer discovers and
synchronizes with (empty accepts any peer it has discovered); it also limits
which peers are learned from and about through peer exchange. It does not
apply to the HTTP API, which only checks its token.
`memory_limit` is the daemon's soft Go memory limit: empty for 25% of the
memory the machine (or the daemon's cgroup) has, at least 256 MiB; `"off"`;
a percentage such as `"10%"`; or a size such as `"1GiB"` or `"512MB"` (at
least 64 MiB). Near it the garbage collector works harder; nothing fails
for reaching it. `GOMEMLIMIT` in the daemon's environment overrides it.
With `peer.addr` `0.0.0.0` the sync service listens everywhere and the address
announced to peers is chosen automatically: an address of `peer.interface`
when set (an interface name such as `wg0` or `utun4`, or a CIDR such as
`10.42.0.0/16`, useful when the WireGuard address may change), otherwise
the address of the interface whose prefix contains a discovery peer (the
gateway-derived ones included), or the source address of the route towards
one. Only when none of these applies
does it use the default route's address (often the Wi-Fi one), with a
warning at startup; `locus init` prints the address it would announce.
Peers do not trust the announced host (they take the address the packet
came from), so it mainly matters for what `locus status` shows.
`tombstone_age` (default 30 days) is the minimum age of a deletion record
before it is forgotten; it is also kept until every peer this one has ever
synchronized that root with has completed a clean round since the deletion,
so a peer that was away longer cannot bring the file back.
`audit` bounds the local audit log in `state.db`: every `interval`, refused
peer and API requests are written as one summary row per remote host and
reason (never one row per request), then rows older than `max_age` and all
but the newest `max_entries` are deleted.
`update` controls auto-update from CI-signed releases; see [Keeping peers up
to date](#keeping-peers-up-to-date).
`embedding.device` is `auto`, `cpu` or `gpu`; the default embedder is a
deterministic dependency-free hashed bag-of-words model (`locus-hashing-v1`),
and a neural runtime can replace it by implementing one interface.

**How peers pair roots.** Each root has a name, and two peers synchronize a
root when its name matches; the absolute paths never need to agree. A root
given as a bare path string is named by its last path component, so
`/home/you/Documents` is `Documents` on every machine. Give a root an explicit
`name` (or `locus init --root work=/home/you/Sync/Work`) to pair folders that
are called differently on different machines, for instance `~/Documents` on a
laptop with `~/Sync/Docs` on a server, both named `docs`. Renaming the folder
then no longer breaks pairing. Names must be unique on a peer, so two roots
with the same folder name (`~/Work/Notes`, `~/Personal/Notes`) need distinct
explicit names; `locus status` shows each root's name next to its path. A peer
whose root has no counterpart of the same name elsewhere simply has nothing to
exchange for it.

**Hidden paths are not synchronized.** A path with any component starting
with `.` is skipped: `.git/**`, `notes/.obsidian/workspace.json`, `.DS_Store`
and the like are never hashed, recorded, indexed or sent to a peer, Locus
does not even walk into hidden directories, and a hidden path a peer sends is
refused (logged once per peer and root as `refusing hidden paths from peer`).
There is no pattern-based ignore list; hiding is the only exclusion.

**Keep git repositories out of roots, or accept that only the working tree
syncs.** A repository is a database of thousands of small, interdependent
files: replicating `.git` one file at a time, with no grouping, carries lock
files across and turns concurrent commits into conflict siblings inside
`.git/objects` and `.git/refs`, which corrupts the repository. Use a git
remote to move commits between machines. A repository inside a root has its
working tree synced like any other files and its `.git` left alone on each
machine.

`"sync_hidden": true` on a root (object form only; `--root` cannot set it)
treats its hidden paths as ordinary documents. Both peers must set it for a
root's hidden files to travel; a peer without it advertises so and is not
sent them. You are on your own with it: nothing makes a `.git` directory, an
editor's state or any other multi-file store safe to replicate file by file.

Environment: `LOCUS_CONFIG_DIR`, `LOCUS_DATA_DIR`, `LOCUS_LOG_LEVEL=info`
(the default logs warnings only), `LOCUS_WG_INTERFACE`.

On disk:

```
~/.config/locus/config.json       configuration
~/.local/share/locus/             data directory
  identity.json                   this peer's ed25519 identity
  api_token                       generated API bearer token
  daemon.lock                     held by a running `locus serve`
  partials/                       staged transfers; interrupted pushes kept 24 h for resuming
  state.db                        documents, versions, tombstones, conflicts, audit log
  index.db                        chunks, embeddings, indexing queue (disposable)
  update.json                     last update check, builds rolled back
  update-health.json              present while a newly installed build has not reached serving
```

Next to the binary, an update leaves `locus.previous` (the binary it
replaced, for `locus update --rollback`) and, only while downloading,
`locus.new`.

## Ports

| Port | Transport | Purpose |
| --- | --- | --- |
| 8909 | TCP | peer-to-peer synchronization (`peer.port`) |
| 8911 | UDP | discovery probes (`discovery.port`) |
| 8910 | TCP | HTTP API and browser UI (`api.port`, loopback by default) |

## Running Locus as a service

`contrib/` holds service definitions for the three usual setups:

| File | For |
| --- | --- |
| `contrib/systemd/locus-user.service` | Linux desktop or laptop, runs as you (systemd user unit) |
| `contrib/launchd/io.github.teulaert.locus.plist` | macOS, runs as you (LaunchAgent) |
| `contrib/systemd/locus.service` | always-on server peer or rendezvous peer, runs as a dedicated `locus` user (see below) |
| `contrib/config.system.json` | configuration template for the server peer |

**Linux, per user:**

```bash
make install
locus init --root ~/Documents
install -Dm644 contrib/systemd/locus-user.service ~/.config/systemd/user/locus.service
systemctl --user daemon-reload && systemctl --user enable --now locus
loginctl enable-linger "$USER"   # optional: keep running while logged out
```

**macOS:** launchd expands neither `~` nor `$HOME`, so the plist names
`/Users/USER`; substitute your account name. It runs
`~/.local/bin/locus serve` at login, restarts it when it exits (no more often
than every 10 seconds) and logs to `~/Library/Logs/locus/`.

```bash
make install
locus init --root ~/Documents
mkdir -p ~/Library/Logs/locus
sed "s/USER/$(whoami)/g" contrib/launchd/io.github.teulaert.locus.plist \
  > ~/Library/LaunchAgents/io.github.teulaert.locus.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/io.github.teulaert.locus.plist
```

`launchctl bootout gui/$(id -u)/io.github.teulaert.locus` stops it.

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
owns, so that it can [update itself](#keeping-peers-up-to-date);
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

## CLI

| Command | Description |
| --- | --- |
| `locus init` | write a configuration file with sync roots (`--root`, `--peer-port`, `--discovery-peer`, `--force`; `--system`, `--user` for a server peer); prints the address peers will be told to dial |
| `locus serve` | run the peer daemon |
| `locus status` | peer, roots, index, embedding device, watch mode, peers, conflicts |
| `locus peers` | observed peers and their presence state |
| `locus sync now` / `locus sync status` | force a round, or read sync state and per-peer progress |
| `locus search <query>` | hybrid search (`--json`, `--context`, `--path`, `--type`, `--limit`, `--budget`) |
| `locus index status` / `locus index rebuild` | derived index state, or rebuild it |
| `locus conflicts` (`show <id>`, `resolve <id>`) | list and resolve conflicts |
| `locus update` (`--check`, `--rollback`) | install the newest CI-signed build and restart the daemon into it, only report, or return to the previous binary |
| `locus skill` (`--section <heading>`) | print the embedded agent documentation |
| `locus version` | version |

Global flags: `--config`, `--data-dir` and `--timeout` before the command;
`--json` and `--quiet` before or after it.
Exit codes: `0` success · `1` error · `2` usage · `3` no results ·
`4` configuration problem · `5` unresolved conflicts.

## Development

```bash
make check        # make lint + unit + acceptance
make lint         # vet + gofmt + documentation drift + go mod tidy + staticcheck
make test         # tests only
make test-short   # skips the slow 10k-document scenario
make skill        # regenerate SKILL.md from the embedded copy
make dist         # cross-compile the release binaries into dist/
```

The acceptance suite in `tests/` runs real peers against real filesystems and
real HTTP — no mocks — and is described in [TESTING.md](TESTING.md).
`LOCUS_DESIGN.md` is the authoritative specification; code comments say *why*,
the design document says *what*.

| Package | Responsibility |
| --- | --- |
| `cmd/locus` | the CLI: flags, output and exit codes |
| `internal/app` | wires presence, sync, indexing and API together; runs the loops |
| `internal/agent` | the agent documentation (`SKILL.md`) embedded in the binary |
| `internal/config` | configuration file, defaults, resolution |
| `internal/memlimit` | the daemon's soft Go memory limit (`memory_limit`, `GOMEMLIMIT`) |
| `internal/identity` | per-install ed25519 identity and request signing |
| `internal/discovery` | WireGuard-derived discovery, probing and peer exchange |
| `internal/peer` | presence table: state, reachability, expiry |
| `internal/sync` | manifest planning, transfer protocol, apply and conflicts |
| `internal/reconcile` | filesystem ↔ state reconciliation, tombstone pruning |
| `internal/fswatch` | inventory scans, hashing, temporary-file sweeping, directory watching (inotify, kqueue-dirs, poll fallback) |
| `internal/store` | `state.db` and `index.db` schemas and queries |
| `internal/vvector` | version vectors |
| `internal/extract` · `internal/chunk` · `internal/embed` | text extraction, chunking, vectors |
| `internal/jobs` · `internal/pipeline` | durable work queue and index pipeline |
| `internal/search` | hybrid retrieval and reranking |
| `internal/pathguard` | strict containment of paths inside roots |
| `internal/api` · `internal/web` | local HTTP API and browser UI |
| `internal/update` | release manifests and their verification, checking GitHub, swap, re-exec, rollback |
| `internal/release` · `cmd/locus-release` | CI side of releases: key generation, manifest, signing (never linked into `locus`) |
| `internal/testutil/synthpdf` | large synthetic PDFs for tests |

## Troubleshooting

* **A peer is `online` but nothing syncs.** Reachability is one-directional:
  it answers discovery probes but cannot be dialed for transfer. Such a peer is
  reported with `reachable: false`, a `failures` count and the `last_error` in
  `locus peers` / `locus status`, the daemon logs `peer unreachable for
  transfer`, and retries back off exponentially up to five minutes. Usually the
  sync port is not allowed inbound on that machine. The note says when the
  last attempt failed and when the next one is due, e.g. `[transfer failed
  32m ago (connection refused); next attempt in 12s]`. Each peer is
  synchronized on its own, so a long transfer to one peer does not hold back
  retries with another.
* **A large transfer seems stuck.** `locus sync status` lists every peer a
  round is working with: per root the actions planned and done, files and
  bytes pushed and pulled, the file moving now, the rate over the last ten
  seconds and an ETA. `locus status` summarises it as e.g. `sync    pushing
  1.2 GB / 5.6 GB to MacBook (921.6 KB/s, ~1h20m)`; content the peer already
  holds is not sent and is named instead (`pushing to hermes: 0 B sent,
  4.8 GB already held there (1150/1152 files)`). `stalled` means neither a
  byte moved nor a file was done for ten seconds.
* **Changes on a Mac show up only after a while.** `locus status` prints how
  the roots are watched, e.g. `watch   kqueue-dirs, 412 directories`. On
  macOS (and the BSDs) only directories are watched, so files created,
  deleted or renamed (including editors' save-by-rename) are seen at once,
  but a file rewritten in place is found by the periodic reconciliation,
  within `reconcile_interval` (30s by default). On Linux, inotify reports
  in-place writes too. `watch   poll (...)` means the daemon gave up
  watching, with the reason: the system refused more watches or
  descriptors (on Linux raise `fs.inotify.max_user_watches`), or the
  directories would need more than half the process's descriptor limit. It
  keeps working on the periodic reconciliation alone; restart it after
  raising the limit to watch again.
* **A peer is `incompatible`.** The two builds speak different sync
  protocols; `locus peers` shows the versions and the reason. Nothing is
  exchanged with it (and it is not retried) until one side is upgraded and
  its announcement changes.
* **One root syncs with a peer, another does not.** Roots are matched by
  name, and a peer only offers the roots it has. A root the peer does not have
  is logged once (`peer does not share root; skipping it`) and skipped; a peer
  that refuses a root (for example 403 before it has discovered this device)
  is logged as `peer refused sync of root`. Neither counts as unreachable or
  delays the other roots.
* **Conflicts appeared after a meeting.** Both versions are on disk; the
  conflict records (`locus conflicts show <id>`) name the hash and author of
  the version on the normal path and of the sibling. Once you have merged the
  content yourself, delete the sibling and close the record with
  `locus conflicts resolve <id>` (resolving does not touch either file).
* **`index.documents` is 0 although files exist.** Indexing has not run yet;
  start `locus serve`, or run `locus index rebuild`.
* **Deleted files came back.** A deletion record (tombstone) is only forgotten
  once it is older than `tombstone_age` *and* every peer this one has synced
  that root with has completed a round since the deletion, so a device that was
  offline for months still receives the delete. Files can still come back from a
  device this peer has never synced with (or under a new identity) that holds an
  old copy. Conversely, a peer that never returns keeps tombstones created
  after its last sync from being pruned; they are small metadata records.
* **A hidden folder (`.notes/`) never arrives.** Hidden paths are not
  synchronized by default. The receiving peer logs `refusing hidden paths
  from peer` once per peer and root; rename the folder, or set
  `"sync_hidden": true` on that root on every peer.
* **`locus serve` fails with `open daemon lock: permission denied`.** The
  data directory holds files another user created, typically root running a
  CLI command against a service's data directory with an older build. The
  error names the file's owner and the user locus runs as; `chown -R` the data
  directory back to the service user. Current builds never create
  `daemon.lock` from a CLI command and refuse (exit 4) to open a data
  directory owned by another user; run the CLI as that user
  (`runuser -u locus -- locus ...`).
* **systemd reports a memory peak of several GB.** The unit's "memory peak"
  counts the page cache of every file the daemon wrote and read, so a bulk
  pull of several GB shows a peak of several GB; the kernel reclaims that
  cache under pressure. The daemon itself is the `anon` line of
  `/sys/fs/cgroup/system.slice/locus.service/memory.stat` (a few hundred MB
  at most while indexing, 50 MB or so idle). Its heap is capped softly by
  `memory_limit`, and `/debug/pprof/heap` on the local API shows what it
  holds. See "Memory" in `LOCUS_DESIGN.md`.
* **Upgrades change embeddings.** Old embeddings are marked stale and
  regenerated in the background; synchronization is never blocked by indexing.

## Keeping peers up to date

Releases are built, signed and published by CI, never on a developer's
machine, and peers update themselves from them: run `locus update` on a
peer, or set `update.auto` and let the daemons do it.

**Where releases come from.** Every push to `main` that passes the checks
becomes a GitHub prerelease tagged `build-<sha7>` (channel `main`); every
tag `v*` becomes a release (channel `stable`). Each carries the binaries
`locus-linux-amd64`, `locus-linux-arm64`, `locus-darwin-arm64` and
`locus-darwin-amd64`, `manifest.json` (version, commit, commit time,
channel, and each binary's size and SHA-256), `manifest.sig` and
`SHA256SUMS`. Only the newest 20 `build-*` releases are kept.

**Trust.** `manifest.sig` is an ed25519 signature over the exact bytes of
`manifest.json`, made with the release key. Its public half is compiled
into every binary (`internal/update/release_pubkey.go`); its private half
exists only on the self-hosted CI runner that signs releases (or as a
repository secret), never on a peer or a developer machine. A peer installs
a binary only when the manifest verifies with the key compiled into the
binary it runs *and* the download has the manifest's size and SHA-256, and
only when its commit is newer than the running build's: an old release
served again is never installed. Peers never send each other binaries. A
build with no release key compiled in (a development build) cannot update:
`locus update` exits 4 with "no release key compiled in", and its daemon
never checks.

**Checking and updating by hand.**

```bash
locus update --check      # available: build-1a2b3c4 (version 1a2b3c4); running e8c67fb, channel main
locus update              # download, verify, swap, and restart the daemon into it
locus update --rollback   # back to the previous binary (locus.previous)
locus status              # ... update  available: build-1a2b3c4 (auto off; run: locus update)
```

With a daemon running on the data directory, `locus update` asks it
(`POST /api/v1/update`) to do the whole thing, and it restarts into the new
binary; with none, the binary is swapped and the next start runs it. On a
system install run it as the service user, like every command:
`sudo runuser -u locus -- locus --config /etc/locus/config.json update`
(or the `/usr/local/sbin/locus` wrapper).

**Automatic updates** are off by default. Turn them on per peer:

```json
"update": { "auto": true, "channel": "main" }
```

and restart the daemon. It checks every `update.interval` (default `30m`;
the GitHub API allows 60 unauthenticated requests an hour, and a check that
finds nothing new is a conditional request GitHub does not count). A new
build is installed after a random delay of up to ten minutes, so peers do
not all restart at once; a rendezvous peer waits one more interval, so the
hub is never first. When GitHub rate-limits a check, the daemon waits for
the time GitHub names. With `auto` off the daemon still checks and `locus
status` says when a build is available. `channel` is `main` (every build of
main) or `stable` (`v*` tags only); `url` replaces the GitHub API base for a
mirror.

**What an update does.** The new binary is downloaded next to the running
one as `locus.new`, verified, and swapped in: the running binary is kept as
`locus.previous` (replacing an older one) and the new one is renamed over
it, so the path never points at a partial or missing binary. The daemon
then shuts down exactly as on SIGTERM and re-executes the new binary with
the same arguments and environment in the same process: systemd and launchd
see the same pid carry on and do not count a restart. (Where re-executing
fails, or on Windows, it exits 0 and the service manager starts the new
binary; the user unit uses `Restart=always` for this, and the launchd
plist's `KeepAlive` restarts it anyway.) A new binary that fails to reach
serving three starts in a row is rolled back on the next start, with a
warning in the log; the daemon then does not install that build again on
its own, though the next newer build, or `locus update` by hand, will.

**Where the binary must live.** The daemon replaces the file it runs, so it
must be writable by the user the service runs as. A user install
(`~/.local/bin/locus` with the systemd user unit or the launchd plist)
already is. A system install runs `/var/lib/locus/bin/locus`, owned by the
`locus` user, with `/usr/local/bin/locus` a symlink to it for the CLI
(`make install-system` and `locus init --system` set this up, and
`contrib/systemd/locus.service` runs it). An existing system install that
runs `/usr/local/bin/locus` moves over with:

```bash
sudo install -D -o locus -g locus -m 0755 /usr/local/bin/locus /var/lib/locus/bin/locus
sudo ln -sfn /var/lib/locus/bin/locus /usr/local/bin/locus
sudo install -m 0644 contrib/systemd/locus.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart locus
```

(Keep any drop-ins from `systemctl edit locus`; only `ExecStart` changes.
A `/usr/local/sbin/locus` runuser wrapper keeps working unchanged.)

**Maintainers: the release pipeline.** `.github/workflows/release.yml` runs
on pushes to `main` and on `v*` tags, never on pull requests. It first runs
the checks of `ci.yml` (on GitHub-hosted runners) and only then the release
job on the self-hosted runner with the labels `self-hosted, linux, locus`:
`make dist`, `locus-release manifest`, `locus-release sign`, `locus-release
verify` (with the key compiled into the binaries) and `gh release create`.
The signing key is read from the file `$LOCUS_RELEASE_KEY_FILE`
(`/etc/locus-release/key` on the runner: the base64 private key, mode 600,
owned by the runner's user), or else from the repository secret
`LOCUS_RELEASE_KEY`. To set up or rotate the key:

```bash
make release-keygen       # public <hex>, private <base64>
# put the private line in /etc/locus-release/key on the runner (chmod 600),
# set ReleasePublicKey in internal/update/release_pubkey.go to the public hex,
# commit and push: that build is the first one peers can verify.
```

Peers running a build with an older key cannot verify releases signed with a
new one; install one build by hand on each after a rotation. The same tool
checks a release locally: `make dist release-manifest`, then
`make release-sign` and `make release-verify`.

## Upgrading

* **Hidden files leave the index and stop syncing.** Builds before hidden
  paths were skipped inventoried, indexed and synchronized everything under a
  root, `.git` included. After upgrading, the first reconciliation pass
  retires the records of hidden paths as if the files had been deleted, so
  they disappear from search results and `locus status` counts. The files
  themselves are not touched, and the retirement is never sent to peers, so
  no peer deletes its copies either. Set `"sync_hidden": true` on a root to
  keep the old behaviour for it.
  Until every peer is upgraded, older peers keep offering their hidden files
  each round and upgraded peers refuse them, logging it once per peer.
* **Peer exchange needs upgraded peers.** Older builds neither send nor read
  peer lists (a responder only includes one for a prober that announces it
  understands it), and they do not announce their discovery port, so they
  are not learned from inbound probes either. Keep listing them in
  `discovery.peers` until they are upgraded; everything else works as before.

## Security assumptions

* The transport is the WireGuard link, and on top of it peers authenticate
  each other: each install has an ed25519 identity, every sync request carries
  a signature over method, path and query, timestamp and body hash (for a
  put, the declared content hash, so it is checked before the body is read),
  and `authorized_peers` can pin which peers are discovered and served.
* Files are not encrypted at rest; on-device confidentiality is the operating
  system's job.
* Paths are strictly validated against configured roots — `..`, absolute paths
  and symlink escapes are rejected for both sync and API operations.
* Uploads are size-capped, hash-verified and installed atomically.
* Pushes of files of 1 MiB or more are resumable: an interrupted put leaves a
  partial object in `<data_dir>/partials` (keyed by content hash, never inside
  a root), the sender asks `GET /v1/put/status?hash=` how much arrived and
  continues with `offset=N` in the signed put query. The size cap applies to
  the whole object across continuations, the complete object is re-hashed
  before it is installed (a mismatch discards the partial), and partials
  untouched for 24 hours are removed. Peers without the status endpoint get a
  plain full put, as before. See `LOCUS_DESIGN.md`, "Resumable puts".
* Received content (pushed to this peer or pulled by it) is staged in the
  data directory and verified before the file's lock is taken, so a peer that
  stalls mid-transfer cannot block other transfers of the file. A transfer
  that moves no bytes for 60 seconds is abandoned on either side.
* Binaries are installed only from releases signed with the release key
  compiled into the running binary, which only CI holds, and only when they
  match the signed manifest; see [Keeping peers up to
  date](#keeping-peers-up-to-date).
* The API is token-authenticated and loopback-only unless `api.addr` says
  otherwise; the token is generated on first start and stored in the data
  directory.

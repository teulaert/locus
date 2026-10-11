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
  normal path and one as a `<name>.conflict-<host>-<hash8>.<ext>` sibling, and
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
locus status          # identity, roots, index, peers, conflicts
locus peers           # presence of the other peers
```

Peers find each other over WireGuard: they read the peer list of every `wg*`
interface (or only `LOCUS_WG_INTERFACE`, using the `wg` tool) and probe each
peer's tunnel address, plus any explicit `discovery.peers` — which is what
remains where there is no WireGuard tooling. They must be able to reach each
other in **both** directions:

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
| POST | `/api/v1/sync` | reconcile and sync now, as `locus sync now --json` |
| GET | `/api/v1/index` | as `locus index status --json` |

File routes act on the first configured root unless given `root=` (or
`"root"` in a JSON body) with another root's absolute path. By default the
API is loopback-only and not reachable from other machines; to use the PWA
from a phone over WireGuard, set `api.addr` to this peer's WireGuard address.

```bash
TOKEN=$(cat ~/.local/share/locus/api_token)
curl -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:8910/api/v1/search?q=grid"
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
  "peer":      { "addr": "0.0.0.0", "port": 8909 },
  "api":       { "addr": "127.0.0.1", "port": 8910, "token": "" },
  "discovery": { "peers": [], "port": 8911, "interval": "10s" },
  "sync":              { "interval": "10s" },
  "reconcile_interval": "30s",
  "workers": 2,
  "embedding": { "device": "auto" },
  "upload_max_bytes": 1073741824,
  "tombstone_age": "720h",
  "authorized_peers": [],
  "audit": { "max_entries": 10000, "max_age": "2160h", "interval": "1m" }
}
```

Only `roots` is required; roots may not be `/`, repeat or lie inside one
another. A missing or zero value takes the default shown; ports must be
1–65535, and intervals, `tombstone_age`, `audit.max_age` and
`audit.max_entries` may not be negative (`workers` and `upload_max_bytes`
below 1 also mean the default). `discovery.peers` supplements the
WireGuard-derived peer list with explicit `host:port` entries (a bare host,
like every WireGuard-derived peer, is probed on `discovery.port`), and
`authorized_peers` restricts which peer IDs this peer discovers and
synchronizes with (empty accepts any peer it has discovered); it does not
apply to the HTTP API, which only checks its token.
`tombstone_age` (default 30 days) is the minimum age of a deletion record
before it is forgotten; it is also kept until every peer this one has ever
synchronized that root with has completed a clean round since the deletion,
so a peer that was away longer cannot bring the file back.
`audit` bounds the local audit log in `state.db`: every `interval`, refused
peer and API requests are written as one summary row per remote host and
reason (never one row per request), then rows older than `max_age` and all
but the newest `max_entries` are deleted.
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
  state.db                        documents, versions, tombstones, conflicts, audit log
  index.db                        chunks, embeddings, indexing queue (disposable)
```

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
| `contrib/systemd/locus.service` | always-on server peer, runs as a dedicated `locus` user (see below) |
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

**2. Install the binary, unit and a service user.**

```bash
make build                       # as yourself
sudo make install-system         # /usr/local/bin/locus, /etc/systemd/system/locus.service,
                                 # /etc/locus/config.json.example
sudo useradd --system --home-dir /var/lib/locus --shell /usr/sbin/nologin locus
```

`make install-system` does not build (so `sudo` needs no Go toolchain) and
never replaces an existing `config.json`. `DESTDIR`, `SYSTEM_BIN_DIR`,
`SYSTEMD_UNIT_DIR` and `SYSTEM_CONFIG_DIR` override where things go.

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
`discovery.peers` lists the clients' tunnel addresses: the hardened unit has
no `CAP_NET_ADMIN`, so the `wg` tool cannot list the WireGuard peers for it.
`authorized_peers` pins which peer ids may sync with it.

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

If an older build already left root-owned files behind, give them back:
`sudo chown -R locus:locus /backup/locus` (the error names the owner).

## CLI

| Command | Description |
| --- | --- |
| `locus init` | write a configuration file with sync roots (`--root`, `--peer-port`, `--force`; `--system`, `--user` for a server peer) |
| `locus serve` | run the peer daemon |
| `locus status` | peer, roots, index, embedding device, peers, conflicts |
| `locus peers` | observed peers and their presence state |
| `locus sync now` / `locus sync status` | force a round, or read sync state |
| `locus search <query>` | hybrid search (`--json`, `--context`, `--path`, `--type`, `--limit`, `--budget`) |
| `locus index status` / `locus index rebuild` | derived index state, or rebuild it |
| `locus conflicts` (`show <id>`, `resolve <id>`) | list and resolve conflicts |
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
| `internal/identity` | per-install ed25519 identity and request signing |
| `internal/discovery` | WireGuard-derived discovery and probing |
| `internal/peer` | presence table: state, reachability, expiry |
| `internal/sync` | manifest planning, transfer protocol, apply and conflicts |
| `internal/reconcile` | filesystem ↔ state reconciliation, tombstone pruning |
| `internal/fswatch` | inventory scans, hashing, temporary-file sweeping |
| `internal/store` | `state.db` and `index.db` schemas and queries |
| `internal/vvector` | version vectors |
| `internal/extract` · `internal/chunk` · `internal/embed` | text extraction, chunking, vectors |
| `internal/jobs` · `internal/pipeline` | durable work queue and index pipeline |
| `internal/search` | hybrid retrieval and reranking |
| `internal/pathguard` | strict containment of paths inside roots |
| `internal/api` · `internal/web` | local HTTP API and browser UI |

## Troubleshooting

* **A peer is `online` but nothing syncs.** Reachability is one-directional:
  it answers discovery probes but cannot be dialed for transfer. Such a peer is
  reported with `reachable: false`, a `failures` count and the `last_error` in
  `locus peers` / `locus status`, the daemon logs `peer unreachable for
  transfer`, and retries back off exponentially up to five minutes. Usually the
  sync port is not allowed inbound on that machine.
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
* **Upgrades change embeddings.** Old embeddings are marked stale and
  regenerated in the background; synchronization is never blocked by indexing.

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
* The API is token-authenticated and loopback-only unless `api.addr` says
  otherwise; the token is generated on first start and stored in the data
  directory.

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
  tombstones, conflicts — losing it means re-adoption) and `index.db` (chunks
  and embeddings — derived, disposable, rebuildable with `locus index rebuild`).
* **Peers are equal.** There is no primary, no coordinator and no relay. Any
  number of peers can be up or down in any combination; they converge when they
  meet.
* **Causality, not clocks.** Versions are version vectors. Two peers editing
  the same file offline produce a *conflict*: both versions are kept, one on the
  normal path and one as a `<name>.conflict-<host>-<hash8>.<ext>` sibling, and
  the winner is chosen deterministically from content hashes so every peer
  agrees: the greater content hash takes the normal path, and the writer's
  peer ID only breaks a tie between identical contents.
* **Transfers are atomic.** Content is streamed to a `.locus-tmp-*` file,
  hash-verified, then renamed into place. A killed peer never leaves a partial
  document, and leftovers from a crash are swept by later passes.
* **Local-first network.** Discovery and sync assume a trusted WireGuard
  network. There is no NAT traversal, relay or public endpoint.

## Install

Go 1.25 or newer is the only requirement; everything else is pure Go
(`CGO_ENABLED=0`), including the SQLite driver and the embedder.

```bash
make install      # → ~/.local/bin/locus
make build        # → ./bin/locus
```

`make install` builds a version-stamped binary and copies it into
`$HOME/.local/bin`, printing a note if that directory is not on your `PATH`.
Choose somewhere else with `make install INSTALL_DIR=~/bin`.

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

Peers find each other over WireGuard: they read the peer list of every WireGuard
interface (or `LOCUS_WG_INTERFACE`, using the `wg` tool) and probe each address,
falling back to `discovery.peers` where there is no WireGuard tooling. They must
be able to reach each other in **both** directions:

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
locus search "invoice" --path Finance/ --type pdf
```

Exit code 3 means *no evidence found*, not failure. `locus skill` prints the
agent-facing documentation embedded in the binary.

## HTTP API and browser UI

`locus serve` listens on `127.0.0.1:8910` and serves a browser UI at `/`. All
API routes require the bearer token: the configured `api.token`, or the
auto-generated one in `<data_dir>/api_token`.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/healthz` | liveness, no auth |
| GET | `/api/v1/status` | same shape as `locus status --json` |
| GET | `/api/v1/peers` | presence table |
| GET | `/api/v1/search?q=` | hybrid search |
| GET | `/api/v1/files?path=` | list a directory |
| GET | `/api/v1/files/content?path=` | download a file |
| POST | `/api/v1/files` | upload (`?path=`, body is the content) |
| POST | `/api/v1/files/move` | rename/move |
| DELETE | `/api/v1/files?path=` | delete |
| POST | `/api/v1/directories` | create a directory |
| GET | `/api/v1/conflicts` | unresolved conflicts |
| POST | `/api/v1/conflicts/resolve` | accept the normal-path version |
| POST | `/api/v1/sync` | reconcile and sync now |
| GET | `/api/v1/index` | derived index state |

The API binds to loopback: it is not reachable from other machines, and
cross-host file operations are refused.

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
    { "path": "/home/you/Sync/Work", "name": "work" }
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
  "authorized_peers": []
}
```

Only `roots` is required; roots may not repeat or lie inside one another, and
intervals, `workers` and `upload_max_bytes` must be positive. `discovery.peers` supplements the WireGuard-derived
peer list with explicit `host:port` entries, and `authorized_peers` restricts
which peer IDs may connect (empty accepts any peer this one has discovered).
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

Environment: `LOCUS_CONFIG_DIR`, `LOCUS_DATA_DIR`, `LOCUS_LOG_LEVEL`,
`LOCUS_WG_INTERFACE`.

On disk:

```
~/.config/locus/config.json       configuration
~/.local/share/locus/             data directory
  identity.json                   this peer's ed25519 identity
  api_token                       generated API bearer token
  state.db                        documents, versions, tombstones, conflicts
  index.db                        chunks and embeddings (disposable)
```

## Ports

| Port | Transport | Purpose |
| --- | --- | --- |
| 8909 | TCP | peer-to-peer synchronization |
| 8911 | UDP | discovery probes |
| 8910 | TCP | local HTTP API and browser UI (loopback only) |

## CLI

| Command | Description |
| --- | --- |
| `locus init` | write a configuration file with sync roots |
| `locus serve` | run the peer daemon |
| `locus status` | peer, roots, index, embedding device, peers, conflicts |
| `locus peers` | observed peers and their presence state |
| `locus sync now` / `locus sync status` | force a round, or read sync state |
| `locus search <query>` | hybrid search (`--json`, `--context`, `--path`, `--type`, `--limit`) |
| `locus index status` / `locus index rebuild` | derived index state, or rebuild it |
| `locus conflicts` (`show`, `resolve`) | list and resolve conflicts |
| `locus skill` | print the embedded agent documentation |
| `locus version` | version |

Global flags: `--config`, `--data-dir`, `--json`, `--quiet`, `--timeout`.
Exit codes: `0` success · `1` error · `2` usage · `3` no results ·
`4` configuration problem · `5` unresolved conflicts.

## Development

```bash
make check        # vet + gofmt + documentation drift + unit + acceptance
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
| `internal/app` | wires presence, sync, indexing and API together; runs the loops |
| `internal/config` | configuration file, defaults, resolution |
| `internal/identity` | per-install ed25519 identity and request signing |
| `internal/discovery` | WireGuard-derived discovery and probing |
| `internal/peer` | presence table: state, reachability, expiry |
| `internal/sync` | manifest planning, transfer protocol, apply and conflicts |
| `internal/reconcile` | filesystem ↔ state reconciliation, tombstone pruning |
| `internal/fswatch` | inventory scans, hashing, temporary-file sweeping |
| `internal/store` | `state.db` and `index.db` schemas and queries |
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
* **Conflicts appeared after a meeting.** Both versions are on disk; the
  conflict records (`locus conflicts show <id>`) name the local and the incoming
  hash and author. Resolve with `locus conflicts resolve <id>` once you have
  merged the content yourself.
* **`index.documents` is 0 although files exist.** Indexing has not run yet;
  start `locus serve`, or run `locus index rebuild`.
* **Deleted files came back.** They only come back if no peer has the tombstone;
  a device that was offline longer than the tombstone age (30 days) can re-adopt
  its own old files.
* **Upgrades change embeddings.** Old embeddings are marked stale and
  regenerated in the background; synchronization is never blocked by indexing.

## Security assumptions

* The network is assumed hostile, but *peer* authentication is: each install
  has an ed25519 identity, every sync request carries a signature over method,
  path, timestamp and body hash, and `authorized_peers` can pin who may connect.
* Files are not encrypted at rest; on-device confidentiality is the operating
  system's job. Transport relies on the underlying WireGuard link.
* Paths are strictly validated against configured roots — `..`, absolute paths
  and symlink escapes are rejected for both sync and API operations.
* Uploads are size-capped, hash-verified and installed atomically.
* The API is loopback-only and token-authenticated; the token is generated on
  first start and stored in the data directory.

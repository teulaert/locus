# Locus

[![ci](https://github.com/teulaert/locus/actions/workflows/ci.yml/badge.svg)](https://github.com/teulaert/locus/actions/workflows/ci.yml)

Locus keeps plain files on your own filesystems in sync between your own
machines, and makes them searchable — offline, without a server, and without a
database you have to trust with your documents.

It is a single static Go binary. `locus serve` runs the peer: it watches your
sync roots, replicates changes to the other peers it can see, extracts and
indexes text, serves hybrid (lexical + semantic) search, and exposes a local
HTTP API and browser UI. Your documents stay ordinary files in ordinary
directories; nothing about Locus is on their path to being readable.

## Why Locus

* **Files are canonical.** The filesystem is the source of truth. The two
  SQLite databases hold only metadata: `state.db` (documents, version vectors,
  tombstones, conflicts; losing it means re-adoption) and `index.db` (chunks,
  embeddings, the indexing queue; disposable, `locus index rebuild`).
* **Peers are equal.** No primary, no coordinator and no relay. Any number of
  peers can be up or down in any combination; they converge when they meet.
* **Causality, not clocks.** Versions are version vectors. Concurrent offline
  edits keep both versions, one as a `<name>.conflict-<peer8>-<hash8>.<ext>`
  sibling; the greater content hash takes the normal path on every peer (the
  writer's peer ID only breaks a tie between identical contents), and an edit
  concurrent with a delete always survives
  ([§7](LOCUS_DESIGN.md#7-versioning-and-conflicts)).
* **Transfers are atomic.** Content is streamed to a `.locus-tmp-*` file,
  hash-verified, fsynced and renamed into place (the directory fsynced)
  before its hash is recorded; leftovers from a crash are swept later.
* **A trusted WireGuard network.** Peers find each other over the tunnel with
  nothing configured and authenticate every request with ed25519 identities.
  There is no NAT traversal, relay or public endpoint.
* **Hidden paths stay local.** Any name starting with `.` (`.git`,
  `.obsidian`, `.DS_Store`, `.venv`, ...) is per-machine state: not
  inventoried, indexed or synchronized, and refused when a peer sends it,
  unless a root sets `sync_hidden` (see [Configuration](docs/configuration.md)).
* **Hybrid search.** Lexical and semantic retrieval over PDFs, DOCX, Markdown,
  HTML and plain text, with compact output for an LLM prompt.
* **Self-updating.** Peers install CI-signed releases and roll back a build
  that fails to start.

## Install

Go 1.25 or newer is the only requirement; everything else is pure Go
(`CGO_ENABLED=0`), including the SQLite driver and the embedder.

```bash
go install github.com/teulaert/locus/cmd/locus@latest   # → $(go env GOPATH)/bin/locus
```

or from a checkout:

```bash
make install      # → ~/.local/bin/locus
make build        # → ./bin/locus
```

`make install` builds a version-stamped binary and copies it into
`$HOME/.local/bin`, printing a note if that directory is not on your `PATH`.
Choose somewhere else with `make install INSTALL_DIR=~/bin`. A binary from
`go install` carries no version stamp and reports a development version;
`locus update` replaces it with the newest CI-signed build (see
[Updates](docs/updates.md)). Prebuilt binaries for Linux and macOS (amd64 and
arm64) are attached to every [GitHub
release](https://github.com/teulaert/locus/releases).

To run Locus as a service see [docs/service.md](docs/service.md), and for a
server see [docs/server-peer.md](docs/server-peer.md).

## Quick start

On each machine, point Locus at the directories you want replicated:

```bash
locus init --root ~/Documents --root ~/Notes
locus serve
```

Machines pair roots by name, which defaults to the folder name, so
`~/Documents` here syncs with `Documents` there. Use `--root docs=~/Sync/Docs`
to name a root explicitly when the folders are called differently (see
[Configuration](docs/configuration.md)).

`init` writes `~/.config/locus/config.json` (it will not replace an existing
one unless given `--force`); `serve` starts discovery, sync, indexing and the
API. Check what it sees:

```bash
locus status          # identity, roots, index, watch mode, peers, conflicts
locus peers           # presence of the other peers
```

Peers find each other over WireGuard with nothing configured: each probes one
address per route through a WireGuard interface (the hub, on a hub-and-spoke
network), the WireGuard peers `wg` can list, and every peer it has learned
about through peer exchange. On a hub-and-spoke tunnel, run a discovery-only
**rendezvous peer** on the hub and every client needs only
`locus init --root ~/Documents`; elsewhere, list an always-on peer with
`--discovery-peer <host>`. [Server peers and
discovery](docs/server-peer.md) has the details and the setup for both.

Peers must be able to reach each other in **both** directions:

```bash
sudo ufw allow in on wg0 to any port 8909 proto tcp  # sync
sudo ufw allow in on wg0 to any port 8911 proto udp  # discovery
```

Edits made while a peer is offline are exchanged when it returns; there is
nothing to click.

## Commands

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

## Documentation

* [Configuration](docs/configuration.md): the configuration file, how roots
  pair, hidden paths, environment, files on disk and ports.
* [Server peers and discovery](docs/server-peer.md): how peers find each
  other, an always-on server peer, and a rendezvous peer on a hub-and-spoke
  tunnel.
* [Running Locus as a service](docs/service.md): systemd user and system
  units, and launchd on macOS.
* [Search and the HTTP API](docs/api.md): searching from the CLI, the HTTP
  API, the browser UI and profiling a running daemon.
* [Updates](docs/updates.md): CI-signed releases, `locus update`, automatic
  updates, the release pipeline, and notes for upgrading.
* [Troubleshooting](docs/troubleshooting.md): peers that do not sync, stuck
  transfers, conflicts, permissions and memory.
* [Security assumptions](docs/security.md): authentication, path
  validation, transfers, releases and the API.
* [Development](docs/development.md): make targets and the package layout.
* [LOCUS_DESIGN.md](LOCUS_DESIGN.md): the authoritative specification.
* [TESTING.md](TESTING.md): the acceptance suite and how tests are written.
* `locus skill` prints the agent-facing documentation embedded in the binary
  (also in [SKILL.md](SKILL.md)), including every JSON shape.

## License

MIT; see [LICENSE](LICENSE).

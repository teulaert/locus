# Configuration

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
see [Zero-config discovery](server-peer.md#zero-config-discovery-on-a-hub-and-spoke-tunnel));
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
to date](updates.md#keeping-peers-up-to-date).
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

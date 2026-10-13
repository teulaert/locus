# Locus — agent skill

Locus is a local-first, peer-to-peer document system. Files stay ordinary
files on the local filesystem; peers replicate files to each other over an
existing WireGuard network; search indexes, extracted text, chunks and
embeddings are **local, derived and disposable**. There is no central server
and no cloud storage.

> Files are canonical. Synchronization replicates files. Search state is
> local, derived and disposable. Every computer is an equal peer.

## When to use Locus

Use Locus when you need to find or read something in the user's own
documents (or push/pull those documents between the user's machines):

* "find the invoice for …", "where did I write about …", "what are the terms
  of …"
* locating a file whose name or folder you do not know
* retrieving a *passage* (page/section) rather than a whole document
* checking whether the user's machines are in sync, or resolving conflicts

Do **not** use Locus to edit file contents; it replicates files, it does not
merge them.

## Core guidance

When looking for information in the user's documents, prefer:

```bash
locus search "<natural language query>" --context --json
```

Do not recursively open every file unless the search results are insufficient.
Use returned paths and provenance to retrieve only the necessary source
material.

## Discovering the interface

```bash
locus --help                          # command list, global flags, exit codes
locus search --help                   # search flags
locus skill                           # this document, embedded in the binary
locus skill --section "exit codes"    # one section, by the start of its heading
```

Locus needs no running daemon for CLI commands: each invocation opens the
local databases directly. `locus serve` runs the daemon (discovery, sync,
indexing, HTTP API, PWA) and is what keeps peers converged.

When a daemon is running on the same data directory (it holds
`<data_dir>/daemon.lock`), status comes from it: `locus status`,
`locus sync status` and `locus peers` ask its API (at `api.addr`/`api.port`,
with the token from the config or `<data_dir>/api_token`) for its live state,
and `locus sync now` asks it to run the round (`POST /api/v1/sync`) instead of
running a second one beside it. The JSON is the same either way. With no
daemon, or one whose API cannot be reached (a warning goes to stderr), the
CLI reports its own snapshot of the databases.

## Commands

| Command | Purpose |
| --- | --- |
| `locus init` | write a configuration file with sync roots (see below) |
| `locus status` | peer identity, roots, index, embedding device, watch mode, peers, conflicts |
| `locus peers` | observed peers and their presence state (exit 3 when none) |
| `locus sync status` | local synchronization state (the default `sync` subcommand) |
| `locus sync now` | reconcile locally and exchange changes with online peers |
| `locus search <query>` | hybrid lexical (FTS5/BM25) + semantic search |
| `locus index status` | derived index counters and queue state (the default `index` subcommand) |
| `locus index rebuild` | delete and reconstruct the search index from local files (refused while `locus serve` runs on the same data directory) |
| `locus conflicts` | list unresolved conflicts (exit 5 when there are any) |
| `locus conflicts show <id>` | one conflict, both sides identified |
| `locus conflicts resolve <id>` | keep the normal-path version and close the record |
| `locus serve` | run the peer daemon |
| `locus skill [--section <heading>]` | print this documentation, or one section of it |
| `locus version` | print version |

Global flags: `--config <path>`, `--data-dir <dir>` and `--timeout <dur>`
(default `2m`) go **before** the command; `--json` and `--quiet` may go before
or after it (`locus --json status` and `locus status --json` are the same).
Search flags may also follow the query (`locus search invoice --limit 5`).

`locus init --root <dir>` (repeatable; `--peer-port <n>` optional) creates the
configuration and the peer identity; without `--root` it uses `~/Documents`.
Usually nothing else is needed: every route through a WireGuard interface
yields one address to probe (its gateway, or the first address of its prefix:
`10.42.0.0/16` gives `10.42.0.1`, the hub), a node without a WireGuard
interface probes its private default gateway (a container on the hub reaches
the hub's rendezvous peer that way), and peers pass on the other peers they
know (peer exchange) and remember them across restarts.
`--discovery-peer <host[:port]>` (repeatable) adds a discovery target where no
WireGuard route leads to an always-on peer (a container with no WireGuard
interface, a full-mesh network); one always-on peer is enough.
`--root <name>=<dir>` gives the root an explicit name. Peers synchronize a root
when its *name* matches, not its path: an unnamed root is named by its last
path component (`~/Documents` is `Documents`), so folders with different names
on different machines pair only when both are given the same name. Names must
be unique on a peer (exit 4 otherwise). In `config.json` a root is a path
string or `{"path": "<dir>", "name": "<name>", "sync_hidden": true}` (`name`
and `sync_hidden` optional); `locus init --json` prints `config`, `data_dir`,
`peer_id`, `roots` (each root in that same shape), `rendezvous`,
`gateway_targets` (the `host:port` addresses derived from WireGuard routes
that the daemon will probe; `[]` when none, and then a `--discovery-peer` is
needed unless other peers probe this one), `advertised_addr` (the
`host:port` other peers are told to dial) and `advertised_by` (how that host
was chosen; `default route` means it is a guess that may be the Wi-Fi rather
than the WireGuard address, fixed by setting `peer.addr` or `peer.interface`). A
`--data-dir` given to `init` is saved in the configuration. `init` refuses
(exit 4) to replace an existing configuration, which holds the API token and
configured peers; pass `--force` only when the user explicitly wants to start
over.
With `--system` (`locus init --system --root <dir>`, run as root) init sets
up a server peer for a system service: it writes `/etc/locus/config.json` (or
`--config`), creates the data directory (`/var/lib/locus`, or `--data-dir`)
and the identity, hands both to the service user (`--user <name>`, default
`locus`), and prints the full peer id; `--json` adds `user` to the keys above.
It needs `--root` (or `--rendezvous`) and an existing user (exit 4 otherwise).

`locus init --rendezvous` (also with `--system`) configures a **rendezvous
peer**: `"rendezvous": true` and no roots (`--root` is a usage error, exit 2).
It is what the hub of a hub-and-spoke WireGuard network runs so that clients
need no configuration: `locus serve` answers discovery probes, learns every
peer that probes it, shares that list (kept in `state.db` across restarts) and
runs no sync, indexing or watching. `locus status`, `locus peers`, `locus
sync status` and `locus index status` work and say "rendezvous peer, no
roots"; `locus sync now`, `locus index rebuild`, `locus search` and `locus
conflicts` exit 4 with "rendezvous peer: no roots" (run them on a client).

**Run the CLI as the user that owns the data directory.** Every command except
`init --system` refuses (exit 4, "data directory ... belongs to user ...") a
data directory owned by another user, root included, because files it created
there would lock the daemon out. For a system install use
`runuser -u locus -- locus --config /etc/locus/config.json <command>`; never
work around the refusal with `sudo` or `chown`.

**Hidden paths are not synchronized.** Any path with a component starting
with `.` (`.git/**`, `.obsidian/`, `.DS_Store`, `.venv/`) is not inventoried,
indexed, searchable or sent to peers, and a peer sending one is refused. A git
repository inside a root syncs its working tree only; advise keeping
repositories out of roots and using a git remote, since replicating `.git`
file by file corrupts it. `"sync_hidden": true` on a root (in `config.json`
only, `--root` cannot set it, and needed on every peer) syncs its hidden paths
like any other file; the user is on their own with it. There is no
pattern-based ignore list.

### Search flags

| Flag | Meaning |
| --- | --- |
| `--json` | stable machine-readable results with provenance |
| `--context` | compact passages suitable for an LLM context window |
| `--path <prefix>` | restrict to a path prefix, e.g. `Finance` |
| `--type <ext>` | restrict to a file extension: `pdf`, `md`, `txt`, `docx` |
| `--limit <n>` | maximum number of documents (default 10) |
| `--budget <chars>` | `--context` character budget (default 8000) |

## JSON schemas (stable)

All `--json` output is indented JSON on stdout, with no decorative text.
Documented collections are always arrays or objects (`[]` or `{}`, never
`null`).

`locus search "q" --json` (and `GET /api/v1/search?q=q`, same shape)

```json
{
  "query": "q",
  "results": [
    {
      "root": "/home/user/Documents",
      "path": "Finance/2026/solar.pdf",
      "score": 0.91,
      "snippet": "…invoice for the solar panels…",
      "chunks": [
        { "ordinal": 3, "page": 4, "section": "Payment", "score": 0.93,
          "snippet": "…invoice for the solar panels…", "start": 1180 }
      ]
    }
  ]
}
```

* `path` is relative to `root`; join them for the absolute path.
* `chunks` are the strongest matching passages, ordered.
* `page` is present for paginated formats (PDF); `section` for headed text.
* `start` is the passage's offset in characters (Unicode code points) into
  the document's extracted text, or into the page's text when `page` is
  set. It is absent for passages indexed before Locus recorded offsets.

`locus search "q" --context --json` (and `GET /api/v1/search?q=q&context=1`,
which returns exactly the same shape)

```json
{
  "query": "q",
  "results": [ { "root": "…", "path": "Finance/2026/solar.pdf", "…": "as above" } ],
  "context": ["[Finance/2026/solar.pdf#page=4]\n…invoice for the solar panels…"],
  "provenance": [{ "path": "Finance/2026/solar.pdf", "root": "…", "ordinal": 3,
                   "page": 4, "section": "Payment", "score": 0.93, "start": 1180 }]
}
```

* `results` is the same array `--json` returns without `--context`.
* `context` holds compact passages, each headed by `[path#page=N]` or
  `[path#section]`, cut off at `--budget` characters (`budget=` on the API).
* `provenance` has one entry per matching passage in `results`; `page`,
  `section` and `start` appear only when known, as in `chunks`.
* With no results (exit 3 on the CLI), all three arrays are `[]`.

`locus index status --json` (and `GET /api/v1/index`, same shape)

```json
{
  "index": { "documents": 120, "no_text": 4, "chunks": 860, "embeddings": 860,
             "stale": 0, "model": "…", "chunk_ver": 2 },
  "jobs": { "index": { "pending": 2, "running": 1 } },
  "model": "…"
}
```

`jobs` maps job kind to state to count; it is `{}` when the queue is empty.
`locus index rebuild --json` prints the `index` object alone, once the
rebuild is done. `index.no_text` counts files that synchronize but have no
searchable text (images, archives, unsupported or unreadable files); they are
not in `index.documents` and are not re-extracted until their content
changes.

`locus status --json` (and `GET /api/v1/status`) keys: `peer_id`,
`hostname`, `version`, `data_dir`,
`roots[] {path,name,files,deleted,sync_state}` (`name` is what peers pair the
root by), `index {documents,no_text,chunks,embeddings,stale,model,chunk_ver}`,
`jobs {kind:{state:count}}`, `embedding {requested,used,gpu_found,note?}`,
`peers[]` (as in `locus peers --json`), `conflicts` (the number unresolved)
`last_sync` (the `locus sync now --json` object of the daemon's last
round), `progress[]` (the rounds running now, as in `locus sync status
--json`; `[]` when none), `watch {mode,watches,max_watches?,fd_limit?,reason?}`
(only from a running daemon: how it watches the roots; `mode` is `inotify`,
`kqueue-dirs` (macOS: directories only, so a file rewritten in place is seen
by the periodic reconcile, within `reconcile_interval`) or `poll` (nothing
watched, `reason` says why; changes are found by the periodic reconcile
only)) and `daemon` (`true` when a running daemon reported the status). With
a daemon running the CLI relays its status, so `sync_state` is
`synchronizing` during a round and `last_sync` is present once one has run;
with no daemon the CLI's own snapshot has `daemon: false`, `sync_state`
`idle` and no `last_sync`. A rendezvous peer adds `rendezvous: true` and has
`roots: []`; `locus peers --json` on it adds `"rendezvous": true` too.

`locus peers --json` (and `GET /api/v1/peers`)

```json
{ "peers": [
  { "peer_id": "<peer id>", "hostname": "macbook", "version": "0.1.0",
    "sync_addr": "10.0.0.2:8909", "capabilities": ["sync-protocol=1"],
    "state": "online", "addr": "10.0.0.2:8911", "first_seen": 1780000000,
    "last_seen": 1780000100, "last_error": "…", "failures": 0,
    "reachable": true, "protocol": 1, "sync_protocol": 1,
    "last_transport_error_at": "2026-10-11T21:22:03Z",
    "transport_reason": "connection refused",
    "next_retry_at": "2026-10-11T21:22:33Z", "retry_wait": "backoff" }
]}
```

`state` is `online`, `offline`, `synchronizing`, `incompatible` or
`rendezvous` (a discovery-only peer announcing no `sync` capability: it
introduces peers and is never synchronized with). A peer with
`state: "online"` is answering discovery probes; only `reachable: false` (with
`failures` > 0 and a `last_error`) means transfers to it are actually failing.
`state: "incompatible"` means the peer speaks another sync protocol
(`sync_protocol`, 0 when it never said; `last_error` says why); it is not
synchronized with or retried until one side is upgraded. While transfers
fail, `last_transport_error_at` is when the last one failed and
`transport_reason` its short cause; `next_retry_at` is when the daemon tries
again and `retry_wait` why it waits: `backoff` (the retry delay, growing up
to five minutes), `interval` (the next sync tick) or `round` (a round with
this peer is still running and the peer has not answered in it yet; no
`next_retry_at`). The failure is cleared as soon as the peer serves a
manifest in a later round, not when that round ends. These, `last_error` and
`capabilities` are omitted when empty. With `--json`, no observed peers is
`{"peers": []}` and exit 0.

`locus sync status --json`: `{"roots": [...as in status], "last_sync": null,
"syncing": false, "progress": []}`, from the running daemon when there is
one, as for `locus status`. `GET /api/v1/sync` answers `{"syncing",
"progress", "last_sync"}`. Peers are synchronized concurrently, each in its
own round part, so `progress` has one object per peer being synchronized
now:

```json
{ "peer_id": "<peer id>", "hostname": "macbook",
  "started_at": "2026-10-11T21:00:00Z", "root": "Documents",
  "phase": "transfer", "current_file": "scans/2019.pdf", "current_op": "push",
  "roots": [ { "root": "Documents", "state": "transfer", "planned": 2930,
    "push_planned": 2930, "pull_planned": 0, "delete_planned": 0,
    "push_bytes_planned": 6012954214, "pull_bytes_planned": 0,
    "done": 610, "pushed": 609, "pulled": 0, "deleted": 0, "failed": 1,
    "pushed_bytes": 1288490188, "pulled_bytes": 0,
    "skipped_in_flight": 0, "skipped_bytes": 0, "skipped_held": 0,
    "skipped_receiving": 0, "skipped_push_bytes": 0 } ],
  "push_bytes_planned": 6012954214, "pull_bytes_planned": 0,
  "pushed_bytes": 1288490188, "pulled_bytes": 0, "skipped_bytes": 0,
  "skipped_push_bytes": 0, "skipped_held_bytes": 0,
  "skipped_receiving_bytes": 0, "skipped_in_flight": 0, "skipped_held": 0,
  "skipped_receiving": 0, "actions_planned": 2930, "actions_done": 610,
  "files_planned": 2930, "files_done": 610, "bytes_per_sec": 943718.4,
  "eta_seconds": 5006, "stalled": false }
```

`phase` and a root's `state` are `pending` (roots only), `manifest`
(exchanging manifests and planning), `transfer`, `done` or `failed` (roots
only). Bytes planned cover the roots planned so far. `bytes_per_sec` is
averaged over the last ten seconds (0 when nothing moved); `eta_seconds`
estimates the time for the bytes still to send (skipped bytes excluded) and
is omitted while it cannot be estimated. `stalled` is true while the
transfer phase has neither moved a byte nor finished an action, a skip
included, for ten seconds; a round whose pushes the peer all answers "held"
moves no bytes but is not stalled. `actions_planned`/`actions_done` sum the
roots' `planned`/`done`, and `files_planned`/`files_done` count the pushes
and pulls among them (moved, skipped or failed). A pull another peer's round is already fetching (or has just
applied) is skipped rather than fetched twice: `skipped_in_flight` counts
those per root and `skipped_bytes` their size, which is not waited for (the
text output says `skipped 412 (in flight from other peers)` and leaves them
out of the bytes to pull). Likewise a push the peer answers without its
content is skipped: `skipped_held` when the peer already holds that content
at the path (only the version is applied there), `skipped_receiving` when
another peer is already sending it the same content (it is planned again
next round); `skipped_push_bytes` is their size, left out of the bytes to
push (text: `skipped 90 pushes (peer already holds them), 1 push (peer
receiving them from another peer)`). Per peer, `skipped_held`,
`skipped_receiving` and `skipped_in_flight` sum the roots' counts, and
`skipped_held_bytes` and `skipped_receiving_bytes` split
`skipped_push_bytes`. The text output lists the same per peer and root, and
`locus status` prints one `sync` line per peer, e.g. `pushing 1.2 GB /
5.6 GB to macbook (921.6 KB/s, ~1h20m)`. Content handled without being sent
is left out of the bytes to send and named after the rate and ETA:
`pushing 120.0 MB / 1.1 GB to hermes (3.1 MB/s, ~5m; 2.3 GB already held
there)` (also `on its way there from another peer` for pushes the peer is
receiving elsewhere, `coming in from another peer` for skipped pulls). While
nothing was sent but something was handled that way, the line counts files
instead: `pushing to hermes: 0 B sent, 4.8 GB already held there
(1150/1152 files)`. `stalled` is appended only when `stalled` is true.

`locus sync now --json` (and `POST /api/v1/sync`)

```json
{ "peers": 1, "pushed": 2, "pulled": 0, "deleted": 0, "conflicted": 0,
  "noop": 14, "skipped_in_flight": 0, "skipped_held": 0, "skipped_receiving": 0,
  "errors": ["…"], "started_at": "2026-10-01T09:00:00Z",
  "duration": "120ms" }
```

A peer a running round is already synchronizing with is left to that round
and listed by short ID in `busy` (omitted when empty); when every online peer
is busy the command fails with `sync: round already running`.
`skipped_in_flight` counts pulls left to another peer's round that was
fetching, or had just applied, the same content; the next round plans them
again against what arrived. `skipped_held` counts pushes the peer answered
without the content because it already held it (the version was applied,
nothing to redo), `skipped_receiving` pushes it answered as in progress
because another peer was sending it the same content (planned again next
round). None of these are failures; the text output adds a line such as
`skipped 3 (in flight from other peers), 40 pushes (peer already holds
them)`, which `locus sync status` appends to its `last round:` line.
`errors` is omitted when the round had none; a failure against one peer or
root is reported there and does not fail the command. The exit code is 5 when
`conflicted` is above 0, with or without `--json`.

`locus conflicts --json` (and `GET /api/v1/conflicts`)

```json
{ "conflicts": [
  { "id": "documents:proposal.txt:1a2b3c4d:9f8e7d6c", "root": "…", "relpath": "proposal.txt",
    "local_path": "proposal.txt", "conflict_path": "proposal.conflict-30f2593b-9f8e7d6c.txt",
    "local_hash": "…", "conflict_hash": "…", "local_writer": "<peer id>",
    "conflict_writer": "<peer id>", "created_at": 1780000000, "resolved_at": null }
]}
```

`local_*` describe the version on the normal path, `conflict_*` the other
version; `conflict_path` is `""` when no sibling file was written (a delete
concurrent with an edit). The `id` is `<root name>:<relpath>:<normal-path
hash prefix>:<other hash prefix>`, the same on every peer that records the
conflict; records from older builds may carry another form and keep it.
`locus conflicts show <id> --json` prints one such
object; `locus conflicts resolve <id> --json` prints `{"resolved": "<id>"}`.

`locus skill --json` prints `{"skill": "<markdown>"}`; with `--section`,
`{"section": "<heading>", "body": "<markdown>"}`.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | success |
| 1 | error (I/O, database, network, unknown conflict id) |
| 2 | usage error (bad flags, subcommand or `--section`) |
| 3 | no results: search found nothing, or `locus peers` observed no peer |
| 4 | configuration problem (missing/invalid config, unreadable roots, a command that needs roots on a rendezvous peer) |
| 5 | unresolved conflicts exist (`locus conflicts`, `locus sync now`) |

Treat 3 as "no evidence found", not as a failure of your query.

## Conflict behaviour

If two peers edit the same file while offline, neither version supersedes the
other. Both are preserved:

```text
proposal.docx                               <- deterministic normal-path winner
proposal.conflict-30f2593b-1a2b3c4d.docx    <- the other version, kept beside it
```

The sibling is named `<name>.conflict-<first 8 characters of its writer's
peer id>-<first 8 hex of its hash><ext>`, the same on every peer (use
`locus peers` to see which machine a peer id belongs to; siblings from older
builds may carry a hostname instead). The winner is the version with the greater content hash (the
writer's peer ID only breaks a tie between identical contents), so every peer
picks the same one and no machine is favoured. A delete concurrent with an
edit never wins: the edit stays on the normal path and the conflict is
recorded without a sibling.

* Nothing is silently discarded; the sibling file is a normal file and syncs.
* `locus conflicts` lists them, `show` explains both sides, `resolve <id>`
  accepts the current normal-path version. Resolving only closes the record
  on this peer; the sibling stays on disk (delete it, for instance through
  the API, once its content is merged).
* Renames may appear as delete + create. A directory a synced delete leaves
  empty is removed (its empty parents too, never the root), so a renamed
  folder does not linger as an empty copy; a directory holding anything,
  even a `.DS_Store`, is kept, and an empty folder you made yourself that
  never held synced files is left alone.

## File operations

The CLI is read-only for document contents. Writes go through the HTTP API
(used by the PWA) and land on the ordinary filesystem, so they propagate to
other peers and become searchable like any other change:

```text
GET    /healthz                                  no token; {"status":"ok","peer_id":"…"}
GET    /api/v1/status                            as locus status --json
GET    /api/v1/peers                             as locus peers --json
GET    /api/v1/search?q=&path=&type=&limit=&root=&context=1&budget=
GET    /api/v1/index                             as locus index status --json
GET    /api/v1/sync                              {"syncing","progress","last_sync"}
POST   /api/v1/sync                              as locus sync now --json
GET    /api/v1/files?path=/dir&root=             {"root","path","entries":[…]}
GET    /api/v1/files/content?path=/file&root=    raw bytes, as an attachment
POST   /api/v1/files?path=/dir/&name=x.ext&root= raw body -> 201 {"path","size","hash"}
DELETE /api/v1/files?path=/file&root=            {"deleted":"/file"}
POST   /api/v1/files/move  {"from":"/a.txt","to":"/dir/b.txt","root":"…"}
POST   /api/v1/directories {"path":"/New Folder","root":"…"}  -> 201 {"path"}
GET    /api/v1/conflicts                         as locus conflicts --json
POST   /api/v1/conflicts/resolve {"id":"<id>"}   {"resolved":"<id>"}, 404 if unknown
```

Search and index status answer the same JSON as `locus search --json`
(`--context --json` when `context=1`) and `locus index status --json`.
Directory entries are `{name, path, is_dir, size, mode, mtime, hash?}`, with
`path` relative to the root. File routes take an optional `root=` (query) or
`"root"` (JSON body) naming a configured root by its absolute path (as in
`roots[].path` of the status); the default is the first root. On search,
`root=` restricts results to that root and is omitted to search every root.
An unconfigured root is refused with 400. An upload's `path` is the target
file, or a directory ending in `/` with the file name in `name=` (or an
`X-Locus-Filename` header). A move stays within one root.

An upload is streamed to disk, so its size is bounded only by
`upload_max_bytes`; a larger body (declared or actual) is refused with 413 and
leaves no file, as is (with 400) a body shorter than its `Content-Length`.
JSON request bodies are limited to 1 MiB (413 beyond that).

The API requires `Authorization: Bearer <token>` (`api.token` from the
configuration, or else the token generated in `<data_dir>/api_token`) and
listens on `api.addr`:`api.port` (default `127.0.0.1:8910`, loopback only).
Errors answer `{"error": "<message>"}`; messages never contain absolute host
paths. A path that does not exist is `404` for listing, reading, moving (the
source) and deleting; a refused delete changes nothing and is not audited.

## Examples

```bash
# find and read a passage about an invoice
locus search "invoice for the solar panels" --context --json

# only PDFs under Finance, 5 results
locus search "insurance terms" --type pdf --path Finance --limit 5 --json

# is everything synchronized?
locus status --json | jq '.roots, .conflicts'

# after a conflict, inspect and accept one side
locus conflicts
locus conflicts show documents:proposal.txt:1a2b3c4d:9f8e7d6c
locus conflicts resolve documents:proposal.txt:1a2b3c4d:9f8e7d6c
```

## Common failure modes

* **Exit 4, "at least one sync root is required"** — no configuration; ask the
  user to run `locus init --root <dir>` (or pass `--config`).
* **`index.documents` is 0 but files exist** — indexing has not run; use
  `locus index rebuild` (or start `locus serve`).
* **Exit 1, "a locus daemon is running on this data directory"** — `locus
  serve` holds an exclusive lock on `<data_dir>/daemon.lock` while it runs;
  `locus index rebuild` (and a second `locus serve`) refuse instead of
  dropping the index under its workers. The daemon indexes changes on its
  own; to force a rebuild, stop the daemon, rebuild, and start it again. The
  operating system releases the lock when the daemon exits, even on a crash.
* **WARN "daemon shutdown cut short" when `locus serve` stops** — something
  was still running at the 15-second shutdown deadline; the `pending` field
  names it (a part of the daemon, or the requests still in flight). The
  daemon still exits 0: writes are atomic and interrupted transfers resume
  on the next start, so nothing needs repairing. Only a recurring entry
  naming the same part is worth reporting.
* **Exit 4, "data directory ... belongs to user ..."** — the command runs as
  a different user than the one that owns the data directory (typically root
  against a system service's). Rerun it as that user, e.g.
  `runuser -u locus -- locus --config /etc/locus/config.json status`.
* **`locus serve` fails with "open daemon lock: ... permission denied"** —
  another user (root, usually) created `daemon.lock`; the message names its
  owner. Ask the user to `chown -R` the data directory back to the service
  user; do not delete the data directory.
* **Exit 3 with a plausible query** — the document may be missing, deleted,
  or excluded by `--path`/`--type`; retry without filters.
* **Only one peer appears in `locus peers`** — the other machine is offline or
  not on the same WireGuard network; Locus converges automatically once it
  returns. Peers that are not in each other's `discovery.peers` only meet
  through a peer both can reach (peer exchange): the gateway of a WireGuard
  route (`gateway_targets` in `locus init --json`) or a configured
  `--discovery-peer`, typically a rendezvous peer on the hub. After that they
  remember each other, but until then that peer must be up.
* **A peer shows as `online` but never syncs** — reachability is one-directional
  (a host firewall allowing probes but not the sync port, or vice versa). The
  peer is marked `reachable: false`, `locus peers` prints why, when it last
  failed and when it is tried next (`[transfer failed 32m ago (connection
  refused); next attempt in 12s]`), and the daemon logs `peer unreachable
  for transfer` while retrying with backoff (at most five minutes apart). On the peer in question, allow the sync port
  (8909/tcp) and discovery port (8911/udp) inbound on the WireGuard interface.
* **A peer shows as `incompatible`** — the two machines run Locus builds
  with different sync protocols; `locus peers` prints the versions. Upgrade
  the older one; the peer becomes `online` again once its announcement
  changes (or after restarting `locus serve`).
* **One root syncs with a peer, another does not** — roots are matched by
  name and the peer does not have that one (logged once as `peer does not
  share root; skipping it`), or it refused it (`peer refused sync of root`,
  e.g. 403 before it discovered this device). This is not a reachability
  problem and does not back off the peer's other roots.
* **A deleted file came back** — a device that never saw the deletion pushed
  its old copy. A deletion record is kept for at least `tombstone_age` and
  until every peer that synchronized the root has completed a clean round
  since, so this takes a device this peer never synchronized with (or one
  with a new identity).
* **A hidden file or folder (`.notes/`, `.git`) is not on the other machine,
  or not found by search** — hidden paths are skipped by default (see
  above); the receiving daemon logs `refusing hidden paths from peer` once
  per peer and root. After upgrading from a build that indexed them, they
  disappear from search on the first pass; the files are untouched.
* **Stale embeddings after an upgrade** — automatic: old embeddings are marked
  stale and regenerated in the background; sync is unaffected.
* **`connection refused` on the API** — the daemon is not running
  (`locus serve`), or you are connecting from another machine while `api.addr`
  is the default loopback address.

## Security assumptions

* Transport is assumed to be a trusted WireGuard network; Locus does no NAT
  traversal, relays or public discovery.
* Peer handshakes are authenticated with a per-install ed25519 identity;
  sync requests carry a signature over method, path and query, timestamp and
  body hash (for a put, the content hash it declares, so the signature is
  checked before the body is read). `authorized_peers` restricts which peer
  IDs are discovered and served; empty admits any peer this one has
  discovered.
* Paths are strictly validated against configured roots: `..`, absolute paths
  and symlink escapes are rejected. API file operations cannot leave roots.
* Uploads are size-limited and installed atomically after hash verification,
  so partial files never appear as valid documents.
* The API requires an application-level bearer token; wire-level access alone
  does not grant document access (`authorized_peers` does not apply to the
  API). Security-sensitive actions and conflicts are recorded in a local
  audit log; refused requests are summarized per remote host, and the log is
  pruned by age and size (`audit` in the configuration).

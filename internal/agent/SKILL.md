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
| `locus status` | peer identity, roots, index, embedding device, peers, conflicts |
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
`--discovery-peer <host[:port]>` (repeatable) adds a discovery target; one
always-on peer is enough, since peers pass on the other peers they know (peer
exchange) and remember them across restarts.
`--root <name>=<dir>` gives the root an explicit name. Peers synchronize a root
when its *name* matches, not its path: an unnamed root is named by its last
path component (`~/Documents` is `Documents`), so folders with different names
on different machines pair only when both are given the same name. Names must
be unique on a peer (exit 4 otherwise). In `config.json` a root is a path
string or `{"path": "<dir>", "name": "<name>", "sync_hidden": true}` (`name`
and `sync_hidden` optional); `locus init --json` prints `config`, `data_dir`,
`peer_id`, `roots` (each root in that same shape), `advertised_addr` (the
`host:port` other peers are told to dial) and `advertised_by` (how that host
was chosen; `default route` means it is a guess that may be the Wi-Fi rather
than the WireGuard address, fixed by setting `peer.addr` or `peer.interface`). A
`--data-dir` given to `init` is saved in the configuration. `init` refuses
(exit 4) to replace an existing configuration, which holds the API token and
configured peers; pass `--force` only when the user explicitly wants to start
over.

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
round) and `daemon` (`true` when a running daemon reported the status). With
a daemon running the CLI relays its status, so `sync_state` is
`synchronizing` during a round and `last_sync` is present once one has run;
with no daemon the CLI's own snapshot has `daemon: false`, `sync_state`
`idle` and no `last_sync`.

`locus peers --json` (and `GET /api/v1/peers`)

```json
{ "peers": [
  { "peer_id": "<peer id>", "hostname": "macbook", "version": "0.1.0",
    "sync_addr": "10.0.0.2:8909", "capabilities": ["sync-protocol=1"],
    "state": "online", "addr": "10.0.0.2:8911", "first_seen": 1780000000,
    "last_seen": 1780000100, "last_error": "…", "failures": 0,
    "reachable": true, "protocol": 1, "sync_protocol": 1 }
]}
```

`state` is `online`, `offline`, `synchronizing` or `incompatible`. A peer with
`state: "online"` is answering discovery probes; only `reachable: false` (with
`failures` > 0 and a `last_error`) means transfers to it are actually failing.
`state: "incompatible"` means the peer speaks another sync protocol
(`sync_protocol`, 0 when it never said; `last_error` says why); it is not
synchronized with or retried until one side is upgraded. `last_error` and
`capabilities` are omitted when empty. With `--json`, no observed peers is
`{"peers": []}` and exit 0.

`locus sync status --json`: `{"roots": [...as in status], "last_sync": null,
"syncing": false}`, from the running daemon when there is one, as for
`locus status`.

`locus sync now --json` (and `POST /api/v1/sync`)

```json
{ "peers": 1, "pushed": 2, "pulled": 0, "deleted": 0, "conflicted": 0,
  "noop": 14, "errors": ["…"], "started_at": "2026-10-01T09:00:00Z",
  "duration": "120ms" }
```

`errors` is omitted when the round had none; a failure against one peer or
root is reported there and does not fail the command. The exit code is 5 when
`conflicted` is above 0, with or without `--json`.

`locus conflicts --json` (and `GET /api/v1/conflicts`)

```json
{ "conflicts": [
  { "id": "proposal.txt:1a2b3c4d:9f8e7d6c", "root": "…", "relpath": "proposal.txt",
    "local_path": "proposal.txt", "conflict_path": "proposal.conflict-macbook-9f8e7d6c.txt",
    "local_hash": "…", "conflict_hash": "…", "local_writer": "<peer id>",
    "conflict_writer": "<peer id>", "created_at": 1780000000, "resolved_at": null }
]}
```

`local_*` describe the version on the normal path, `conflict_*` the other
version; `conflict_path` is `""` when no sibling file was written (a delete
concurrent with an edit). `locus conflicts show <id> --json` prints one such
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
| 4 | configuration problem (missing/invalid config, unreadable roots) |
| 5 | unresolved conflicts exist (`locus conflicts`, `locus sync now`) |

Treat 3 as "no evidence found", not as a failure of your query.

## Conflict behaviour

If two peers edit the same file while offline, neither version supersedes the
other. Both are preserved:

```text
proposal.docx                                  <- deterministic normal-path winner
proposal.conflict-macbook-1a2b3c4d.docx        <- the other version, kept beside it
```

The sibling is named `<name>.conflict-<host of its writer>-<first 8 hex of
its hash><ext>`. The winner is the version with the greater content hash (the
writer's peer ID only breaks a tie between identical contents), so every peer
picks the same one and no machine is favoured. A delete concurrent with an
edit never wins: the edit stays on the normal path and the conflict is
recorded without a sibling.

* Nothing is silently discarded; the sibling file is a normal file and syncs.
* `locus conflicts` lists them, `show` explains both sides, `resolve <id>`
  accepts the current normal-path version. Resolving only closes the record
  on this peer; the sibling stays on disk (delete it, for instance through
  the API, once its content is merged).
* Renames may appear as delete + create.

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
locus conflicts show proposal.txt:1a2b3c4d:9f8e7d6c
locus conflicts resolve proposal.txt:1a2b3c4d:9f8e7d6c
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
* **Exit 3 with a plausible query** — the document may be missing, deleted,
  or excluded by `--path`/`--type`; retry without filters.
* **Only one peer appears in `locus peers`** — the other machine is offline or
  not on the same WireGuard network; Locus converges automatically once it
  returns. Peers that are not in each other's `discovery.peers` only meet
  through a peer both can reach (peer exchange): after that they remember each
  other, but until then the always-on peer must be up, and listed by both.
* **A peer shows as `online` but never syncs** — reachability is one-directional
  (a host firewall allowing probes but not the sync port, or vice versa). The
  peer is marked `reachable: false`, `locus peers` prints why, and the daemon
  logs `peer unreachable for transfer` while retrying with backoff (at most
  five minutes apart). On the peer in question, allow the sync port
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

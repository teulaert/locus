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
locus --help          # command list, global flags, exit codes
locus <command> --help
```

Locus needs no running daemon for CLI commands: each invocation opens the
local databases directly. `locus serve` runs the daemon (discovery, sync,
indexing, HTTP API, PWA) and is what keeps peers converged.

## Commands

| Command | Purpose |
| --- | --- |
| `locus init` | write a configuration file with sync roots (see below) |
| `locus status` | peer identity, roots, index, embedding device, peers, conflicts |
| `locus peers` | reachable peers and presence state |
| `locus sync status` | local synchronization state |
| `locus sync now` | reconcile locally and exchange changes with online peers |
| `locus search <query>` | hybrid lexical (FTS5/BM25) + semantic search |
| `locus index status` | derived index counters and queue state |
| `locus index rebuild` | delete and reconstruct the search index from local files |
| `locus conflicts` | list unresolved conflicts |
| `locus conflicts show <id>` | one conflict, both sides identified |
| `locus conflicts resolve <id>` | keep the normal-path version |
| `locus serve` | run the peer daemon |
| `locus version` | print version |

Global flags (before the command): `--config <path>`, `--data-dir <dir>`,
`--json`, `--quiet`, `--timeout <dur>`.

`locus init --root <dir>` (repeatable; `--peer-port <n>` optional) creates the
configuration and the peer identity. A `--data-dir` given to `init` is saved in
the configuration. `init` refuses (exit 4) to replace an existing configuration,
which holds the API token and configured peers; pass `--force` only when the
user explicitly wants to start over.

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
Collections are always arrays (`[]`, never `null`).

`locus search "q" --json`

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
          "snippet": "…invoice for the solar panels…" }
      ]
    }
  ]
}
```

* `path` is relative to `root`; join them for the absolute path.
* `chunks` are the strongest matching passages, ordered.
* `page` is present for paginated formats (PDF); `section` for headed text.

`locus search "q" --context --json`

```json
{
  "query": "q",
  "context": ["[Finance/2026/solar.pdf#page=4]\n…invoice for the solar panels…"],
  "provenance": [{ "path": "Finance/2026/solar.pdf", "root": "…", "ordinal": 3, "page": 4, "score": 0.93 }]
}
```

`locus status --json` keys: `peer_id`, `hostname`, `version`, `data_dir`,
`roots[] {path,files,deleted,sync_state}`, `index {documents,no_text,chunks,embeddings,stale,model,chunk_ver}`,
`jobs {kind:{state:count}}`, `embedding {requested,used,gpu_found}`,
`peers[] {peer_id,hostname,state,addr,sync_addr,failures,reachable,last_error}`,
`conflicts`, `last_sync`. A peer with `state: "online"` is answering discovery
probes; only `reachable: false` (with `failures` > 0 and a `last_error`) means
transfers to it are actually failing. `index.no_text` counts files that
synchronize but have no searchable text (images, archives, unsupported or
unreadable files); they are not in `index.documents` and are not re-extracted
until their content changes.

`locus conflicts --json`

```json
{ "conflicts": [
  { "id": "proposal.txt:1a2b3c4d:9f8e7d6c", "root": "…", "relpath": "proposal.txt",
    "local_path": "proposal.txt", "conflict_path": "proposal.conflict-macbook-1a2b3c4d.txt",
    "local_hash": "…", "conflict_hash": "…", "local_writer": "<peer id>",
    "conflict_writer": "<peer id>", "created_at": 1780000000, "resolved_at": null }
]}
```

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | success |
| 1 | error (I/O, database, network) |
| 2 | usage error (bad flags or subcommand) |
| 3 | search completed but found nothing |
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

* Nothing is silently discarded; the sibling file is a normal file and syncs.
* `locus conflicts` lists them, `show` explains both sides, `resolve <id>`
  accepts the current normal-path version (the sibling stays on disk).
* Renames may appear as delete + create.

## File operations

The CLI is read-only for document contents. Writes go through the HTTP API
(used by the PWA) and land on the ordinary filesystem, so they propagate to
other peers and become searchable like any other change:

```text
GET    /api/v1/status            GET  /api/v1/peers
GET    /api/v1/search?q=&path=&type=&limit=&context=1
GET    /api/v1/files?path=/dir   GET  /api/v1/files/content?path=/file
POST   /api/v1/files?path=/dir/&name=x.ext   (raw body upload)
DELETE /api/v1/files?path=/file
POST   /api/v1/files/move  {"from":"/a.txt","to":"/dir/b.txt"}
POST   /api/v1/directories {"path":"/New Folder"}
GET    /api/v1/conflicts
```

The API requires `Authorization: Bearer <token>` (token from the peer's data
directory or `api.token` config) and binds to trusted interfaces only.

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
* **Exit 3 with a plausible query** — the document may be missing, deleted,
  or excluded by `--path`/`--type`; retry without filters.
* **Only one peer appears in `locus peers`** — the other machine is offline or
  not on the same WireGuard network; Locus converges automatically once it
  returns.
* **A peer shows as `online` but never syncs** — reachability is one-directional
  (a host firewall allowing probes but not the sync port, or vice versa). The
  peer is marked `reachable: false`, `locus peers` prints why, and the daemon
  logs `peer unreachable for transfer` while retrying with backoff. On the peer
  in question, allow the sync port (8909/tcp) and discovery port (8911/udp)
  inbound on the WireGuard interface; the log line names the interface.
* **A file arrived but `locus log` mentions adopting provenance** — a previous
  process died between writing a received file and recording it. The remote
  author and version are kept; nothing to do.
* **Stale embeddings after an upgrade** — automatic: old embeddings are marked
  stale and regenerated in the background; sync is unaffected.
* **`connection refused` on the API** — the daemon is not running
  (`locus serve`), or you are not on the trusted network.

## Security assumptions

* Transport is assumed to be a trusted WireGuard network; Locus does no NAT
  traversal, relays or public discovery.
* Peer handshakes are authenticated with a per-install ed25519 identity;
  requests carry signed method, path, timestamp and body hash.
* Paths are strictly validated against configured roots: `..`, absolute paths
  and symlink escapes are rejected. API file operations cannot leave roots.
* Uploads are size-limited and installed atomically after hash verification,
  so partial files never appear as valid documents.
* The API requires an application-level bearer token; wire-level access alone
  does not grant document access. Security-sensitive actions and conflicts are
  recorded in a local audit log.

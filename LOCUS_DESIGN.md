# Locus --- Technical Design & Implementation Specification

**Status:** Initial implementation brief\
**Primary language:** Go\
**Clients:** Linux, macOS, Web/PWA (including iPhone)\
**Network assumption:** participating computers are reachable through an
existing WireGuard network.

## 1. Vision

Locus is a local-first, peer-to-peer document system combining file
synchronization, local indexing, full-text and semantic search, an
agent-friendly CLI, and a mobile-friendly PWA.

There is **no central server, no master node, and no cloud storage
requirement**. Every computer is an equal peer. Files remain ordinary
files on the local filesystem. Search indexes, extracted text, chunks
and embeddings are derived local state and are never synchronized.

**The filesystem is the source of truth.**

## 2. Core design principles

-   **Local first.** Files remain usable through Finder, Linux file
    managers, shells and normal applications even when Locus is stopped.
-   **Peer-to-peer.** Any peer may synchronize with any other reachable
    peer. Peers may be offline for arbitrary periods.
-   **WireGuard as network layer.** Locus does not implement public
    discovery, NAT traversal, relays or cloud rendezvous.
-   **Derived data stays local.** SQLite search databases, extracted
    text, embeddings, previews and indexing queues are never
    synchronized.
-   **Disposable indexes.** Deleting and rebuilding the local search
    index must always be safe.
-   **Small and understandable.** Locus is not intended to become a
    general-purpose Syncthing replacement.

## 3. Per-peer architecture

One `locus` Go binary runs on every computer and contains four logically
separated subsystems:

1.  peer discovery and presence;
2.  filesystem synchronization;
3.  local indexing and search;
4.  HTTP API and embedded PWA.

A peer owns normal filesystem roots plus private application state. The
CLI and HTTP API call the same application services rather than
duplicating behavior.

## 4. Peer discovery

WireGuard does not provide application discovery, but it already gives
Locus a finite set of possible network peers.

On startup Locus should identify the configured WireGuard interface and
known WireGuard peers/addresses, then send a small discovery probe to a
fixed UDP port on those addresses. Machines running Locus reply with an
authenticated handshake. Successful peers enter an in-memory presence
table and are periodically probed.

Do not scan the entire subnet. Do not require multicast, mDNS or an
external discovery service.

Where the WireGuard topology does not name the other peers (a
hub-and-spoke network, where a spoke's only WireGuard peer is the hub with
a routed prefix, or a peer in a container with no WireGuard interface),
peers exchange what they know: a reply lists the peers the responder has
verified itself (peer ID, discovery address, sync address; at most 64), and
a probe announces the prober's discovery port (`discovery-port=<n>`), so the
responder can probe it back from the probe's source host. A peer probes its
configured targets plus every peer learned this way, filtered by
`authorized_peers`, and keeps learned peers in `state.db` so they are found
again after a restart while the peer that introduced them is down. Only
addresses reported in a signed packet by an authorized peer, or configured,
are ever probed.

Every Locus installation generates a persistent cryptographic peer
identity on first launch. A handshake should include at least
protocol/version, peer ID, hostname, Locus version and capabilities.
WireGuard already secures transport, but the Locus identity gives stable
application identity and protects against accidentally speaking the
protocol to an unrelated service.

Typical presence states are `online`, `offline`, `synchronizing` and
`incompatible`.

Two versions are negotiated. The discovery packet carries the discovery
protocol; the peer announcement carries the sync protocol (the manifest
format and the meaning of put, delete and apply) as the capability
`sync-protocol=<n>`. Every sync request carries `X-Locus-Protocol: <n>`
and every authenticated response answers with the server's own. A server
refuses a request in another sync protocol with `426 Upgrade Required`
before it reaches apply; a request without the header comes from a peer
that predates it and speaks version 1. A peer whose discovery announcement
names another sync protocol, or whose server refused a transfer with 426,
is `incompatible`: it is left out of synchronization rounds (not retried
with transport backoff) until its announcement changes, and status shows
the version it speaks and why.

## 5. Filesystem model

Users configure one or more synchronization roots, for example:

``` text
~/Documents
~/Projects/Personal
```

Each root has a name that identifies it across peers: an explicit name in
the configuration, or else the root's last path component. Peers synchronize
roots whose names match, so the same logical root may live at different
paths, and under different folder names, on different machines. Names are
unique per peer.

The hierarchy below each root is synchronized, except hidden paths: any
path with a component whose name starts with `.` (`.git`, `.obsidian`,
`.DS_Store`) is per-machine state, not a document. It is not inventoried,
indexed or synchronized, and a peer sending one is refused, unless the root
opts in with `sync_hidden`. Hidden directories are not walked or watched.

Locus metadata should not pollute user directories. Private state belongs in the
platform-appropriate application data directory.

## 6. Synchronization engine

Optimize the first implementation for correctness and simplicity. Do
**not** implement block-level delta synchronization initially. Transfer
the complete contents of a changed file.

Tracked metadata should include relative path, size, content hash,
logical version information and deletion/tombstone state where required.
Do not use modification timestamps as the sole indication of content
identity.

Peers exchange compact manifests and determine operations such as
create, update, delete, conflict and no-op. Rename detection is
optional; correctness may initially represent a rename as delete +
create.

### Desired-state model

Filesystem watcher events are hints, not an immutable event log. Locus
works toward the current desired state.

If a file is created, changed twice and deleted before work starts, the
final desired state is simply "file absent." Locus should avoid needless
synchronization, parsing and embedding of obsolete intermediate states.

Incoming transfers must use temporary files, verify hashes and then
atomically rename into place. Partial files must never appear as valid
documents.

### Request signatures and streamed transfers

Every sync request carries `X-Locus-Peer`, `X-Locus-Timestamp` and
`X-Locus-Signature`: an ed25519 signature over method, request target
(path and query), timestamp and a body hash, one per line. File content is
never held in memory on any hop, so the signature has to be checkable
before the body is read:

-   `POST /v1/put` declares the content's SHA-256 in its query (`hash`,
    lowercase hex, together with `size`, `version` and `writer`), and the
    body hash it signs is that declared hash. For an honest sender this is
    exactly the SHA-256 of the body, so the wire format is unchanged and
    peers that sign the bytes they send verify as before.
-   Every other request has an empty body and signs the hash of the empty
    body.

The server checks the signature, the timestamp window, authorization and
the protocol version from the headers and query alone, and refuses a put
whose declared `size` (or `Content-Length`) exceeds `upload_max_bytes`
with 413, all before reading a byte of the body. An unauthenticated or
unauthorized client therefore cannot make a peer buffer or write anything.
An accepted body streams into a `.locus-tmp-*` file in fixed-size chunks
while being hashed; reading stops one byte past the size limit (413), and
content that does not hash to the declared value is rejected and the
temporary file removed before any rename. A tampered body is thus refused
as it was when the signature covered the bytes themselves. Pull streams the
`/v1/file` response through the same install path, verified against the
manifest's hash, and API uploads stream the same way, hashed on the way to
disk and checked against a declared `Content-Length`.

### Peer endpoints

The sync service listens on the peer port (8909 by default). Roots are
addressed by name (`rootname=`), never by local path.

``` text
GET  /v1/whoami                         unauthenticated: peer ID, sync protocol, build version
GET  /v1/manifest?rootname=             every record of the root, tombstones included
GET  /v1/file?rootname=&relpath=        the file's bytes, X-Locus-Hash header
POST /v1/put?rootname=&relpath=&hash=&size=&version=&writer=[&offset=]   body: the content (from offset)
GET  /v1/put/status?hash=               {"hash", "offset"}: bytes held of an interrupted put
POST /v1/delete?rootname=&relpath=&hash=&version=&writer=
```

Put and delete answer `{"result": "applied" | "noop" | "conflicted" |
"rejected"}`, with the conflict record when the receiver detected one (so
both peers record it) and an `error` when rejected. Refusals are HTTP
statuses, checked in this order: 401 (missing or bad signature, timestamp
outside a five-minute window), 403 (peer not in `authorized_peers`, or, when
that list is empty, not yet discovered), 426 (another sync protocol), then
404 (no root of that name), 400 (bad path or version) and 413 (larger than
`upload_max_bytes`). The client treats only a dial failure, a dropped
connection or a 5xx as a transport failure that backs the peer off; a 426
marks the peer incompatible; any other status is an answer for that root
alone, and the round goes on with the next root and peer.

### Resumable puts

A put of a file of at least 1 MiB (`size` declared) is received into a
partial object in `<data_dir>/partials/<hash>.part`, keyed by the declared
content hash and never inside a root. When the body breaks off (the sender
restarted, slept or changed network), what arrived is kept. Before pushing a
file of at least 1 MiB the sender asks `GET /v1/put/status?hash=` (signed
like any bodiless request) and, when the answer's `offset` is between 1 and
the size, sends only the rest: the same put query plus `offset=N` (signed
with it), the body being the bytes from `N` on. The signed body hash is
still the hash of the whole object.

The receiver accepts a continuation only when `offset` equals the length of
the partial object it holds and no other upload of that object is in
progress; otherwise it answers 409 and the sender sends the whole file
again from zero. `upload_max_bytes` bounds the whole object across
continuations: the declared `size`, `offset` and `offset` plus a
`Content-Length` over it are refused with 413 before the body is read, and
reading stops one byte past it. Once all `size` bytes are present the whole
object is hashed (the stored prefix is read back, not trusted); a mismatch
removes the partial and the put is rejected, otherwise the object is handed
to Apply, which installs it through the usual temporary file, hash check,
fsync and rename. A body that ends before `size` is answered `rejected`
(`incomplete`) and kept for the next attempt. Partial objects not written
for 24 hours are removed when the daemon starts and with each periodic
reconciliation.

### Receive first, lock later

Nothing waits on a peer while holding a path's lock (`LockPath`, which
Apply, PullVersion and the reconciler's record writes share). Every put
body and every pulled file is first staged in `<data_dir>/partials`
(resumable `<hash>.part` objects, private `*.tmp` files otherwise),
bounded by `upload_max_bytes` and verified against its declared hash. Only
then does Apply take the lock, decide, and install: the staged file is
renamed into the root (copied through a `.locus-tmp-*` file when the data
directory is on another filesystem), flushed, and recorded. A sender that
stalls mid-body therefore holds up only its own request; before, it held
the path's lock, and with it every other put and pull of that file and the
whole sequential round, for as long as it stalled.

Stalls are bounded on both sides. The receiver puts each read of a put
body under a 60-second inactivity deadline (instead of the server's
whole-request `ReadTimeout`, which would cut off a large upload on a slow
link) and answers a stalled put 408, keeping a resumable partial. The
sender runs every peer request under a 60-second watchdog that aborts it,
as a transport failure, when no byte moves in either direction; there is no
limit on a request as a whole. A lock wait longer than five seconds is
logged while it lasts and when it ends, with the path, the waiting and the
holding operation. At startup, when it holds the daemon lock, `locus serve`
removes every staging `*.tmp` file and every `.locus-tmp-*` file in the
roots: only a killed process can have left them. A peer cannot create a
file with a `.locus-tmp-` name.

No protocol version change is involved. A build without resumable puts
answers the status query 404, and the sender then sends a plain put
without `offset`, which is exactly the request older builds send; the
receiver never sees an `offset` it did not report. Within a round, actions
that move no content run first and transfers follow smallest first, so an
interrupted round has delivered the small files; on shutdown a round
finishes the transfer in progress for up to five seconds and starts no
other before it is cancelled.

### Deletions and tombstones

A deletion is recorded as a tombstone carrying the file's version vector,
and travels like an edit. A tombstone is forgotten only when it is older
than `tombstone_age` (30 days by default) *and* predates the start of the
last clean round (every planned action succeeded) with every peer this one
has ever synchronized the root with, as recorded per root and peer. A
long-offline peer therefore still receives the delete instead of
resurrecting its stale copy.

## 7. Versioning and conflicts

Timestamps cannot safely resolve concurrent edits. If two offline peers
independently edit the same common ancestor, neither version supersedes
the other.

Use a simple per-file **version vector or equivalent causal version
representation** to distinguish descendants from genuine divergence.

On a genuine conflict:

-   preserve both versions;
-   choose the normal-path version using a deterministic rule (the
    greater content hash wins; the writer peer ID breaks a tie between
    identical contents), so no peer is favoured in every conflict;
-   store the other as a sibling conflict file;
-   record the conflict locally;
-   never silently discard either side.

The sibling's name is a pure function of the losing version (its
writer's hostname, or short peer ID when unknown, and the first eight hex
digits of its content hash), so peers that detect the same conflict
independently write the same sibling:

``` text
proposal.docx
proposal.conflict-macbook-1a2b3c4d.docx
```

A delete concurrent with an edit is a conflict too, but the edit always
survives on the normal path; the conflict is recorded without a sibling.

The initial implementation should not attempt semantic/text
auto-merging.

The CLI offers `locus conflicts`, `locus conflicts show <id>` and
`locus conflicts resolve <id>`, which accepts the current normal-path
version and closes the local record; it does not touch either file.

## 8. Change detection and reconciliation

Use native filesystem notifications where practical, but never depend
exclusively on them. Periodically reconcile actual filesystem state
against known metadata to recover from missed events, sleep, crashes and
external modifications.

## 9. Persistent work queue

Expensive asynchronous operations should run through a SQLite-backed
queue. Example jobs include text extraction, chunking, embedding
generation, index removal and preview generation.

Jobs must be revalidated against current desired state before execution.
Multiple modifications should coalesce into work for the newest version.
If a queued document has already disappeared, obsolete
extraction/embedding work should be dropped.

Long operations should accept cancellation contexts. Worker concurrency
should be configurable/resource-aware. Interactive search takes priority
over background indexing.

## 10. Local SQLite database

SQLite is the default local storage engine, split in two databases in
the data directory:

``` text
state.db   docs (records, version vectors, tombstones), conflicts, audit,
           peer_rounds, pending_arrivals, meta
index.db   documents, chunks, fts_chunks, embeddings, jobs, meta
```

Keep synchronization correctness separate from disposable search state.
`locus index rebuild` must be able to reconstruct derived indexes
entirely from local files.

Use SQLite FTS5 for lexical/full-text indexing.

## 11. Document extraction

Initial useful formats:

-   plain text;
-   Markdown;
-   PDF;
-   DOCX;
-   optionally HTML.

Extraction should be behind a Go interface so additional formats can be
added later. Files without extractable text are still synchronized
normally.

## 12. Chunking

Semantic indexing operates at **chunk level**, not whole-document level.

Target roughly **500--1000 tokens** per chunk while preferring natural
boundaries: headings/sections, paragraphs, PDF pages, then token-size
fallback. Small overlap may be used.

Each chunk should retain provenance including document ID/path, page if
known, heading/section if known, chunk ordinal and content hash. Search
must be able to identify not merely a relevant document but the relevant
passage/page.

## 13. Embeddings

Embeddings are generated locally. No cloud API is required.

Define a pluggable Go embedding interface so the inference
implementation/model can change without affecting search APIs.

Locus must work CPU-only. Initial bulk indexing may take time, but
subsequent work is incremental. Where the chosen runtime supports
hardware acceleration, use automatic GPU detection with clean CPU
fallback.

Configuration should support approximately:

``` text
embedding.device = auto
embedding.device = cpu
embedding.device = gpu
```

`auto` is the default.

Store model identifier/version, vector dimensions and chunking version.
An incompatible model/configuration change marks old embeddings stale
and queues regeneration.

## 14. Vector storage and retrieval

Initially store vectors directly in SQLite as compact blobs. Do not
introduce a dedicated vector database.

A simple implementation may use FTS/metadata to obtain candidates and
calculate cosine similarity in Go for reranking. If profiling later
shows this is insufficient, introduce a SQLite vector extension or local
ANN index behind the same search abstraction.

Optimization follows measurement.

## 15. Hybrid search

Search combines lexical and semantic evidence:

-   FTS5/BM25;
-   embedding similarity;
-   useful metadata signals.

Exact ranking weights are internal and may evolve.

Results should primarily group by document and expose the strongest
matching chunks/passages.

Example:

``` bash
locus search "invoice for the solar panels"
locus search "insurance terms" --path Finance --type pdf --limit 10
```

## 16. CLI

The executable and CLI command are both:

``` bash
locus
```

The CLI is a first-class interface for both humans and AI agents.

The surface:

``` text
locus init [--root [<name>=]<dir>]... [--peer-port <n>] [--discovery-peer <host[:port]>]... [--force]
locus serve

locus status
locus peers

locus sync status
locus sync now

locus search <query>
locus search <query> --json
locus search <query> --context [--budget <chars>]
locus search <query> --path <path>
locus search <query> --type pdf
locus search <query> --limit 10

locus index status
locus index rebuild

locus conflicts
locus conflicts show <id>
locus conflicts resolve <id>

locus skill [--section <heading>]
locus version
```

Global flags are `--config`, `--data-dir` and `--timeout` before the
command, and `--json` and `--quiet` in either position. Exit codes: 0
success, 1 error, 2 usage, 3 no results, 4 configuration problem, 5
unresolved conflicts.

Commands exposing useful data must support stable machine-readable JSON
with no decorative output, documented fields, stable identifiers, source
paths and meaningful exit codes.

`--context` is specifically useful to agents: it returns a compact set
of high-quality passages/chunks plus provenance suitable for insertion
into an LLM context window.

Locus does **not** need an `ask` command or built-in generative LLM.
Locus is the retrieval layer; calling agents provide reasoning and
generation.

## 17. SKILL.md for AI agents

The repository must include a maintained `SKILL.md` as part of the
public interface.

It should teach an unfamiliar agent:

-   what Locus does;
-   when to use it;
-   how to discover CLI commands;
-   how to search;
-   available filters;
-   when to use `--context`;
-   JSON schemas;
-   exit codes;
-   file operations if/when exposed via CLI;
-   conflict behavior;
-   examples;
-   common failure modes;
-   security assumptions.

Core guidance should include the equivalent of:

``` text
When looking for information in the user's documents, prefer:

locus search "<natural language query>" --context --json

Do not recursively open every file unless the search results are insufficient.
Use returned paths and provenance to retrieve only the necessary source material.
```

Every command must also have useful `locus <command> --help`
documentation. The CLI plus `SKILL.md` should be sufficient for an agent
to use Locus without reading its source.

## 18. HTTP API

The PWA uses the same core services as the CLI through a small versioned
HTTP API.

Routes (every `/api/v1` route requires the bearer token):

``` text
GET    /healthz
GET    /api/v1/status
GET    /api/v1/peers
GET    /api/v1/search
GET    /api/v1/index
POST   /api/v1/sync
GET    /api/v1/files
GET    /api/v1/files/content
POST   /api/v1/files
DELETE /api/v1/files
POST   /api/v1/files/move
POST   /api/v1/directories
GET    /api/v1/conflicts
POST   /api/v1/conflicts/resolve
```

Responses that have a CLI counterpart (status, peers, search, index,
sync, conflicts) are the same JSON values as its `--json` output.
Parameters and shapes are documented in `SKILL.md`.

Do not expose the API on public interfaces by default. It listens on
`api.addr` (loopback by default); a peer that serves the PWA to other
devices sets it to its WireGuard address.

## 19. PWA

The PWA is a **real file manager**, not merely a search page.

It should be implemented as a polished mobile-first
JavaScript/TypeScript UI. Next.js with a component system such as
shadcn/ui is a reasonable choice.

For production, prefer a static build that can be embedded into the Go
binary (`go:embed`) and served by Locus. Running Node.js should not be
required on a deployed peer.

### Required PWA capabilities

**Browse** - navigate folders; - list files and directories; -
breadcrumbs; - useful metadata; - responsive list/grid presentation.

**Search** - natural-language/hybrid search; - relevant snippets; -
matched page/section where available; - filters; - direct navigation
from result to file/preview.

**Preview** - preview supported documents where practical; - display
extracted text or native/browser PDF preview where appropriate; -
clearly offer download when preview is unavailable.

**Download** - download any accessible synchronized file from the
connected peer.

**Upload** - upload one or multiple files; - upload into the currently
selected directory; - show progress/errors; - write uploads to the
normal filesystem.

An upload must enter the **normal pipeline**: filesystem state changes,
sync engine notices/reconciles it, local index work is queued, and other
peers eventually receive it. There must not be a separate "web upload
store."

**File management** - create folder; - rename; - move; - delete; -
optionally drag/drop where the browser supports it.

All operations modify the normal filesystem and therefore flow through
ordinary sync and indexing logic.

**Status** Expose useful information without overwhelming users, such
as: - peer connected to; - sync state; - indexing state; - conflicts; -
upload/sync progress.

### PWA behavior on iPhone

An iPhone is **not a full Locus synchronization peer** in the initial
design.

Instead, while connected to WireGuard, the PWA connects to any reachable
Locus peer that exposes the web service. That peer performs filesystem
and search operations.

The UI itself may be installable/cacheable as a PWA, but document
contents should not be silently mirrored into browser storage. Offline
document synchronization on iOS is outside the initial scope.

## 20. Security

Primary assumptions:

-   transport occurs over WireGuard;
-   Locus services bind only to configured/trusted interfaces by
    default;
-   peer handshakes use stable application identities;
-   incoming paths are strictly validated;
-   API file operations are restricted to configured roots;
-   `..`, symlink and path-escape attacks must be explicitly prevented;
-   uploads are size-limited/configurable;
-   temporary transfers are verified before atomic installation.

The PWA/API should additionally have application-level authentication or
peer authorization so possession of generic WireGuard network access
does not automatically grant document access. The API requires a bearer
token (`api.token`, or one generated into the data directory); peers
authenticate with their identity, and `authorized_peers`, when set,
limits which peer IDs are discovered and served (when empty, only peers
this one has discovered are served). `authorized_peers` does not apply to
the API.

Security-sensitive actions and conflicts should be auditable locally.
Refused peer and API requests are counted in memory and written
as one summary row per remote host and reason every `audit.interval`, so
they cannot grow the log one row per request; the log keeps at most
`audit.max_entries` rows, none older than `audit.max_age`.

## 21. Suggested internal Go packages

The current structure (README.md describes each package):

``` text
cmd/locus/

internal/app/
internal/agent/
internal/config/
internal/identity/
internal/discovery/
internal/peer/
internal/sync/
internal/vvector/
internal/reconcile/
internal/fswatch/
internal/pathguard/
internal/store/
internal/jobs/
internal/pipeline/
internal/extract/
internal/chunk/
internal/embed/
internal/search/
internal/api/
internal/web/
```

Do not treat this exact layout as mandatory. Prefer clean interfaces
over excessive package fragmentation.

## 22. MVP implementation phases

### Phase 1 --- Local core

-   configuration and roots;
-   filesystem inventory;
-   SQLite metadata;
-   filesystem watching/reconciliation;
-   FTS5 extraction/search;
-   basic CLI/status.

### Phase 2 --- Peer sync

-   persistent peer identity;
-   WireGuard peer discovery;
-   handshake/presence;
-   manifest comparison;
-   whole-file transfers;
-   deletes/tombstones;
-   version vectors;
-   conflict preservation.

### Phase 3 --- Semantic retrieval

-   document chunking;
-   local embedding runtime;
-   CPU operation;
-   optional automatic GPU acceleration;
-   persistent/coalescing work queue;
-   hybrid search;
-   `--context --json`.

### Phase 4 --- PWA/API

-   embedded static UI;
-   file browsing;
-   search;
-   previews;
-   upload/download;
-   create/rename/move/delete;
-   peer/sync/index/conflict status;
-   iPhone/PWA testing over WireGuard.

### Phase 5 --- Agent interface hardening

-   stable JSON schemas;
-   strong exit-code semantics;
-   complete CLI help;
-   `SKILL.md`;
-   agent-oriented integration tests.

## 23. Critical acceptance scenarios

Before calling Locus reliable, test at least these scenarios:

1.  Two online peers converge after creating/editing/deleting files.
2.  A laptop is offline, files change elsewhere, then it reconnects and
    converges.
3.  Two offline peers edit the same file and both versions survive as a
    conflict.
4.  10,000 files are added at once without making search unusable.
5.  Hundreds of newly added files are immediately deleted; obsolete
    queue work is coalesced/cancelled.
6.  Locus is killed mid-transfer; no partial document becomes visible as
    valid.
7.  Locus is killed mid-indexing; restart safely resumes/reconciles
    work.
8.  The entire search database is deleted; rebuild produces a
    functioning equivalent index.
9.  An embedding model changes; embeddings are safely regenerated
    without affecting sync.
10. A CPU-only machine works correctly.
11. A supported GPU machine accelerates embedding automatically but
    behaves identically semantically.
12. A file uploaded from an iPhone PWA appears as a normal file,
    synchronizes to other peers and becomes searchable.
13. Rename/move/delete from the PWA propagates normally.
14. Attempts to escape a configured root through API paths/symlinks
    fail.
15. An unfamiliar AI agent can read `SKILL.md`, find relevant documents
    with the CLI and consume JSON output without source-code knowledge.

## 24. Explicit non-goals for the first version

Avoid scope creep. The initial Locus design does not require:

-   cloud storage;
-   a central coordination server;
-   public Internet peer discovery;
-   NAT traversal;
-   relay servers;
-   collaborative real-time document editing;
-   automatic merge of conflicting documents;
-   an LLM/chat assistant;
-   a dedicated vector database;
-   block-level binary deltas;
-   iPhone filesystem synchronization;
-   syncing embeddings or indexes between machines.

These can be reconsidered only when a concrete use case justifies them.

## 25. Architectural invariant

The most important invariant for future contributors and agents is:

> **Files are canonical. Synchronization replicates files. Search state
> is local, derived and disposable. Every computer is an equal peer.**

If a proposed feature breaks that property, it should receive deliberate
architectural review rather than being added incidentally.

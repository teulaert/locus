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

Every Locus installation generates a persistent cryptographic peer
identity on first launch. A handshake should include at least
protocol/version, peer ID, hostname, Locus version and capabilities.
WireGuard already secures transport, but the Locus identity gives stable
application identity and protects against accidentally speaking the
protocol to an unrelated service.

Typical presence states are `online`, `offline`, `synchronizing` and
`incompatible`.

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

The hierarchy below each root is synchronized. Locus metadata should not
pollute user directories. Private state belongs in the
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

Example:

``` text
proposal.docx
proposal.conflict-macbook-20260923-095412.docx
```

The initial implementation should not attempt semantic/text
auto-merging.

CLI support should include `locus conflicts`,
`locus conflicts show <id>` and an eventual
`locus conflicts resolve <id> ...`.

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

SQLite is the default local storage engine. Conceptual tables include:

``` text
documents
chunks
embeddings
fts_chunks
jobs
conflicts
local_state
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

Suggested surface:

``` text
locus status
locus peers

locus sync status
locus sync now

locus search <query>
locus search <query> --json
locus search <query> --context
locus search <query> --path <path>
locus search <query> --type pdf
locus search <query> --limit 10

locus index status
locus index rebuild

locus conflicts
locus conflicts show <id>
locus conflicts resolve <id>

locus serve
```

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

Likely capabilities:

``` text
GET    /api/v1/status
GET    /api/v1/peers
GET    /api/v1/search
GET    /api/v1/files
GET    /api/v1/files/content
POST   /api/v1/files
DELETE /api/v1/files
POST   /api/v1/files/move
POST   /api/v1/directories
GET    /api/v1/conflicts
```

Exact routes are not fixed by this document. Define a coherent versioned
API before implementation.

Do not expose the API on public interfaces by default. It is intended to
be reachable over the trusted WireGuard network.

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
does not automatically grant document access.

Security-sensitive actions and conflicts should be auditable locally.

## 21. Suggested internal Go packages

A possible structure:

``` text
cmd/locus/

internal/config/
internal/identity/
internal/discovery/
internal/peer/
internal/sync/
internal/fswatch/
internal/version/
internal/store/
internal/jobs/
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

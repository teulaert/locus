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

### Memory

An idle peer is small: 45--70 MB resident, most of it the Go runtime,
SQLite's page caches and the watcher. Nothing is held per document or per
peer between rounds, so this does not grow with the size of the roots.

During a bulk transfer and indexing the process grows with what is in
flight, not with what has been moved (LOC-84):

-   a transfer streams through fixed-size buffers into a staging file
    (sections 6, "Receive first, lock later"), so it costs the same for a
    1 KB note and a 1 GB video; a round holds the manifests of the roots it
    is syncing with each peer (a few hundred bytes per file);
-   each index worker (`workers`, 2 by default) holds one document at a
    time: its extracted text, at most 16 MB (section 11), the chunks cut
    from it and their vectors, a small multiple of the text. A large PDF
    of scanned pages costs almost nothing, as its pages carry no text; a
    long report costs its text, not its file size;
-   the Go garbage collector lets the heap reach twice its live size
    before collecting (GOGC=100).

So the peak heap is about 2 (GC) × workers × 2 copies × the largest
document text, plus some tens of MB: the acceptance scenario
`TestBulkPullAndIndexHeapStaysBounded` (≈500 files, 300 MB, three 40 MB
scans, three 1500-page reports and a 60 MB CSV) peaks at about 90 MB of
heap and 140 MB resident for both peers in one process.

The daemon sets a soft limit on the Go heap, `memory_limit` (default 25%
of the memory the machine, or the cgroup the daemon runs in, has, and at
least 256 MiB; `"off"`, a percentage or a size such as `"1GiB"`). Near it
the collector works harder and returns memory to the system sooner;
nothing fails for reaching it. `GOMEMLIMIT` in the environment overrides
it.

A service manager's figure is larger than the process: systemd's "memory
peak" for a unit is the cgroup's, which also counts the page cache of every
file the daemon wrote, hashed or extracted. A bulk pull of several GB
therefore reports a peak of several GB (the observed 4.5 GB during a 6.2 GB
pull), almost all of it clean cache the kernel reclaims under pressure. The
same scenario run under `systemd-run` reports a 1 GB peak, of which 190 MB
is the process, test harness included (`anon` in the unit's `memory.stat`),
and 850 MB page cache.
To see what the process itself holds on a live peer, read `anon` there, or
take a heap profile from `/debug/pprof/heap` on the local API (section 18).

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

The WireGuard configuration itself (`wg show <iface> dump`) only names
peers on a full mesh, where each peer is a /32 (or /128) allowed-ip, and
reading it needs `CAP_NET_ADMIN`. Locus therefore also derives targets from
the routing table, which any user can read (`/proc/net/route` on Linux,
`netstat -rn -f inet` on macOS), every probe round:

* An interface is a WireGuard interface when `peer.interface` or
  `LOCUS_WG_INTERFACE` names it (by name, or a CIDR one of its addresses
  lies in), when it is named `wg*` or `utun*`, or when it is up,
  point-to-point and has no link-layer address (WireGuard on Linux is
  `POINTOPOINT,NOARP`, `link/none`, whatever the interface is called).
* Every IPv4 route through such an interface whose destination lies in
  RFC 1918 space or `100.64.0.0/10` yields **one** target on
  `discovery.port`: the route's gateway when it has one (not the machine's
  own address), else the first usable address of the prefix (network + 1;
  the network address of a /31). Default routes, host routes (/32, left to
  `wg show`), link-local, loopback, multicast and public destinations yield
  nothing, nor does any of the machine's own addresses. Targets are
  deduplicated: a client with `10.42.0.3/24` and `10.42.0.0/16` on its
  tunnel probes `10.42.0.1` once.
* A node with no WireGuard interface of its own, such as a container on
  the hub, probes its **default gateway** when that is a private address
  (LOC-79): for a container on the hub the gateway is the hub's bridge
  address, where the rendezvous peer answers. Public default gateways are
  never probed. On a network whose default gateway is not a Locus peer
  this costs one signed probe per interval to the LAN router, which does
  not answer; `discovery.gateways: false` turns all route-derived probing
  off.

**Invariant: one address per route, never a scan.** No target is ever
derived by enumerating a prefix; everything else is learned from signed
peer lists. On a hub-and-spoke network that one address is the hub, which
runs a **rendezvous peer** (`"rendezvous": true`): a discovery-only peer
with no roots. It runs the discovery responder and prober and the address
book with peer exchange (persisted in `state.db`), and no sync service,
reconciler, index workers or watcher; the local API answers `status` and
`peers`, and commands that need roots refuse with "rendezvous peer: no
roots". It announces `rendezvous`, `peer-exchange` and its discovery port
but not `sync`, and a peer that does not announce `sync` is shown as
`rendezvous` and never chosen for a synchronization round. Clients then
need no `discovery.peers`: they find the hub through their routes and each
other through the hub.

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

Learned addresses are kept clean (LOC-80). A peer never records itself: a
reply or an inbound probe carrying its own peer ID is ignored, and a learned
address that answers as this peer is forgotten, since it is one of ours. No
address of this machine's own interfaces is ever learned, whatever peer ID
claims it, and loopback addresses are learned (or shown in presence for an
inbound probe) only when a configured target is on loopback or the sync
listener is bound to loopback, as in the test harness. When a learned
address answers as a different peer than the entry names, the entry is
dropped and only the peer that answered is recorded; while an address is
known from a direct reply, hearsay naming another peer there is ignored. A
learned peer that never answered is forgotten on its first failed probe, and
one that did answer once its probes have failed for more than an hour
(`discovery.LearnedFailureLimit`); it leaves the address book, `state.db` and
presence. Configured targets keep being retried. On startup, persisted
entries on loopback (without loopback targets) or at an own address are
dropped from `state.db`, which heals a database polluted by older builds that
still re-share such entries.

Every Locus installation generates a persistent cryptographic peer
identity on first launch. A handshake should include at least
protocol/version, peer ID, hostname, Locus version and capabilities.
WireGuard already secures transport, but the Locus identity gives stable
application identity and protects against accidentally speaking the
protocol to an unrelated service.

Typical presence states are `online`, `offline`, `synchronizing`,
`incompatible` and `rendezvous` (a reachable discovery-only peer).

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
opts in with `sync_hidden`. Hidden directories are not walked or watched
(see §8 for how roots are watched).

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
create, update, delete, conflict and no-op. A rename is represented as
delete + create; the receiving side recognizes the content by hash and
produces the new file from its own copy instead of transferring it
("Renames: adopting content held locally" below).

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
GET  /v1/put/status?hash=[&rootname=&relpath=]   {"hash", "offset", "held", "pending"}: bytes held of an interrupted put
POST /v1/delete?rootname=&relpath=&hash=&version=&writer=
```

Put and delete answer `{"result": "applied" | "noop" | "conflicted" |
"rejected"}`, with the conflict record that request created when the
receiver detected one (so both peers record it) and an `error` when
rejected. A put answered without its body carries `"held": true` or
`"pending": true` (see the in-flight claims below), and `"held": true,
"adopted": true` when the content was produced from a copy under another
path (see renames below). Refusals are HTTP
statuses, checked in this order: 401 (missing or bad signature, timestamp
outside a five-minute window), 403 (peer not in `authorized_peers`, or, when
that list is empty, not yet discovered), 426 (another sync protocol), then
404 (no root of that name), 400 (bad path or version) and 413 (larger than
`upload_max_bytes`). The client treats only a dial failure, a dropped
connection or a 5xx as a transport failure that backs the peer off; a 426
marks the peer incompatible; any other status is an answer for that root
alone, and the round goes on with the next root and peer.

A round works with its peers concurrently, one goroutine per peer running
that peer's roots in turn, and a peer is in at most one round at a time.
Each sync tick starts a round with the online peers that are neither backed
off nor still held by an earlier round, so a transfer that takes hours with
one peer does not delay retries, manifest exchanges or status of the others.
While a round runs, the daemon reports per peer and root the actions planned
and done, the bytes moved, the current file and a ten-second transfer rate
(`GET /api/v1/sync`, `locus sync status`); the part of a resumed put the
peer already held counts as moved, but not towards the rate. Content a
round handles without sending it (pushes the peer already holds or is
receiving from another peer, pulls another round is fetching) counts as
handled: it is left out of the bytes to send, the rate and the ETA, and the
summary names it (`pushing to hermes: 0 B sent, 4.8 GB already held there
(1150/1152 files)`). A round is stalled only when neither a byte moved nor
an action finished for ten seconds. A transport failure recorded for a peer
is cleared as soon as the peer serves a manifest in a later round, not when
that round ends. On shutdown every running round is drained together (see
below) before it is cancelled.

Each peer's part plans against the local manifest as it was when that part
began, so peers holding the same files would each plan to send all of them
to a peer that has none, and a joining peer would download its whole root
once per online peer. Pulls are therefore claimed first: before fetching a
file the round claims it in the engine's in-flight set, keyed by root, path
and content hash. A round that finds the claim held by another peer's round
skips the pull, as it does one whose content the state database already
records as live (another round applied it after this plan was made, and
only the version vector is adopted, as for `pull_version`). Skipped pulls
are neither failures nor changes; they are counted (`skipped_in_flight`,
`sync status`: `skipped 412 (in flight from other peers)`) and the next
round plans them again against what arrived. A claim is held only for the
duration of its pull and is released however the pull ends, so a pull that
fails or is cancelled leaves the file to any other round, and the set is
empty whenever no round runs. Whichever round reaches a file first takes it;
since every part works through its plan smallest first, parallel parts with
the same files split them between them. Deletions and
`pull_version` are not claimed, and pulls are never resumed (only puts are,
below), so a claim cannot stand in the way of resuming a transfer.

The same goes for pushes, from the other side: every peer that holds a file
a joining peer lacks plans a push of it in its own round, so with N seeds
online the joiner would be offered N copies of each file on top of what it
pulls. The receiver therefore answers a put early, before reading a byte of
its body, when the content is already here or already on its way:

- *Held*: the state database records the path as live with the declared
  hash and the file on disk still holds it. The put's version is applied
  as receiving those identical bytes would have (a descendant vector is
  recorded, a concurrent one merged with ours as for `pull_version`, an
  older or equal one changes nothing), and the answer is the usual reply
  with `"held": true`, e.g. `{"result": "applied", "held": true}`.
- *Pending*: a put of the same root, path and hash from another peer is
  being received, or this peer's own round is pulling it (the LOC-77
  claim). Nothing is applied; the answer is `{"result": "noop", "pending":
  true}`, and the pusher plans the file again next round against what
  arrived.

A put being received claims its (root, path, hash) in the same in-flight
set as pulls, released when the request ends however it ends, so a pull
planned meanwhile skips it too. The early answer comes after the signature,
authorization, protocol, root, path, hidden-path and size checks, exactly
where the body would otherwise be read; different content (a real other
version) is received and applied as before, conflicts included. Both
answers are HTTP 200 with fields an earlier build ignores: it reads held as
the result it names and pending as a noop, so the wire protocol version is
unchanged.

To keep the body off the link, a pusher sends every put with `Expect:
100-continue`: Go's client then waits (up to a second) for the server's
`100 Continue`, which the server sends only once the handler starts reading
the body. An early answer never reads it, so the body is never sent and the
connection is closed after the reply. A pusher that sends the body with the
request regardless (builds before LOC-78 did so below 1 MiB) has it read
and discarded before the early answer, so it is not cut off mid-request. A
large push also learns it from `GET /v1/put/status`, which now carries the
path: `pending` there skips the put altogether, `held` sends it from offset
0 only to have the version applied. The pusher counts these pushes as
neither pushed nor failed: `skipped_held` and `skipped_receiving` in the
round's statistics and per root in its progress (`sync status`: `skipped 3
pushes (peer already holds them), 1 push (peer receiving them from another
peer)`).

The checks before the body must therefore be prompt (LOC-83). They take an
in-memory claim and read the path's record through the state database's
read pool (section 10), never its writer connection: a receiver busy
recording a reconciliation pass or other transfers would otherwise hold
every put at its first record read, and past the client's one-second
`Expect` wait the pusher sends the body anyway, so the early answer would
save nothing. Applying a held put's version reads the record again under
the path's lock, where it is authoritative. A put whose checks take longer
than a second is logged (`put waited long before reading its body`), and
each received put logs its check, receive and apply times at debug level.
Peer requests go through a transport of their own (`sync.NewTransport`):
no HTTP proxy from the environment, TCP keepalives, a one-second `Expect`
wait and up to 8 idle connections per peer, where Go's default transport
keeps two. A put answered early still closes its connection, as HTTP
requires for a body announced and not read.

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
receiver never sees an `offset` it did not report. Within a round, renames
run first (below), then the actions that move no content, and transfers
follow smallest first, so an interrupted round has delivered the small
files; on shutdown a round finishes the transfer in progress for up to five
seconds and starts no other before it is cancelled.

### Renames: adopting content held locally

A folder renamed or moved on one peer reaches every other peer as a
tombstone for each old path and a creation for each new one (LOC-82).
Those peers hold every byte already, under the old paths, and transferring
them again is what made renaming a 1196-file folder cost hours over a slow
uplink. An incoming file whose content this peer holds live under another
path is therefore *adopted* instead of transferred:

-   **Lookup.** `state.db` has a partial index on `docs (hash, root) WHERE
    deleted = 0`; `FindLiveByHash` returns the live records with a content
    hash. The candidates are the file the plan is about to delete (below),
    then other paths in the same root, then other configured roots, at
    most four. An index lookup costs the same for a pull and for a put,
    which arrives without a manifest at hand, and covers every root, so it
    was chosen over a hash map built from the manifest each round.
-   **Production.** The candidate is staged as a `.locus-tmp-*` file beside
    the destination and verified like received content: size first, then
    the whole file is hashed again, since the copy may have changed since it
    was recorded. A candidate that fails is passed over; when none verifies,
    the file is transferred as ever. The staged file then goes through Apply
    exactly as received bytes would (the path lock, the version decision,
    conflicts, `pending_arrivals`, the atomic rename and flushes), recorded
    with the incoming version vector and writer, so the record is the one a
    transfer would have written.
-   **Hard link or copy.** The stat fingerprint the reconciler keeps
    (size, mtime, ctime, inode) is only a shortcut to skip hashing: a
    link changes the inode's ctime, so both paths are simply hashed again,
    and nothing is confused. The real hazard of a link is two documents
    sharing one file, where an in-place edit of either silently changes
    both. A link is therefore used only for the rename case, for one pull
    per deleted file, when the deletion runs immediately after and leaves
    the link as the file's only name; if that deletion does not remove the
    old file (a local edit made meanwhile is preserved as a conflict), the
    new path is given a copy of its own. Every other adoption, and any
    link the filesystem refuses, copies at local disk speed.
-   **Order within a round.** `Schedule` puts each creation (pull or push)
    whose content a deletion in the same plan removes right before that
    deletion: a pull adopts from the still-present old file, and a push
    lets the peer do the same with its copy. A deletion whose paired
    creation is not in place when its turn comes (failed, left to another
    round that holds the claim, or answered `pending` because the content
    is on its way to the peer) waits for the next round, so the old file
    is not removed from under an adoption in progress. The emptied old
    directories are then pruned as before (LOC-74). The rest of the plan
    keeps its order: moves of no content first, transfers smallest first.
-   **Pushes.** The receiver of a put whose content it does not hold at the
    path, and no other transfer brings, looks the hash up after the
    signature, authorization, protocol, root, path, hidden-path, size and
    claim checks, and adopts a local copy the same way. The answer comes
    before the body is read (`Expect: 100-continue`, as for held content):
    the usual reply plus `"held": true, "adopted": true`. A pusher of an
    earlier build reads it as held. Only files recorded live in configured
    roots are sources, and every authorized peer can read those roots'
    manifests and files already, so adoption tells a peer nothing it could
    not learn otherwise; hidden files of a root that does not sync them
    are never recorded live, so they are never sources.
-   **Reporting.** Adopted files are neither pulled nor pushed. A round's
    statistics count them as `adopted` and `adopted_bytes`, and so does the
    progress per root and per peer (with `adopted_push_bytes`, the share
    that was planned as pushes); adopted bytes are left out of the bytes
    still to transfer and the ETA. `locus sync now` and `sync status` say
    `adopted 1196 files (content already present locally)`.

The reconciler of the peer that renamed records the new paths before the
tombstones of the old ones (§8), so a peer reading its manifest mid-pass
never sees the deletions without the creations. No protocol change is
involved: a peer of an earlier build transfers the files as before.

Shutdown (SIGTERM, `systemctl restart`) stops every part of the daemon at
once and independently, under one 15-second deadline (LOC-81). The
filesystem watcher stops immediately: the reconciliation pass it may be
running is handed a context its stop cancels, and the scan and hashing
honour it, so the watcher never waits for the sync drain, an HTTP server or
a whole pass. The sync and API servers stop accepting connections and give
requests in flight the same five-second grace as the drain; the requests
still running then are cut short: their contexts are cancelled, which ends
a put body being read at once rather than at its idle deadline, and their
connections are closed. An interrupted put keeps its partial object for
the sender to resume. Should anything still run at the deadline, the
daemon logs `daemon shutdown cut short` at WARN, naming each part and the
requests still in flight, and exits 0 anyway: every write is atomic and
every interrupted transfer resumable, so state is consistent and a restart
under load is not a failure for systemd. `locus serve` exits non-zero only
for real failures, such as a configuration error or the daemon lock being
held by another process.

### Deletions and tombstones

A deletion is recorded as a tombstone carrying the file's version vector,
and travels like an edit. A tombstone is forgotten only when it is older
than `tombstone_age` (30 days by default) *and* predates the start of the
last clean round (every planned action succeeded) with every peer this one
has ever synchronized the root with, as recorded per root and peer. A
long-offline peer therefore still receives the delete instead of
resurrecting its stale copy.

### Directories emptied by sync

Locus tracks files only; a directory exists on a peer because a file in it
does. A folder renamed on one peer therefore arrives elsewhere as a
deletion per file plus a creation per file, and the deletions alone would
leave the old folder behind as an empty skeleton beside the new one
(LOC-74). Two rules remove such directories:

-   **Prune on delete.** When a synced deletion (a pulled delete, or a
    delete pushed to this peer) removes a file, its parent directory is
    removed if it is now empty, then the grandparent, and so on upward,
    stopping at the first directory that is kept and never removing the
    root. A directory already gone is passed over, so an interrupted prune
    is finished by the next deletion below it.
-   **Reconciler cleanup.** Each reconciliation pass removes, deepest
    first, the empty directories under a root below which the state store
    holds at least one tombstone and no live file (§8). This removes
    skeletons left before pruning existed, or by a prune that failed. An
    empty folder the user made has no tombstones below it and is kept. A
    folder whose files were all deleted, here or on a peer, is a shell by
    this rule and is removed once empty, even if the user meant to keep
    it.

Both rules use `os.Remove` on a real directory (never a symlink, never
`RemoveAll`), which fails on a directory with any entry, so a directory
that still holds anything, a hidden file such as `.DS_Store` included, is
never removed, no file is ever deleted by pruning, and the root is never
removed. Hidden directories are neither descended into nor removed unless
the root has `sync_hidden`. Each removed directory is logged at info level
(`pruned empty directory`, with the root and the relative path). An install
whose directory is pruned between creating it and writing its temporary
file creates it again; once the temporary file exists the directory is not
empty and cannot be pruned.

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

The sibling's name is a pure function of the losing version: the first
eight characters of its writer's peer ID and the first eight hex digits of
its content hash, data every peer holding the conflict has. Peers that
detect the same conflict independently therefore write the same sibling:

``` text
proposal.docx
proposal.conflict-30f2593b-1a2b3c4d.docx
```

It never uses the writer's hostname. Whether a peer has discovered that
hostname depends on timing, so two peers could name the same sibling
differently and then sync both copies to each other. Siblings written by
earlier builds, which used the hostname when it was known, keep their
names.

A delete concurrent with an edit is a conflict too, but the edit always
survives on the normal path; the conflict is recorded without a sibling.

A conflict record's ID is `<root name>:<relpath>:<first eight hex digits
of the normal-path hash>:<first eight of the other hash>`, using the name
peers pair the root by, never its local path, so every peer that records a
conflict gives it the same ID. A conflict *is* its root, path and the two
full hashes: state.db keeps at most one record per such key, so the same
conflict detected here and reported back by a peer (whatever ID that peer
gave it) is listed once. Records written before this rule keep their old
ID; duplicates among them were folded into the oldest record by a state.db
migration. The reply to a put or delete carries the conflict that request
recorded, which the pusher adopts under the ID it derives itself.

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

A pass records new and changed files before it records files gone from
disk as deleted, so a folder renamed here never shows a peer the
tombstones of its old paths without the creations of the new ones; that
peer would remove its copies before learning where the content moved, and
transfer it all again instead of adopting it (§6, LOC-82).

A pass also removes the empty directories sync emptied (§6, "Directories
emptied by sync"). It needs no query of its own: from the records the pass
already read (one `ListDocs` of the root, tombstones included) it collects
every ancestor directory of a tombstone and every ancestor of a live
record; the candidates are the first set minus the second, filtered by the
hidden-path rule. Each candidate costs one `lstat` (most are long gone) and
at most one `rmdir`, tried deepest first so a parent is tried after its
emptied children. The pass reports the count as `dirs_pruned`.

### Watch modes

The watcher registers directories only, never files, and skips hidden
directories (unless the root has `sync_hidden`) and `.locus-tmp-*` transfer
temporaries; events for hidden paths and temporaries are ignored. Its
events only trigger a reconciliation pass (debounced); the periodic pass
(`reconcile_interval`, default 30s) runs regardless. The mode in use is
reported by `locus status` (`watch` line, `watch` object in the JSON):

-   `inotify` (Linux): one inotify watch per directory, no file descriptor
    per watch. Creations, deletions, renames and in-place writes are all
    reported within the debounce.
-   `kqueue-dirs` (macOS and the BSDs): kqueue can only watch a vnode
    through an open descriptor of it. fsnotify's kqueue backend opens every
    file inside a watched directory to report writes the way inotify does,
    so descriptors grow with the number of files and a large root hits
    `kern.maxfilesperproc` (LOC-70); FSEvents has no such cost but needs
    cgo, which Locus does not use. Locus therefore uses its own kqueue
    backend that registers each directory only, so descriptors scale with
    the number of directories. A directory reports entries created, removed
    or renamed in it, which covers new files, deletions and the
    write-temporary-then-rename saves most editors and sync tools perform.
    The trade-off: a write to an existing file in place (an append, or an
    editor that truncates and rewrites) is not reported, and is found by
    the periodic reconciliation (size, mtime and ctime fingerprint) within
    `reconcile_interval` instead of within the debounce.
-   `poll`: nothing is watched and the periodic reconciliation alone finds
    changes. The watcher falls back to it, for good, when the kernel
    refuses a watch for lack of resources (`EMFILE`, `ENFILE`, or inotify's
    `ENOSPC` when `fs.inotify.max_user_watches` is reached), when no
    watcher can be created, or when a descriptor-holding backend would
    exceed half the soft `RLIMIT_NOFILE` (a warning is logged at a
    quarter). It then releases every watch, so the rest of the daemon
    (sockets, SQLite, transfers) keeps its descriptors. A watcher failure
    never stops the daemon; restarting it tries watching again.

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
state.db   docs (records, version vectors, tombstones; live records indexed
           by content hash), conflicts, audit, peer_rounds,
           pending_arrivals, meta
index.db   documents, chunks, fts_chunks, embeddings, jobs, meta
```

Keep synchronization correctness separate from disposable search state.
`locus index rebuild` must be able to reconstruct derived indexes
entirely from local files.

Both databases run in WAL mode, each with one writer connection (SQLite
serializes writers anyway; one connection avoids lock-upgrade failures)
and a pool of query-only reader connections: `Store.Read` for interactive
search over index.db, `Store.StateRead` for state.db. The rule for state.db
(LOC-83): **reads go through the read pool, the writer connection is only
for writes**. A reader in WAL mode sees the last committed state and never
waits for the writer, while anything queued on the writer connection waits
for every write ahead of it, and on a loaded peer (a reconciliation pass
recording thousands of files, transfers from several peers, index workers
looking records up) that queue is what made puts crawl. A write is
committed when its call returns and every read starts a fresh snapshot, so
a read made after a write sees it; a decision that must not race a write
(sync's Apply, the reconciler's record writes) holds the path's lock and
reads under it. Writes stay short: the reconciler and sync write one record
per transaction (a received file's record and the removal of its pending
provenance commit together), and the indexer writes one document per
transaction in index.db, so no transaction holds a writer for more than a
statement or two and a commit. Each sync round logs, at debug level,
how often and how long anything waited for the state.db writer.

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

Extraction is bounded so an index worker's memory does not follow file
size (LOC-84). PDFs and DOCX files over 64 MB (`extract.MaxBytes`) are not
parsed. At most 16 MB of text (`extract.MaxTextBytes`) is taken from any
file: plain text, Markdown and HTML are read only that far, DOCX XML is
decoded as it is decompressed and PDF extraction stops there; the leading
part is indexed and the document is logged as truncated. A PDF's page tree
is walked once (the parser library's own page lookup searches the tree from
its root for every page, which is quadratic in the page count).

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

locus update [--check | --rollback]

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
GET    /api/v1/sync
POST   /api/v1/sync
GET    /api/v1/files
GET    /api/v1/files/content
POST   /api/v1/files
DELETE /api/v1/files
POST   /api/v1/files/move
POST   /api/v1/directories
GET    /api/v1/conflicts
POST   /api/v1/conflicts/resolve
POST   /api/v1/update
GET    /debug/pprof/...
```

Responses that have a CLI counterpart (status, peers, search, index,
sync, conflicts, update) are the same JSON values as its `--json` output.
Parameters and shapes are documented in `SKILL.md`.

Do not expose the API on public interfaces by default. It listens on
`api.addr` (loopback by default); a peer that serves the PWA to other
devices sets it to its WireGuard address.

`/debug/pprof/` serves the Go runtime's profiles (`heap`, `allocs`,
`goroutine`, `profile`, `trace`, ...) so memory and CPU can be measured on
a live peer. Like `/api/v1` it needs the bearer token, and it answers only
clients on a loopback address (403 otherwise), whatever `api.addr` is.

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

### Releases and updates

Peers run binaries built by CI only, and update themselves from CI's
releases (LOC-86). The trust model:

-   **Only CI-signed manifests are ever installed.** A release consists
    of the binaries, `manifest.json` (version, commit, commit time,
    channel, and every binary's OS, architecture, file name, size and
    SHA-256) and `manifest.sig`, an ed25519 signature (base64) over the
    exact bytes of `manifest.json`. The public key is a constant compiled
    into every binary (`internal/update.ReleasePublicKey`); the private
    key exists only on the self-hosted runner that signs releases (or as
    a repository secret). A binary is installed only after the signature
    verifies with the running binary's key, its channel matches
    `update.channel`, and the download matches the signed size and
    SHA-256. A build with no key compiled in verifies nothing and never
    updates.
-   **Peers never author binaries.** Updates come from the release
    source (GitHub, or a mirror at `update.url`) and nothing else; peers
    do not offer, pass on or accept binaries from each other, and no peer
    request can trigger an update. The local API (token, loopback by
    default) can, as `locus update` does.
-   **No downgrades.** The manifest's commit time orders builds: a
    release is installed only over an older commit, so an old signed
    release served again is ignored. Going back takes an explicit
    `locus update --rollback`, which restores `<binary>.previous`.
-   **The source is CI, not a developer machine.** Releases are made by
    the `release` workflow on pushes to `main` (channel `main`, prerelease
    `build-<sha7>`) and on `v*` tags (channel `stable`), after the checks
    pass, on a self-hosted runner that pull requests never reach. The
    signing tool (`cmd/locus-release`) is a separate binary, so the daemon
    carries no signing code.

Installing: the binary is downloaded to `<exe>.new` next to the running
one (symlinks resolved), verified and fsynced; the running binary is
hard-linked (or copied) to `<exe>.previous` and `<exe>.new` renamed over
`<exe>`, so the path always holds a complete binary. The daemon then runs
its normal shutdown (§3, LOC-81) and re-executes the new binary with the
same arguments and environment in the same process (`execve`), which
systemd and launchd see as the same service process; where that is
impossible it exits 0 for the service manager to start the new binary.
A health marker in the data directory counts the starts of a newly
installed binary until it reaches serving (all listeners bound, the
watcher running); a binary that fails to get there three starts in a row
is swapped back for `<exe>.previous` on the next start, logged at WARN,
and not installed again automatically.

Rollout: with `update.auto` the daemon checks every `update.interval`
(conditional requests with the release list's ETag, so an unchanged list
does not count against GitHub's 60 requests an hour; rate limits are
honoured from `Retry-After` and `X-RateLimit-Reset`), and installs a new
build after a random delay of up to ten minutes; a rendezvous peer waits
one interval more, so the hub is never first. With `auto` off it only
reports a newer build in `locus status`.

## 21. Suggested internal Go packages

The current structure (README.md describes each package):

``` text
cmd/locus/

internal/app/
internal/agent/
internal/config/
internal/memlimit/
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
internal/update/
internal/release/      (CI only, with cmd/locus-release/)
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

# Troubleshooting

* **A peer is `online` but nothing syncs.** Reachability is one-directional:
  it answers discovery probes but cannot be dialed for transfer. Such a peer is
  reported with `reachable: false`, a `failures` count and the `last_error` in
  `locus peers` / `locus status`, the daemon logs `peer unreachable for
  transfer`, and retries back off exponentially up to five minutes. Usually the
  sync port is not allowed inbound on that machine. The note says when the
  last attempt failed and when the next one is due, e.g. `[transfer failed
  32m ago (connection refused); next attempt in 12s]`. Each peer is
  synchronized on its own, so a long transfer to one peer does not hold back
  retries with another.
* **A large transfer seems stuck.** `locus sync status` lists every peer a
  round is working with: per root the actions planned and done, files and
  bytes pushed and pulled, the file moving now, the rate over the last ten
  seconds and an ETA. `locus status` summarises it as e.g. `sync    pushing
  1.2 GB / 5.6 GB to MacBook (921.6 KB/s, ~1h20m)`; content the peer already
  holds is not sent and is named instead (`pushing to hermes: 0 B sent,
  4.8 GB already held there (1150/1152 files)`). `stalled` means neither a
  byte moved nor a file was done for ten seconds.
* **Changes on a Mac show up only after a while.** `locus status` prints how
  the roots are watched, e.g. `watch   kqueue-dirs, 412 directories`. On
  macOS (and the BSDs) only directories are watched, so files created,
  deleted or renamed (including editors' save-by-rename) are seen at once,
  but a file rewritten in place is found by the periodic reconciliation,
  within `reconcile_interval` (30s by default). On Linux, inotify reports
  in-place writes too. `watch   poll (...)` means the daemon gave up
  watching, with the reason: the system refused more watches or
  descriptors (on Linux raise `fs.inotify.max_user_watches`), or the
  directories would need more than half the process's descriptor limit. It
  keeps working on the periodic reconciliation alone; restart it after
  raising the limit to watch again.
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
* **systemd reports a memory peak of several GB.** The unit's "memory peak"
  counts the page cache of every file the daemon wrote and read, so a bulk
  pull of several GB shows a peak of several GB; the kernel reclaims that
  cache under pressure. The daemon itself is the `anon` line of
  `/sys/fs/cgroup/system.slice/locus.service/memory.stat` (a few hundred MB
  at most while indexing, 50 MB or so idle). Its heap is capped softly by
  `memory_limit`, and `/debug/pprof/heap` on the local API shows what it
  holds. See "Memory" in [LOCUS_DESIGN.md](../LOCUS_DESIGN.md).
* **Upgrades change embeddings.** Old embeddings are marked stale and
  regenerated in the background; synchronization is never blocked by indexing.

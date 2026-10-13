# Development

```bash
make check        # make lint + unit + acceptance
make lint         # vet + gofmt + documentation drift + go mod tidy + staticcheck
make test         # tests only
make test-short   # skips the slow 10k-document scenario
make skill        # regenerate SKILL.md from the embedded copy
make dist         # cross-compile the release binaries into dist/
```

The acceptance suite in `tests/` runs real peers against real filesystems and
real HTTP — no mocks — and is described in [TESTING.md](../TESTING.md).
[LOCUS_DESIGN.md](../LOCUS_DESIGN.md) is the authoritative specification;
code comments say *why*, the design document says *what*.

| Package | Responsibility |
| --- | --- |
| `cmd/locus` | the CLI: flags, output and exit codes |
| `internal/app` | wires presence, sync, indexing and API together; runs the loops |
| `internal/agent` | the agent documentation (`SKILL.md`) embedded in the binary |
| `internal/config` | configuration file, defaults, resolution |
| `internal/memlimit` | the daemon's soft Go memory limit (`memory_limit`, `GOMEMLIMIT`) |
| `internal/identity` | per-install ed25519 identity and request signing |
| `internal/discovery` | WireGuard-derived discovery, probing and peer exchange |
| `internal/peer` | presence table: state, reachability, expiry |
| `internal/sync` | manifest planning, transfer protocol, apply and conflicts |
| `internal/reconcile` | filesystem ↔ state reconciliation, tombstone pruning |
| `internal/fswatch` | inventory scans, hashing, temporary-file sweeping, directory watching (inotify, kqueue-dirs, poll fallback) |
| `internal/store` | `state.db` and `index.db` schemas and queries |
| `internal/vvector` | version vectors |
| `internal/extract` · `internal/chunk` · `internal/embed` | text extraction, chunking, vectors |
| `internal/jobs` · `internal/pipeline` | durable work queue and index pipeline |
| `internal/search` | hybrid retrieval and reranking |
| `internal/pathguard` | strict containment of paths inside roots |
| `internal/api` · `internal/web` | local HTTP API and browser UI |
| `internal/update` | release manifests and their verification, checking GitHub, swap, re-exec, rollback |
| `internal/release` · `cmd/locus-release` | CI side of releases: key generation, manifest, signing (never linked into `locus`) |
| `internal/testutil/synthpdf` | large synthetic PDFs for tests |

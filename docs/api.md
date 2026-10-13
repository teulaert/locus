# Search and the HTTP API

## Searching

```bash
locus search "grid tariff"                  # hybrid lexical + semantic
locus search "grid tariff" --json           # stable JSON with provenance
locus search "grid tariff" --context        # compact passages for an LLM prompt
locus search "invoice" --path Finance/ --type pdf --limit 5
```

`--budget <chars>` bounds `--context` output (default 8000). Exit code 3 means
*no evidence found*, not failure. `locus skill` prints the agent-facing
documentation embedded in the binary, including every JSON shape.

## HTTP API and browser UI

`locus serve` listens on `api.addr`:`api.port` (default `127.0.0.1:8910`) and
serves a browser UI at `/`. All `/api/v1` routes require the bearer token: the
configured `api.token`, or the auto-generated one in `<data_dir>/api_token`.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/healthz` | liveness, no auth |
| GET | `/api/v1/status` | same shape as `locus status --json` |
| GET | `/api/v1/peers` | same shape as `locus peers --json` |
| GET | `/api/v1/search?q=` | as `locus search --json` (`&path=`, `&type=`, `&limit=`, `&root=`; `&context=1&budget=` as `--context`) |
| GET | `/api/v1/files?path=` | list a directory |
| GET | `/api/v1/files/content?path=` | download a file |
| POST | `/api/v1/files?path=` | upload; the body is the content (`path` ending in `/` takes `&name=`) |
| POST | `/api/v1/files/move` | rename/move, `{"from", "to"}` |
| DELETE | `/api/v1/files?path=` | delete |
| POST | `/api/v1/directories` | create a directory, `{"path"}` |
| GET | `/api/v1/conflicts` | unresolved conflicts |
| POST | `/api/v1/conflicts/resolve` | accept the normal-path version, `{"id"}` |
| GET | `/api/v1/sync` | running rounds per peer and root, as `locus sync status --json` |
| POST | `/api/v1/sync` | reconcile and sync now, as `locus sync now --json` |
| GET | `/api/v1/index` | as `locus index status --json` |
| GET | `/debug/pprof/` | Go runtime profiles (`heap`, `allocs`, `goroutine`, `profile`, `trace`); loopback clients only |

File routes act on the first configured root unless given `root=` (or
`"root"` in a JSON body) with another root's absolute path. By default the
API is loopback-only and not reachable from other machines; to use the PWA
from a phone over WireGuard, set `api.addr` to this peer's WireGuard address.

```bash
TOKEN=$(cat ~/.local/share/locus/api_token)
curl -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:8910/api/v1/search?q=grid"
```

The profiles under `/debug/pprof/` need the token too, and are served only
to a client on the same machine (loopback), whatever `api.addr` is. To see
what a running daemon holds:

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8910/debug/pprof/heap > heap.pprof
go tool pprof -sample_index=inuse_space -top heap.pprof
```

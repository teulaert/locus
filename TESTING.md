# Testing Locus

Two suites, one command:

```bash
make test      # everything (unit + acceptance)
make test-short  # skips the 10k-document indexing scenario
make test-race   # short suite under -race (needs cgo and a C compiler)
make lint      # vet + gofmt + documentation drift + go mod tidy + staticcheck (pinned, via go run)
make check     # make lint + tests
```

`go test ./...` works too; the acceptance package is `locus/tests`.

## Layout

| Place | What lives there |
| --- | --- |
| `internal/<pkg>/*_test.go` | unit tests for one package's rules |
| `tests/harness_test.go` | the cluster harness shared by acceptance tests |
| `tests/sync_scenarios_test.go` | convergence, offline peers, conflicts |
| `tests/sync_edge_cases_test.go` | three-way conflicts, delete-vs-edit, renames |
| `tests/sync_server_test.go` | the peer protocol over a node's real sync port |
| `tests/sync_protocol_test.go` | sync protocol negotiation between current peers |
| `tests/sync_address_test.go` | syncing when a peer advertises an unroutable sync address |
| `tests/named_roots_test.go` | roots paired by configured name across different folders |
| `tests/peer_authorization_test.go` | `authorized_peers` and serving only discovered peers |
| `tests/peer_exchange_test.go` | clients configured with only the always-on peer find and sync with each other, also once it is down and after a restart |
| `tests/pending_provenance_test.go` | a received file keeps its remote author after a crash |
| `tests/hidden_paths_test.go` | `.git` and other hidden paths stay local; `sync_hidden` opts in |
| `tests/temp_sweep_test.go` | crash leftovers (`.locus-tmp-*`) are swept |
| `tests/index_scenarios_test.go` | extraction, chunking, embedding, search quality |
| `tests/memory_test.go` | a bulk pull with indexing keeps the Go heap within a bound set by the largest document's text (skipped with `-short`) |
| `tests/api_scenarios_test.go` | HTTP API and PWA-facing behaviour |
| `tests/upload_body_test.go` | API uploads whatever the request's content type |
| `tests/agent_cli_test.go` | the CLI as an agent sees it (built binary only) |

## Conventions the acceptance suite follows

**Convergence, not sleeps.** Locus is eventually consistent, so assertions are
`waitFor(t, 30*time.Second, cond, "what we are waiting for")` — never a fixed
`time.Sleep` standing in for correctness. A test that needs a delay to model
the world (e.g. simulating a peer being offline) says why in a comment.

**Real filesystems, real ports, no mocks of the thing under test.** Every node
gets a `t.TempDir()` root and `t.TempDir()` data directory, and listens on
`127.0.0.1` ports obtained from `freePort`. Nothing writes to the machine
running the tests, and tests may run in any order.

**Never the real network.** A test run on a machine whose own Locus daemon is
running must not reach that daemon or its peers (LOC-80). Every make test
target sets `LOCUS_TEST_NO_NETWORK=1`, and the test packages that build peers
set it in `TestMain` as well, so plain `go test` is covered too. Under it
nothing is derived from the routing table or `wg show`, probes go only to
loopback addresses on non-default ports, a discovery responder listens on
loopback only, and the default discovery and sync ports are never bound. The
CLI tests pass it to the binary they run, whose configuration comes from
`locus init` with default ports and gateway discovery on. Test configurations
also set `discovery.gateways: false`.

**Roots share a name, absolute paths differ.** Peers pair roots by name: the
configured `name`, or the root's basename when it has none. `newCluster`
gives every node an unnamed root called `root` in its own temporary
directory, so they pair by basename; `newClusterWithRoots` chooses each
node's folder and configured name, for tests of named roots whose folders
differ. `node.Root` is that node's absolute path.

**Restart means a new App over the same data directory.** `node.stopServing`
cancels the context and closes the databases; `node.restart` builds a fresh
`app.App`, which is what relaunching the binary after rejoining the network
looks like. This is how crash recovery and stale-job resumption are covered.

**The CLI test only uses the executable.** `locusBinary` runs `go build`
once per test binary; `agent_cli_test.go` then drives flags, parses stdout as
JSON and checks exit codes. It never imports an internal package to reach
around the CLI, so it fails if the documented surface drifts.

**Documentation is tested.** The root `SKILL.md` is generated from the copy
embedded in the binary; `TestScenario15` fails if they disagree, if the usage
text mentions a command, subcommand or flag the skill does not document, or
if a documented collection key is ever emitted as `null`.
`TestSkillDocumentsEveryAPIRoute` fails if the API serves a route that the
skill's route list does not name with its method. `make lint` checks the
two copies of `SKILL.md` without running tests.

## What counts as coverage

Synchronisation is only considered working when a test shows all of: content
arriving on a peer, deletions propagating, offline peers converging on
reconnect, interrupted transfers leaving no `.locus-tmp-*` leftovers, and
divergent edits preserving both sides with identical sibling names on both
peers. Divergence is covered beyond the two-peer case: three peers editing
one ancestor keep all three versions, a delete concurrent with an edit keeps
the edit whichever peer acts first, and a rename arrives as the new path
without a conflict (`tests/sync_edge_cases_test.go`). A test that exposes a
known, still open bug is skipped with `t.Skip("LOC-nn: ...")` rather than
deleted, and the skip is removed with the fix.

The peer protocol is covered through the listener a running node opens, not
only through handlers: `tests/sync_server_test.go` drives manifest, file, put
and delete as an authorized peer and shows that unsigned, tampered, forged,
unauthorized, traversal, hash-mismatched and oversized requests change
nothing on disk. Every branch of `Apply` (descendant, stale, divergent,
delete with and without a record, delete-vs-edit) has a unit test in
`internal/sync`. CI runs `make test-race`.

Derived state must be shown to be disposable: a test deletes `index.db` (or
calls `RebuildIndex`) and proves search works again without touching
`state.db` or the files.

Search tests assert *quality*, not just hit counts: the right document ranks
first, the snippet quotes the matched text, chunk provenance names the section
or page, filters exclude what they claim to exclude, and a nonsense query
returns exit 3 with an empty array rather than every document in the index.

## Adding a scenario

1. Give it the number from the acceptance list in `LOCUS_DESIGN.md` §23 and
   name it `TestScenarioNN_<Behaviour>`.
2. Use `newCluster(t, n)`, write files through `node.write`, and end with
   `treesMatch` plus the state the user should be able to observe.
3. Assert the *user-visible* invariant. If a test can only pass by inspecting
   internals, it is a unit test — move it into the package.

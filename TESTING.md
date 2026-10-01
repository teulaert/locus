# Testing Locus

Two suites, one command:

```bash
make test      # everything (unit + acceptance)
make test-short  # skips the 10k-document indexing scenario
make test-race   # short suite under -race (needs cgo and a C compiler)
make check     # vet + gofmt + documentation drift + tests
```

`go test ./...` works too; the acceptance package is `locus/tests`.

## Layout

| Place | What lives there |
| --- | --- |
| `internal/<pkg>/*_test.go` | unit tests for one package's rules |
| `tests/harness_test.go` | the cluster harness shared by acceptance tests |
| `tests/sync_scenarios_test.go` | convergence, offline peers, conflicts |
| `tests/index_scenarios_test.go` | extraction, chunking, embedding, search quality |
| `tests/api_scenarios_test.go` | HTTP API and PWA-facing behaviour |
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
text mentions a command the skill does not document, or if a documented
collection key is ever emitted as `null`.

## What counts as coverage

Synchronisation is only considered working when a test shows all of: content
arriving on a peer, deletions propagating, offline peers converging on
reconnect, interrupted transfers leaving no `.locus-tmp-*` leftovers, and
divergent edits preserving both sides with identical sibling names on both
peers.

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

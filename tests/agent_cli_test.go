package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/teulaert/locus/internal/agent"
	"github.com/teulaert/locus/internal/config"
	"github.com/teulaert/locus/internal/store"
)

// Scenario 15 — an agent with no knowledge of the implementation, only the
// built binary and its --help / skill output, can find documents in the user's
// roots and cite provenance.
//
// The test deliberately never imports an internal package that would let it
// reach around the CLI: it drives the executable exactly the way SKILL.md
// tells an agent to.

var (
	// The build runs from the module root, one level up from this package.
	buildOnce sync.Once
	buildPath string
	buildErr  error
	// The shared binary outlives any single test, so its directory is removed
	// when the test binary finishes rather than by t.Cleanup.
	buildDir string
)

// TestMain removes the shared binary once the suite is done; t.Cleanup for the
// first test would take it away from the tests that run after it.
func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

// locusBinary builds the real binary once per test binary and returns its path.
func locusBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "locus-bin-*")
		if err != nil {
			buildErr = err
			return
		}
		buildDir = dir
		path := filepath.Join(dir, "locus")
		cmd := exec.Command("go", "build", "-o", path, "./cmd/locus")
		cmd.Dir = ".."
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
			return
		}
		buildPath = path
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return buildPath
}

// cli is an agent's view of the tool: run it, get stdout, stderr and the exit
// code. Nothing else about Locus is observable to it.
type cli struct {
	t       *testing.T
	binary  string
	config  string
	dataDir string // passed as --data-dir unless empty
	root    string
	home    string // HOME and XDG base directories of the child process
}

var agentDocs = map[string]string{
	"Finance/2026/solar-installation.md": `# Solar installation contract

## Payment terms

The invoice for the solar panels is payable within 30 days of installation.
Roof orientation must be south-west for the quoted yield of 4.2 kWp.
`,
	"Notes/recipes.txt": `Sourdough starter: 100g flour, 100g water, daily feeding for a week.`,
	"Misc/random.txt":   `Nothing relevant here, just a shopping list for the week.`,
	// An underscore in a directory name must not break --path (LOC-36).
	"House_2025/boiler-service.txt": `Boiler service receipt: annual maintenance of the gas boiler, flue inspected.`,
}

// newAgentCLI creates an isolated peer: its own config, data directory and
// sync root. Nothing touches the machine running the tests.
func newAgentCLI(t *testing.T, docs map[string]string) *cli {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "documents")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for rel, body := range docs {
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	c := &cli{
		t:       t,
		binary:  locusBinary(t),
		config:  filepath.Join(dir, "config.json"),
		dataDir: filepath.Join(dir, "data"),
		root:    root,
		home:    filepath.Join(dir, "home"),
	}
	t.Cleanup(c.requireDefaultDirsUntouched)
	// Step one for any agent: create a configuration with a sync root.
	code, stdout, stderr := c.run("init", "--root", root)
	if code != 0 {
		t.Fatalf("init exit %d: %s %s", code, stdout, stderr)
	}
	if _, err := os.Stat(c.config); err != nil {
		t.Fatalf("init did not write the config file: %v", err)
	}
	return c
}

// env is the child's environment: the caller's, minus anything that would
// point Locus at the developer's real configuration or data, with HOME and the
// XDG base directories moved into the test's temporary directory. A command
// that falls back to a default location therefore lands somewhere the test
// can inspect instead of in ~/.local/share/locus.
func (c *cli) env() []string {
	if c.home == "" {
		c.home = c.t.TempDir()
	}
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch {
		case strings.HasPrefix(name, "LOCUS_"), strings.HasPrefix(name, "XDG_"), name == "HOME":
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"HOME="+c.home,
		"XDG_CONFIG_HOME="+filepath.Join(c.home, ".config"),
		"XDG_DATA_HOME="+filepath.Join(c.home, ".local", "share"),
		"XDG_CACHE_HOME="+filepath.Join(c.home, ".cache"),
		"XDG_STATE_HOME="+filepath.Join(c.home, ".local", "state"),
	)
}

// requireDefaultDirsUntouched fails if anything was written to the default
// configuration or data directory, i.e. if an explicit --config/--data-dir
// was ignored somewhere.
func (c *cli) requireDefaultDirsUntouched() {
	c.t.Helper()
	for _, d := range []string{
		filepath.Join(c.home, ".config", "locus"),
		filepath.Join(c.home, ".local", "share", "locus"),
	} {
		if _, err := os.Stat(d); err == nil {
			c.t.Errorf("%s was created although --config and --data-dir were given", d)
		}
	}
}

// run executes the binary and returns (exit code, stdout, stderr).
func (c *cli) run(args ...string) (int, string, string) {
	c.t.Helper()
	full := []string{"--config", c.config}
	if c.dataDir != "" {
		full = append(full, "--data-dir", c.dataDir)
	}
	full = append(full, args...)
	cmd := exec.Command(c.binary, full...)
	cmd.Env = c.env()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		exit, ok := err.(*exec.ExitError)
		if !ok {
			c.t.Fatalf("running locus %s: %v", strings.Join(args, " "), err)
		}
		code = exit.ExitCode()
	}
	return code, stdout.String(), stderr.String()
}

// mustJSON runs the binary expecting exit 0 and decodes stdout as JSON.
func (c *cli) mustJSON(args ...string) map[string]any {
	c.t.Helper()
	code, stdout, stderr := c.run(args...)
	if code != 0 {
		c.t.Fatalf("locus %s: exit %d (want 0)\nstdout: %s\nstderr: %s", strings.Join(args, " "), code, truncate(stdout), stderr)
	}
	return decode(c.t, []byte(stdout))
}

// ---------- schema helpers, as an agent would use them ----------

// collectionKeys are the keys that must always be arrays, never null, so that
// a caller can iterate without nil checks.
var collectionKeys = map[string]bool{
	"results": true, "context": true, "provenance": true, "chunks": true,
	"roots": true, "peers": true, "conflicts": true, "documents": false,
	"jobs": true,
}

// The documented top-level keys of the responses the CLI and the HTTP API
// share (LOC-23). Both front ends encode the same typed value, so the CLI
// test and the API test assert exactly these sets.
var (
	searchKeys        = []string{"query", "results"}
	contextSearchKeys = []string{"query", "results", "context", "provenance"}
	indexStatusKeys   = []string{"index", "jobs", "model"}
)

// requireExactKeys asserts obj has exactly keys, none of them null.
func requireExactKeys(t *testing.T, obj map[string]any, keys ...string) {
	t.Helper()
	requireKeys(t, obj, keys...)
	for k, v := range obj {
		if !slices.Contains(keys, k) {
			t.Fatalf("undocumented key %q in %s", k, truncate(mustMarshal(t, obj)))
		}
		if v == nil {
			t.Fatalf("key %q is null in %s", k, truncate(mustMarshal(t, obj)))
		}
	}
}

func requireKeys(t *testing.T, obj map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		v, ok := obj[k]
		if !ok {
			t.Fatalf("documented key %q missing from %s", k, truncate(mustMarshal(t, obj)))
		}
		if v == nil && collectionKeys[k] {
			t.Fatalf("collection key %q must be an array, got null in %s", k, truncate(mustMarshal(t, obj)))
		}
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%+v", v)
	}
	return string(b)
}

func truncate(s string) string {
	if len(s) > 800 {
		return s[:800] + "…"
	}
	return s
}

// ---------- the scenario ----------

func TestScenario15_AgentDiscoversAndUsesCLI(t *testing.T) {
	c := newAgentCLI(t, agentDocs)

	t.Run("self discovery", func(t *testing.T) {
		// The binary explains itself without the design document.
		code, stdout, stderr := c.run("--help")
		help := stdout + stderr
		if code != 0 && code != 2 {
			t.Fatalf("--help exit %d", code)
		}
		for _, want := range []string{"Commands:", "search", "index rebuild", "conflicts", "Exit codes"} {
			if !strings.Contains(help, want) {
				t.Fatalf("--help does not mention %q", want)
			}
		}
		// Per-command help exists too.
		if _, stdout, stderr := c.run("search", "--help"); !strings.Contains(stdout+stderr, "--context") {
			t.Fatal("search --help does not document --context")
		}

		// The embedded skill matches the one in the repository.
		code, stdout, _ = c.run("skill")
		if code != 0 {
			t.Fatalf("locus skill exit %d", code)
		}
		if stdout != agent.Skill() {
			t.Fatal("locus skill output does not match the embedded skill documentation")
		}
		if rootSkill, err := os.ReadFile(filepath.Join("..", "SKILL.md")); err == nil && string(rootSkill) != agent.Skill() {
			t.Fatal("root SKILL.md has drifted from the skill embedded in the binary (run: make skill)")
		}
		for _, section := range []string{"Commands", "Exit codes", "JSON schemas", "Common failure modes", "Security assumptions"} {
			if agent.Section(section) == "" {
				t.Fatalf("skill documentation is missing the %q section", section)
			}
		}
		// Every command, subcommand and flag in the usage text is documented
		// in the skill. The usage goes to stderr, so read both streams.
		_, stdout, stderr = c.run("--help")
		usage := stdout + stderr
		subcommands := map[string][]string{
			"sync": {"status", "now"}, "index": {"status", "rebuild"}, "conflicts": {"show", "resolve"},
		}
		checked := 0
		for _, line := range strings.Split(usage, "\n") {
			fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "locus"))
			if len(fields) < 2 || strings.HasPrefix(strings.TrimSpace(line), "locus [") {
				continue
			}
			name := fields[0]
			if strings.HasPrefix(name, "--") {
				if !strings.Contains(agent.Skill(), "`"+name) {
					t.Fatalf("flag %s is in the usage but not in SKILL.md", name)
				}
				continue
			}
			switch name {
			case "init", "serve", "status", "peers", "sync", "search", "index", "conflicts", "skill", "version":
			default:
				continue
			}
			cmd := "locus " + name
			if slices.Contains(subcommands[name], fields[1]) {
				cmd += " " + fields[1]
			}
			if !strings.Contains(agent.Skill(), cmd) {
				t.Fatalf("command %q is in the usage but not in SKILL.md", cmd)
			}
			checked++
		}
		if checked < 10 {
			t.Fatalf("found only %d commands in the usage text; is it still printed?", checked)
		}
	})

	t.Run("indexing", func(t *testing.T) {
		stats := c.mustJSON("index", "rebuild", "--json")
		requireKeys(t, stats, "documents", "chunks", "embeddings")
		if got := int(stats["documents"].(float64)); got != len(agentDocs) {
			t.Fatalf("indexed %v documents, want %d", stats["documents"], len(agentDocs))
		}
		if got := stats["embeddings"].(float64); got == 0 {
			t.Fatal("no embeddings produced; semantic search would silently degrade")
		}
		st := c.mustJSON("index", "status", "--json")
		requireExactKeys(t, st, indexStatusKeys...)
		if st["model"] == "" {
			t.Fatal("index status must report the embedding model")
		}
	})

	t.Run("search finds the right document", func(t *testing.T) {
		res := c.mustJSON("search", "invoice for the solar panels", "--json")
		requireExactKeys(t, res, searchKeys...)
		results, ok := res["results"].([]any)
		if !ok || len(results) == 0 {
			t.Fatalf("search found nothing: %s", truncate(mustMarshal(t, res)))
		}
		top := results[0].(map[string]any)
		requireKeys(t, top, "root", "path", "score", "snippet", "chunks")
		if top["path"] != "Finance/2026/solar-installation.md" {
			t.Fatalf("best match is %v, want the solar contract", top["path"])
		}
		if !strings.Contains(strings.ToLower(top["snippet"].(string)), "invoice") {
			t.Fatalf("snippet does not quote the matched text: %v", top["snippet"])
		}
		if top["root"] != c.root {
			t.Fatalf("results must name the root: %v", top["root"])
		}
		chunks := top["chunks"].([]any)
		if len(chunks) == 0 {
			t.Fatal("no chunk-level provenance in results")
		}
		ch0 := chunks[0].(map[string]any)
		requireKeys(t, ch0, "ordinal", "score")
		if ch0["section"] != "Payment terms" {
			t.Fatalf("chunk provenance lost the section heading: %s", mustMarshal(t, ch0))
		}
	})

	t.Run("context output is bounded and attributed", func(t *testing.T) {
		ctxResp := c.mustJSON("search", "invoice for the solar panels", "--context", "--json", "--limit", "1")
		requireExactKeys(t, ctxResp, contextSearchKeys...)
		if results := ctxResp["results"].([]any); len(results) != 1 {
			t.Fatalf("--context --limit 1 returned %d results, want 1", len(results))
		}
		passages := ctxResp["context"].([]any)
		if len(passages) == 0 {
			t.Fatal("--context returned no passages")
		}
		if !strings.Contains(passages[0].(string), "solar") {
			t.Fatalf("--context passage does not contain the answer: %q", passages[0])
		}
		if n := len(passages[0].(string)); n > 4000 {
			t.Fatalf("--context passage is %d characters; it must fit a bounded context window", n)
		}
		prov := ctxResp["provenance"].([]any)
		if len(prov) == 0 {
			t.Fatal("--context results must carry provenance")
		}
		requireKeys(t, prov[0].(map[string]any), "path", "root")
	})

	t.Run("filters", func(t *testing.T) {
		for _, r := range c.mustJSON("search", "solar", "--type", "md", "--json")["results"].([]any) {
			if !strings.HasSuffix(r.(map[string]any)["path"].(string), ".md") {
				t.Fatalf("--type md returned %v", r)
			}
		}
		for _, r := range c.mustJSON("search", "solar", "--path", "Finance", "--json")["results"].([]any) {
			if !strings.HasPrefix(r.(map[string]any)["path"].(string), "Finance") {
				t.Fatalf("--path Finance returned %v", r)
			}
		}
		// '_' in --path is a literal character, not a LIKE wildcard (LOC-36).
		underscored := c.mustJSON("search", "boiler", "--path", "House_2025", "--json")["results"].([]any)
		if len(underscored) == 0 {
			t.Fatal("--path House_2025 found nothing; '_' in a path filter must match literally")
		}
		for _, r := range underscored {
			if !strings.HasPrefix(r.(map[string]any)["path"].(string), "House_2025/") {
				t.Fatalf("--path House_2025 returned %v", r)
			}
		}
		// A filter that excludes everything yields exit 3.
		code, stdout, _ := c.run("search", "solar", "--path", "Notes", "--json")
		if code != 3 {
			t.Fatalf("search excluded by --path exit %d, want 3: %s", code, truncate(stdout))
		}
	})

	t.Run("no results is exit 3", func(t *testing.T) {
		code, stdout, stderr := c.run("search", "underwater basket weaving", "--json")
		if code != 3 {
			t.Fatalf("empty search exit %d, want 3 (stderr: %s)", code, stderr)
		}
		empty := decode(t, []byte(stdout))
		if results, ok := empty["results"].([]any); !ok || len(results) != 0 {
			t.Fatalf("empty search must return an empty results array: %s", truncate(stdout))
		}
		if strings.Contains(stdout, "found nothing") {
			t.Fatal("--json output must not contain progress text")
		}
		// --context keeps its shape when nothing matches: empty arrays.
		code, stdout, stderr = c.run("search", "underwater basket weaving", "--context", "--json")
		if code != 3 {
			t.Fatalf("empty --context search exit %d, want 3 (stderr: %s)", code, stderr)
		}
		emptyCtx := decode(t, []byte(stdout))
		requireExactKeys(t, emptyCtx, contextSearchKeys...)
		for _, k := range []string{"results", "context", "provenance"} {
			if list, ok := emptyCtx[k].([]any); !ok || len(list) != 0 {
				t.Fatalf("empty --context search: %q must be an empty array: %s", k, truncate(stdout))
			}
		}
	})

	t.Run("status and conflicts", func(t *testing.T) {
		st := c.mustJSON("status", "--json")
		requireKeys(t, st, "peer_id", "hostname", "version", "data_dir", "roots", "index", "embedding", "peers", "conflicts")
		roots := st["roots"].([]any)
		if len(roots) != 1 {
			t.Fatalf("status roots: %s", mustMarshal(t, st["roots"]))
		}
		ru := roots[0].(map[string]any)
		requireKeys(t, ru, "path", "name", "files", "sync_state")
		if int(ru["files"].(float64)) != len(agentDocs) {
			t.Fatalf("status reports %v files on disk", ru["files"])
		}
		if emb := st["embedding"].(map[string]any); emb["used"] != "cpu" {
			t.Fatalf("embedding device should be cpu in tests, got %v", emb["used"])
		}
		if list, ok := c.mustJSON("conflicts", "--json")["conflicts"].([]any); !ok || len(list) != 0 {
			t.Fatal("a freshly indexed peer must have no conflicts")
		}
		if list, ok := c.mustJSON("peers", "--json")["peers"].([]any); !ok {
			t.Fatal("peers must be an array")
		} else {
			for _, p := range list {
				requireKeys(t, p.(map[string]any), "peer_id", "hostname", "state")
			}
		}
	})

	t.Run("usage and configuration errors", func(t *testing.T) {
		for _, bad := range [][]string{{"no-such-command"}, {"search"}, {"index", "nope"}, {"conflicts", "show"}} {
			code, _, stderr := c.run(bad...)
			if code != 2 {
				t.Fatalf("locus %v exit %d, want 2 (stderr: %s)", bad, code, truncate(stderr))
			}
		}
		orphan := &cli{t: t, binary: c.binary, config: filepath.Join(t.TempDir(), "absent.json"), dataDir: t.TempDir(), home: t.TempDir()}
		if code, _, _ := orphan.run("status", "--json"); code != 4 {
			t.Fatalf("locus status with missing config exit %d, want 4", code)
		}
	})
}

// LOC-12: init must create the identity in the --data-dir it was given (and
// remember that directory), and must not silently replace an existing
// configuration.
func TestCLIInitHonoursDataDirAndKeepsConfig(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "documents")
	c := &cli{
		t:       t,
		binary:  locusBinary(t),
		config:  filepath.Join(dir, "config.json"),
		dataDir: filepath.Join(dir, "data"),
		root:    root,
		home:    filepath.Join(dir, "home"),
	}
	t.Cleanup(c.requireDefaultDirsUntouched)

	initOut := c.mustJSON("init", "--root", root, "--json")
	requireKeys(t, initOut, "config", "data_dir", "peer_id", "roots")
	if got := initOut["data_dir"]; got != c.dataDir {
		t.Fatalf("init reported data_dir %v, want the --data-dir %s", got, c.dataDir)
	}
	if _, err := os.Stat(filepath.Join(c.dataDir, "identity.json")); err != nil {
		t.Fatalf("init did not create the identity in --data-dir: %v", err)
	}
	peerID := initOut["peer_id"]

	// Later commands, with and without --data-dir, see the identity init made.
	if st := c.mustJSON("status", "--json"); st["peer_id"] != peerID || st["data_dir"] != c.dataDir {
		t.Fatalf("status reports peer %v in %v, init created %v in %s", st["peer_id"], st["data_dir"], peerID, c.dataDir)
	}
	bare := *c
	bare.dataDir = ""
	if st := bare.mustJSON("status", "--json"); st["peer_id"] != peerID || st["data_dir"] != c.dataDir {
		t.Fatalf("without --data-dir status reports peer %v in %v; init did not persist %s", st["peer_id"], st["data_dir"], c.dataDir)
	}

	// Re-running init must not discard the existing configuration.
	before, err := os.ReadFile(c.config)
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "other")
	code, stdout, stderr := c.run("init", "--root", other, "--json")
	if code != 4 {
		t.Fatalf("init over an existing config exit %d, want 4\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "--force") {
		t.Fatalf("refusal does not mention --force: %s", stderr)
	}
	if after, _ := os.ReadFile(c.config); !bytes.Equal(before, after) {
		t.Fatal("refused init still changed the configuration file")
	}

	// --force replaces it, keeping the identity in the data directory.
	forced := c.mustJSON("init", "--root", other, "--force", "--json")
	if forced["peer_id"] != peerID {
		t.Fatalf("init --force changed the peer identity: %v, was %v", forced["peer_id"], peerID)
	}
	roots, _ := forced["roots"].([]any)
	if len(roots) != 1 || roots[0] != other {
		t.Fatalf("init --force wrote roots %v, want [%s]", forced["roots"], other)
	}
}

// `--root NAME=PATH` writes a named root; a bare `--root PATH` stays a bare
// string named by its basename. Status shows each root's name, and two roots
// that would answer to one name are refused before anything is written.
func TestCLIInitNamedRoots(t *testing.T) {
	dir := t.TempDir()
	docs := filepath.Join(dir, "Sync", "Docs")
	notes := filepath.Join(dir, "Notes")
	c := &cli{
		t:       t,
		binary:  locusBinary(t),
		config:  filepath.Join(dir, "config.json"),
		dataDir: filepath.Join(dir, "data"),
		root:    docs,
		home:    filepath.Join(dir, "home"),
	}
	t.Cleanup(c.requireDefaultDirsUntouched)

	// Two roots both named "Notes" (one by basename) cannot be told apart.
	code, stdout, stderr := c.run("init", "--root", notes, "--root", "Notes="+docs, "--json")
	if code != 4 || !strings.Contains(stderr, "share the name") {
		t.Fatalf("init with a duplicate root name: exit %d, want 4\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if _, err := os.Stat(c.config); !os.IsNotExist(err) {
		t.Fatalf("refused init wrote a configuration: %v", err)
	}

	out := c.mustJSON("init", "--root", "documents="+docs, "--root", notes, "--json")
	want := `[{"name":"documents","path":"` + docs + `"},"` + notes + `"]`
	if got := mustMarshal(t, out["roots"]); got != want {
		t.Fatalf("init roots = %s, want %s", got, want)
	}
	raw, err := os.ReadFile(c.config)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"name": "documents"`) {
		t.Fatalf("config does not record the root name:\n%s", raw)
	}

	st := c.mustJSON("status", "--json")
	names := map[string]any{}
	for _, r := range st["roots"].([]any) {
		ru := r.(map[string]any)
		names[ru["path"].(string)] = ru["name"]
	}
	if names[docs] != "documents" || names[notes] != "Notes" {
		t.Fatalf("status root names = %v", names)
	}
	code, stdout, stderr = c.run("status")
	if code != 0 || !strings.Contains(stdout, "documents  "+docs) || !strings.Contains(stdout, "Notes  "+notes) {
		t.Fatalf("status does not show root names (exit %d):\n%s%s", code, stdout, stderr)
	}
}

// Every route the HTTP API registers is listed in SKILL.md with its method,
// so an agent never has to read the source to find one (LOC-32). The routes
// are read from the API's source rather than by importing it, which keeps
// this package's rule of driving Locus only from the outside.
func TestSkillDocumentsEveryAPIRoute(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "internal", "api", "api.go"))
	if err != nil {
		t.Fatal(err)
	}
	routes := regexp.MustCompile(`mux\.HandleFunc\("([A-Z]+) (/[^"]*)"`).FindAllStringSubmatch(string(src), -1)
	if len(routes) < 10 {
		t.Fatalf("found only %d routes in internal/api/api.go; did registration change?", len(routes))
	}
	skill := agent.Skill()
	for _, r := range routes {
		method, path := r[1], r[2]
		if path == "/" {
			continue // the PWA shell
		}
		if !regexp.MustCompile(`(?m)^` + method + ` +` + regexp.QuoteMeta(path) + `([ ?]|$)`).MatchString(skill) {
			t.Errorf("route %s %s is served but not listed in SKILL.md", method, path)
		}
	}
}

// The main --help lists every search flag SKILL.md documents, --budget
// included (LOC-53).
func TestCLIHelpMentionsBudget(t *testing.T) {
	c := newAgentCLI(t, nil)
	code, stdout, stderr := c.run("--help")
	if code != 0 {
		t.Fatalf("--help exit %d", code)
	}
	if !strings.Contains(stdout+stderr, "--budget") {
		t.Fatalf("--help does not mention --budget:\n%s%s", stdout, stderr)
	}
}

// A conflict row whose hashes are empty or shorter than the eight characters
// the listing abbreviates to renders instead of panicking (LOC-53). Seeding
// the row is the one step that reaches into the store: no real round
// produces such a row on demand.
func TestCLIConflictsRenderShortHashes(t *testing.T) {
	c := newAgentCLI(t, nil)
	st, err := store.Open(c.dataDir)
	if err != nil {
		t.Fatal(err)
	}
	rows := []*store.Conflict{
		{ID: "c-empty", Root: "documents", RelPath: "gone.txt", LocalPath: "gone.txt",
			ConflictPath: "gone.conflict-x.txt", ConflictHash: "abc", CreatedAt: time.Now().Unix()},
		{ID: "c-short", Root: "documents", RelPath: "tiny.txt", LocalPath: "tiny.txt",
			LocalHash: "1234", CreatedAt: time.Now().Unix()},
	}
	for _, r := range rows {
		if err := st.PutConflict(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := c.run("conflicts")
	if code != 5 {
		t.Fatalf("conflicts exit %d (want 5)\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if strings.Contains(stderr, "panic") {
		t.Fatalf("conflicts panicked:\n%s", stderr)
	}
	for _, want := range []string{"c-empty", "gone.txt", "c-short", "(1234,"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("conflicts output lacks %q:\n%s", want, stdout)
		}
	}
	for _, id := range []string{"c-empty", "c-short"} {
		if code, stdout, stderr := c.run("conflicts", "show", id); code != 0 || !strings.Contains(stdout, id) {
			t.Fatalf("conflicts show %s: exit %d\nstdout: %s\nstderr: %s", id, code, stdout, stderr)
		}
	}
}

// locus sync now exits 5 when the round produced conflicts and 0 otherwise,
// whether or not --json is given: the exit code never depends on the output
// format (LOC-53). The CLI peer synchronizes with one running peer that has
// independently created the same paths with different content.
func TestCLISyncNowExitCodeParity(t *testing.T) {
	c := newAgentCLI(t, map[string]string{"Notes/hello.txt": "hello from the CLI peer"})
	selfID, _ := c.mustJSON("status", "--json")["peer_id"].(string)
	if selfID == "" {
		t.Fatal("status --json has no peer_id")
	}

	// The running peer pairs with the CLI peer's root by folder name and
	// admits it explicitly: the CLI peer never answers discovery itself.
	ports := allocatePorts(t, 1)
	other := newNodeWithRoot(t, "other", ports[0], ports, 0, filepath.Base(c.root), "")
	other.Cfg.AuthorizedPeers = []string{selfID}
	other.start(t)
	t.Cleanup(func() { other.stopServing(t) })

	// Point the CLI peer at the running one through static discovery.
	raw, err := os.ReadFile(c.config)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	disc, _ := cfg["discovery"].(map[string]any)
	if disc == nil {
		disc = map[string]any{}
	}
	disc["peers"] = []string{fmt.Sprintf("127.0.0.1:%d", ports[0].disc)}
	cfg["discovery"] = disc
	if raw, err = json.MarshalIndent(cfg, "", "  "); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.config, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	// clash writes rel with different content on both peers and makes sure
	// the running peer has recorded its version before the CLI round.
	clash := func(rel string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(c.root, rel), []byte("CLI peer's "+rel), 0o644); err != nil {
			t.Fatal(err)
		}
		other.write(t, rel, "running peer's "+rel)
		if _, err := other.App.Reconciler.Pass(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	// A round without conflicts exits 0 in both modes.
	for _, args := range [][]string{{"sync", "now", "--json"}, {"sync", "now"}} {
		code, stdout, stderr := c.run(args...)
		if code != 0 {
			t.Fatalf("locus %s without conflicts: exit %d (want 0)\nstdout: %s\nstderr: %s",
				strings.Join(args, " "), code, stdout, stderr)
		}
	}
	if !other.has(t, "Notes/hello.txt") {
		t.Fatal("the round did not reach the running peer")
	}

	clash("json-clash.txt")
	code, stdout, stderr := c.run("sync", "now", "--json")
	if code != 5 {
		t.Fatalf("sync now --json with a conflict: exit %d (want 5)\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if n, _ := decode(t, []byte(stdout))["conflicted"].(float64); n < 1 {
		t.Fatalf("sync now --json reports conflicted %v, want >= 1:\n%s", n, stdout)
	}

	clash("text-clash.txt")
	code, stdout, stderr = c.run("sync", "now")
	if code != 5 {
		t.Fatalf("sync now with a conflict: exit %d (want 5)\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "conflicted ") || strings.Contains(stdout, "conflicted 0 ") {
		t.Fatalf("sync now does not report the conflict:\n%s", stdout)
	}
}

// With a daemon running on the data directory, the CLI's status commands
// report the daemon's live state instead of a snapshot of their own, and
// `sync now` has the daemon run the round; with no daemon they still answer
// from the databases (LOC-57).
func TestCLIStatusComesFromRunningDaemon(t *testing.T) {
	// No background rounds, so the only round the daemon ever records is the
	// one the CLI asks for.
	cl := newClusterWith(t, 2, nil, func(cfg *config.Config) {
		cfg.Sync.Interval = config.Duration(time.Hour)
	})
	n := cl.nodes[0]
	c := &cli{
		t:       t,
		binary:  locusBinary(t),
		config:  filepath.Join(n.Dir, "config.json"),
		dataDir: n.Cfg.DataDir,
		root:    n.Root,
		home:    t.TempDir(),
	}
	n.write(t, "hello.txt", "hello from the daemon's root")

	st := c.mustJSON("status", "--json")
	requireKeys(t, st, "peer_id", "hostname", "version", "data_dir", "roots", "index", "embedding", "peers", "conflicts", "daemon")
	if st["daemon"] != true {
		t.Fatalf("status with a daemon running reports daemon %v:\n%s", st["daemon"], truncate(mustMarshal(t, st)))
	}
	if st["peer_id"] != n.PeerID {
		t.Fatalf("status peer_id %v, want the daemon's %s", st["peer_id"], n.PeerID)
	}
	if peers, _ := st["peers"].([]any); len(peers) != 1 {
		t.Fatalf("status peers from the daemon: %s", mustMarshal(t, st["peers"]))
	}
	if pe := c.mustJSON("peers", "--json"); len(pe["peers"].([]any)) != 1 {
		t.Fatalf("peers from the daemon: %s", mustMarshal(t, pe))
	}

	// sync now is run by the daemon: its round becomes the daemon's
	// last_sync, which a round run by the CLI process could never set.
	round := c.mustJSON("sync", "now", "--json")
	started, _ := round["started_at"].(string)
	if started == "" {
		t.Fatalf("sync now --json has no started_at: %s", mustMarshal(t, round))
	}
	if got := n.App.LastSync(); got == nil || got.StartedAt.Format(time.RFC3339Nano) != started {
		t.Fatalf("sync now did not run in the daemon: CLI round started %s, daemon's last round %+v", started, got)
	}

	// The daemon's last round is what status and sync status now report.
	st = c.mustJSON("status", "--json")
	if ls, _ := st["last_sync"].(map[string]any); ls == nil || ls["started_at"] != started {
		t.Fatalf("status does not report the daemon's last round %s: %s", started, mustMarshal(t, st["last_sync"]))
	}
	ss := c.mustJSON("sync", "status", "--json")
	requireKeys(t, ss, "roots", "last_sync", "syncing")
	if ls, _ := ss["last_sync"].(map[string]any); ls == nil || ls["started_at"] != started {
		t.Fatalf("sync status does not report the daemon's last round: %s", mustMarshal(t, ss))
	}
	if ss["syncing"] != false {
		t.Fatalf("sync status with an idle daemon: syncing %v", ss["syncing"])
	}

	// Without the daemon the CLI answers from its own snapshot, as before.
	n.stopServing(t)
	st = c.mustJSON("status", "--json")
	if st["daemon"] != false {
		t.Fatalf("status with no daemon reports daemon %v", st["daemon"])
	}
	if _, ok := st["last_sync"]; ok {
		t.Fatalf("status with no daemon has a last_sync: %s", mustMarshal(t, st["last_sync"]))
	}
	for _, r := range st["roots"].([]any) {
		if s := r.(map[string]any)["sync_state"]; s != "idle" {
			t.Fatalf("status with no daemon: sync_state %v, want idle", s)
		}
	}
	ss = c.mustJSON("sync", "status", "--json")
	requireKeys(t, ss, "roots", "last_sync", "syncing")
	if ss["last_sync"] != nil || ss["syncing"] != false {
		t.Fatalf("sync status with no daemon: %s", mustMarshal(t, ss))
	}
}

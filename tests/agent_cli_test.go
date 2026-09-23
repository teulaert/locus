package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/teulaert/locus/internal/agent"
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
	dataDir string
	root    string
}

var agentDocs = map[string]string{
	"Finance/2026/solar-installation.md": `# Solar installation contract

## Payment terms

The invoice for the solar panels is payable within 30 days of installation.
Roof orientation must be south-west for the quoted yield of 4.2 kWp.
`,
	"Notes/recipes.txt": `Sourdough starter: 100g flour, 100g water, daily feeding for a week.`,
	"Misc/random.txt":   `Nothing relevant here, just a shopping list for the week.`,
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
	}
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

// run executes the binary and returns (exit code, stdout, stderr).
func (c *cli) run(args ...string) (int, string, string) {
	c.t.Helper()
	full := append([]string{"--config", c.config, "--data-dir", c.dataDir}, args...)
	cmd := exec.Command(c.binary, full...)
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
		// Every command in the usage text is documented in the skill.
		_, usage, _ := c.run("--help")
		for _, line := range strings.Split(usage, "\n") {
			fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(line), "locus"))
			if len(fields) < 2 || strings.HasPrefix(strings.TrimSpace(line), "locus [") {
				continue
			}
			name := fields[0]
			if name == "init" || name == "serve" || name == "status" || name == "peers" ||
				name == "sync" || name == "search" || name == "index" || name == "conflicts" ||
				name == "skill" || name == "version" {
				if !strings.Contains(agent.Skill(), "locus "+name) {
					t.Fatalf("command %q is in the usage but not in SKILL.md", name)
				}
			}
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
		requireKeys(t, st, "index", "jobs", "model")
		if st["model"] == "" {
			t.Fatal("index status must report the embedding model")
		}
	})

	t.Run("search finds the right document", func(t *testing.T) {
		res := c.mustJSON("search", "invoice for the solar panels", "--json")
		requireKeys(t, res, "query", "results")
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
		requireKeys(t, ctxResp, "query", "context", "provenance")
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
	})

	t.Run("status and conflicts", func(t *testing.T) {
		st := c.mustJSON("status", "--json")
		requireKeys(t, st, "peer_id", "hostname", "version", "data_dir", "roots", "index", "embedding", "peers", "conflicts")
		roots := st["roots"].([]any)
		if len(roots) != 1 {
			t.Fatalf("status roots: %s", mustMarshal(t, st["roots"]))
		}
		ru := roots[0].(map[string]any)
		requireKeys(t, ru, "path", "files", "sync_state")
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
		orphan := &cli{t: t, binary: c.binary, config: filepath.Join(t.TempDir(), "absent.json"), dataDir: t.TempDir()}
		if code, _, _ := orphan.run("status", "--json"); code != 4 {
			t.Fatalf("locus status with missing config exit %d, want 4", code)
		}
	})
}

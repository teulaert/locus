//go:build unix

package update

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

const (
	reexecHelperEnv = "LOCUS_TEST_REEXEC_HELPER"
	reexecStageEnv  = "LOCUS_TEST_REEXEC_STAGE"
	reexecOutEnv    = "LOCUS_TEST_REEXEC_OUT"
	reexecKeepEnv   = "LOCUS_TEST_KEEP"
)

type reexecReport struct {
	Stage1Pid int      `json:"stage1_pid"`
	Pid       int      `json:"pid"`
	Args      []string `json:"args"`
	Keep      string   `json:"keep"`
	Err       string   `json:"err,omitempty"`
}

// reexecHelper runs when the test binary is started as a stand-in daemon
// (TestMain). The first run stages a copy of its own binary, swaps it in
// and re-executes it, as `locus serve` does after an update; the second run,
// now the swapped-in binary, reports its pid, arguments and environment.
func reexecHelper() {
	out := os.Getenv(reexecOutEnv)
	fail := func(err error) {
		b, _ := json.Marshal(reexecReport{Err: err.Error()})
		_ = os.WriteFile(out, b, 0o600)
		os.Exit(1)
	}
	if os.Getenv(reexecStageEnv) == "" {
		exe, err := ResolveExecutable()
		if err != nil {
			fail(err)
		}
		if err := copyFile(exe, StagePath(exe)); err != nil {
			fail(err)
		}
		if err := Swap(exe); err != nil {
			fail(err)
		}
		_ = os.Setenv(reexecStageEnv, strconv.Itoa(os.Getpid()))
		fail(Reexec(exe)) // only returns on failure
	}
	stage1, _ := strconv.Atoi(os.Getenv(reexecStageEnv))
	b, _ := json.Marshal(reexecReport{Stage1Pid: stage1, Pid: os.Getpid(), Args: os.Args, Keep: os.Getenv(reexecKeepEnv)})
	_ = os.WriteFile(out, b, 0o600)
	os.Exit(0)
}

// After a swap the daemon re-executes the new binary in its own process:
// the pid stays (so systemd and launchd keep supervising the same service
// process) and so do the arguments and environment.
func TestReexecKeepsPidArgsAndEnv(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	exe := filepath.Join(dir, "locus")
	if err := copyFile(self, exe); err != nil {
		t.Fatal(err)
	}
	// Started through a symlink, as /usr/local/bin/locus -> the service copy.
	link := filepath.Join(dir, "locus-link")
	if err := os.Symlink(exe, link); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "report.json")
	args := []string{"--config", "/etc/locus/config with space.json", "serve"}
	cmd := exec.Command(link, args...)
	cmd.Env = append(os.Environ(), reexecHelperEnv+"=1", reexecOutEnv+"="+out, reexecKeepEnv+"=kept value")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper: %v\n%s", err, output)
	}
	var rep reexecReport
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("no report: %v\n%s", err, output)
	}
	if err := json.Unmarshal(b, &rep); err != nil || rep.Err != "" {
		t.Fatalf("report %s: %v", b, err)
	}
	if rep.Pid != cmd.Process.Pid || rep.Stage1Pid != rep.Pid {
		t.Fatalf("pid after re-exec %d (before %d), want the started process's %d", rep.Pid, rep.Stage1Pid, cmd.Process.Pid)
	}
	if want := append([]string{link}, args...); !slices.Equal(rep.Args, want) {
		t.Fatalf("args after re-exec %q, want %q", rep.Args, want)
	}
	if rep.Keep != "kept value" {
		t.Fatalf("environment after re-exec: %q", rep.Keep)
	}
	if _, err := os.Stat(PreviousPath(exe)); err != nil {
		t.Fatalf("previous binary not kept: %v", err)
	}
	if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the symlink was replaced instead of the file it points to: %v", err)
	}
}

package update

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

// Files the updater keeps in the data directory.
const (
	// stateFile is the last check's outcome, so `locus status` without a
	// daemon still says what is available, and the commits rolled back.
	stateFile = "update.json"
	// healthFile marks a swap whose new binary has not yet reached serving.
	healthFile = "update-health.json"
)

// savedState is what stateFile holds.
type savedState struct {
	Latest       string    `json:"latest,omitempty"`
	LatestCommit string    `json:"latest_commit,omitempty"`
	State        string    `json:"state,omitempty"`
	Error        string    `json:"error,omitempty"`
	CheckedAt    time.Time `json:"checked_at,omitzero"`
	// Skip lists commits that were rolled back. The daemon never installs
	// them on its own again; `locus update` by hand still does.
	Skip []string `json:"skip,omitempty"`
}

func (s *savedState) skipped(commit string) bool {
	return slices.ContainsFunc(s.Skip, func(c string) bool { return sameCommit(c, commit) })
}

func (s *savedState) addSkip(commit string) {
	if commit == "" || s.skipped(commit) {
		return
	}
	s.Skip = append(s.Skip, commit)
	// Only recent rollbacks matter; a newer build supersedes the rest.
	if len(s.Skip) > 20 {
		s.Skip = s.Skip[len(s.Skip)-20:]
	}
}

func loadState(dataDir string) savedState {
	var s savedState
	if dataDir == "" {
		return s
	}
	b, err := os.ReadFile(filepath.Join(dataDir, stateFile))
	if err == nil {
		_ = json.Unmarshal(b, &s)
	}
	return s
}

func saveState(dataDir string, s savedState) error {
	if dataDir == "" {
		return nil
	}
	return writeJSONFile(filepath.Join(dataDir, stateFile), s)
}

// writeJSONFile replaces path atomically with v as JSON, readable by its
// owner only.
func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// MaxFailedStarts is how many starts in a row a newly installed binary may
// fail to reach serving before the next start rolls it back.
const MaxFailedStarts = 3

// healthMarker is written when a swap installs a new binary and removed
// once that binary is serving. Every start in between counts.
type healthMarker struct {
	Commit    string    `json:"commit"`
	Version   string    `json:"version"`
	Starts    int       `json:"starts"`
	SwappedAt time.Time `json:"swapped_at"`
}

func readHealth(dataDir string) (*healthMarker, error) {
	b, err := os.ReadFile(filepath.Join(dataDir, healthFile))
	if err != nil {
		return nil, err
	}
	var m healthMarker
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func writeHealth(dataDir string, m *healthMarker) error {
	return writeJSONFile(filepath.Join(dataDir, healthFile), m)
}

func removeHealth(dataDir string) bool {
	return os.Remove(filepath.Join(dataDir, healthFile)) == nil
}

// StartupCheck runs when `locus serve` starts, before anything else. After a
// swap it counts the start; when the new binary has already failed to reach
// serving MaxFailedStarts times in a row, it puts the previous binary back
// instead, remembers not to install that build again on its own, and
// reports true: the caller then re-executes exe, which is now the previous
// build. A marker left by another build than the running one (the binary
// was replaced by hand since) is dropped.
func StartupCheck(dataDir, exe string, running Build, log *slog.Logger) (rolledBack bool, err error) {
	m, err := readHealth(dataDir)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		removeHealth(dataDir)
		return false, nil
	}
	if running.Commit != "" && !sameCommit(m.Commit, running.Commit) {
		removeHealth(dataDir)
		return false, nil
	}
	if m.Starts < MaxFailedStarts {
		m.Starts++
		return false, writeHealth(dataDir, m)
	}
	removeHealth(dataDir)
	if err := Rollback(exe); err != nil {
		log.Error("update: new build failed to start and cannot be rolled back", "version", m.Version, "failed_starts", m.Starts, "err", err)
		return false, err
	}
	s := loadState(dataDir)
	s.addSkip(m.Commit)
	s.State, s.Error = StateRolledBack, "build "+m.Version+" did not reach serving in "+strconv.Itoa(m.Starts)+" starts"
	_ = saveState(dataDir, s)
	log.Warn("update: new build did not reach serving; rolled back to the previous build", "version", m.Version, "failed_starts", m.Starts, "binary", exe)
	return true, nil
}

// MarkServing records that the running binary reached serving: a pending
// health marker is removed, and true is returned when there was one.
func MarkServing(dataDir string) bool { return removeHealth(dataDir) }

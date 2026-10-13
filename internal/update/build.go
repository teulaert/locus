package update

import (
	"runtime/debug"
	"strings"
	"time"
)

// BuildCommit and BuildTime identify the commit this binary was built from:
// its full hash and its committer time (RFC 3339). Release builds and `make
// build` set them with -ldflags -X; a binary built otherwise inside a git
// checkout falls back to the VCS stamp the go command records.
var (
	BuildCommit = ""
	BuildTime   = ""
)

// Build describes a binary: the version string it reports and the commit it
// was built from. Time orders builds; a release whose commit is not newer
// than the running build's is never installed, except by a rollback.
type Build struct {
	Version string    `json:"version"`
	Commit  string    `json:"commit,omitempty"`
	Time    time.Time `json:"commit_time,omitempty"`
}

// Running describes the running binary, whose version string is version.
func Running(version string) Build {
	b := Build{Version: version, Commit: strings.TrimSpace(BuildCommit)}
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(BuildTime)); err == nil {
		b.Time = t
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if b.Commit == "" {
					b.Commit = s.Value
				}
			case "vcs.time":
				if t, err := time.Parse(time.RFC3339, s.Value); err == nil && b.Time.IsZero() {
					b.Time = t
				}
			}
		}
	}
	return b
}

// sameCommit reports whether two commit hashes name the same commit, either
// possibly abbreviated (at least seven characters).
func sameCommit(a, b string) bool {
	if len(a) < 7 || len(b) < 7 {
		return false
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	return strings.HasPrefix(b, a)
}

# Updates

## Keeping peers up to date

Releases are built, signed and published by CI, never on a developer's
machine, and peers update themselves from them: run `locus update` on a
peer, or set `update.auto` and let the daemons do it.

**Where releases come from.** Every push to `main` that passes the checks
becomes a GitHub prerelease tagged `build-<sha7>` (channel `main`); every
tag `v*` becomes a release (channel `stable`). Each carries the binaries
`locus-linux-amd64`, `locus-linux-arm64`, `locus-darwin-arm64` and
`locus-darwin-amd64`, `manifest.json` (version, commit, commit time,
channel, and each binary's size and SHA-256), `manifest.sig` and
`SHA256SUMS`. Only the newest 20 `build-*` releases are kept.

**Trust.** `manifest.sig` is an ed25519 signature over the exact bytes of
`manifest.json`, made with the release key. Its public half is compiled
into every binary (`internal/update/release_pubkey.go`); its private half
exists only on the self-hosted CI runner that signs releases (or as a
repository secret), never on a peer or a developer machine. A peer installs
a binary only when the manifest verifies with the key compiled into the
binary it runs *and* the download has the manifest's size and SHA-256, and
only when its commit is newer than the running build's: an old release
served again is never installed. Peers never send each other binaries. A
build with no release key compiled in (a development build) cannot update:
`locus update` exits 4 with "no release key compiled in", and its daemon
never checks.

**Checking and updating by hand.**

```bash
locus update --check      # available: build-1a2b3c4 (version 1a2b3c4); running e8c67fb, channel main
locus update              # download, verify, swap, and restart the daemon into it
locus update --rollback   # back to the previous binary (locus.previous)
locus status              # ... update  available: build-1a2b3c4 (auto off; run: locus update)
```

With a daemon running on the data directory, `locus update` asks it
(`POST /api/v1/update`) to do the whole thing, and it restarts into the new
binary; with none, the binary is swapped and the next start runs it. On a
system install run it as the service user, like every command:
`sudo runuser -u locus -- locus --config /etc/locus/config.json update`
(or the `/usr/local/sbin/locus` wrapper).

**Automatic updates** are off by default. Turn them on per peer:

```json
"update": { "auto": true, "channel": "main" }
```

and restart the daemon. It checks every `update.interval` (default `30m`;
the GitHub API allows 60 unauthenticated requests an hour, and a check that
finds nothing new is a conditional request GitHub does not count). A new
build is installed after a random delay of up to ten minutes, so peers do
not all restart at once; a rendezvous peer waits one more interval, so the
hub is never first. When GitHub rate-limits a check, the daemon waits for
the time GitHub names. With `auto` off the daemon still checks and `locus
status` says when a build is available. `channel` is `main` (every build of
main) or `stable` (`v*` tags only); `url` replaces the GitHub API base for a
mirror.

**What an update does.** The new binary is downloaded next to the running
one as `locus.new`, verified, and swapped in: the running binary is kept as
`locus.previous` (replacing an older one) and the new one is renamed over
it, so the path never points at a partial or missing binary. The daemon
then shuts down exactly as on SIGTERM and re-executes the new binary with
the same arguments and environment in the same process: systemd and launchd
see the same pid carry on and do not count a restart. (Where re-executing
fails, or on Windows, it exits 0 and the service manager starts the new
binary; the user unit uses `Restart=always` for this, and the launchd
plist's `KeepAlive` restarts it anyway.) A new binary that fails to reach
serving three starts in a row is rolled back on the next start, with a
warning in the log; the daemon then does not install that build again on
its own, though the next newer build, or `locus update` by hand, will.

**Where the binary must live.** The daemon replaces the file it runs, so it
must be writable by the user the service runs as. A user install
(`~/.local/bin/locus` with the systemd user unit or the launchd plist)
already is. A system install runs `/var/lib/locus/bin/locus`, owned by the
`locus` user, with `/usr/local/bin/locus` a symlink to it for the CLI
(`make install-system` and `locus init --system` set this up, and
`contrib/systemd/locus.service` runs it). An existing system install that
runs `/usr/local/bin/locus` moves over with:

```bash
sudo install -D -o locus -g locus -m 0755 /usr/local/bin/locus /var/lib/locus/bin/locus
sudo ln -sfn /var/lib/locus/bin/locus /usr/local/bin/locus
sudo install -m 0644 contrib/systemd/locus.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart locus
```

(Keep any drop-ins from `systemctl edit locus`; only `ExecStart` changes.
A `/usr/local/sbin/locus` runuser wrapper keeps working unchanged.)

**Maintainers: the release pipeline.** `.github/workflows/release.yml` runs
on pushes to `main` and on `v*` tags, never on pull requests. It first runs
the checks of `ci.yml` (on GitHub-hosted runners) and only then the release
job on the self-hosted runner with the labels `self-hosted, linux, locus`:
`make dist`, `locus-release manifest`, `locus-release sign`, `locus-release
verify` (with the key compiled into the binaries) and `gh release create`.
The signing key is read from the file `$LOCUS_RELEASE_KEY_FILE`
(`/etc/locus-release/key` on the runner: the base64 private key, mode 600,
owned by the runner's user), or else from the repository secret
`LOCUS_RELEASE_KEY`. To set up or rotate the key:

```bash
make release-keygen       # public <hex>, private <base64>
# put the private line in /etc/locus-release/key on the runner (chmod 600),
# set ReleasePublicKey in internal/update/release_pubkey.go to the public hex,
# commit and push: that build is the first one peers can verify.
```

Peers running a build with an older key cannot verify releases signed with a
new one; install one build by hand on each after a rotation. The same tool
checks a release locally: `make dist release-manifest`, then
`make release-sign` and `make release-verify`.

## Upgrading

* **Hidden files leave the index and stop syncing.** Builds before hidden
  paths were skipped inventoried, indexed and synchronized everything under a
  root, `.git` included. After upgrading, the first reconciliation pass
  retires the records of hidden paths as if the files had been deleted, so
  they disappear from search results and `locus status` counts. The files
  themselves are not touched, and the retirement is never sent to peers, so
  no peer deletes its copies either. Set `"sync_hidden": true` on a root to
  keep the old behaviour for it.
  Until every peer is upgraded, older peers keep offering their hidden files
  each round and upgraded peers refuse them, logging it once per peer.
* **Peer exchange needs upgraded peers.** Older builds neither send nor read
  peer lists (a responder only includes one for a prober that announces it
  understands it), and they do not announce their discovery port, so they
  are not learned from inbound probes either. Keep listing them in
  `discovery.peers` until they are upgraded; everything else works as before.

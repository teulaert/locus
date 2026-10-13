# Running Locus as a service

`contrib/` holds service definitions for the three usual setups:

| File | For |
| --- | --- |
| `contrib/systemd/locus-user.service` | Linux desktop or laptop, runs as you (systemd user unit) |
| `contrib/launchd/io.github.teulaert.locus.plist` | macOS, runs as you (LaunchAgent) |
| `contrib/systemd/locus.service` | always-on server peer or rendezvous peer, runs as a dedicated `locus` user (see [Server peers](server-peer.md)) |
| `contrib/config.system.json` | configuration template for the server peer |

**Linux, per user:**

```bash
make install
locus init --root ~/Documents
install -Dm644 contrib/systemd/locus-user.service ~/.config/systemd/user/locus.service
systemctl --user daemon-reload && systemctl --user enable --now locus
loginctl enable-linger "$USER"   # optional: keep running while logged out
```

**macOS:** launchd expands neither `~` nor `$HOME`, so the plist names
`/Users/USER`; substitute your account name. It runs
`~/.local/bin/locus serve` at login, restarts it when it exits (no more often
than every 10 seconds) and logs to `~/Library/Logs/locus/`.

```bash
make install
locus init --root ~/Documents
mkdir -p ~/Library/Logs/locus
sed "s/USER/$(whoami)/g" contrib/launchd/io.github.teulaert.locus.plist \
  > ~/Library/LaunchAgents/io.github.teulaert.locus.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/io.github.teulaert.locus.plist
```

`launchctl bootout gui/$(id -u)/io.github.teulaert.locus` stops it.

# Locus — local-first peer-to-peer documents, search and sync.
#
# The default runtime is pure Go (no cgo), so building and testing needs
# nothing but a Go toolchain.

GO           ?= go
BIN          ?= bin/locus
PKG          := ./...
# Where `make install` puts the binary for the current user.
INSTALL_DIR  ?= $(HOME)/.local/bin
# Where `make install-system` puts the binary, the systemd unit and the
# configuration template. DESTDIR stages the install for packaging.
DESTDIR            ?=
SYSTEM_BIN_DIR     ?= /usr/local/bin
SYSTEMD_UNIT_DIR   ?= /etc/systemd/system
SYSTEM_CONFIG_DIR  ?= /etc/locus
# The binary the system service runs: in its own state directory and owned
# by the service user, so the daemon can replace it when it updates itself.
# SYSTEM_BIN_DIR/locus becomes a symlink to it for the CLI.
SERVICE_USER       ?= locus
SERVICE_BIN_DIR    ?= /var/lib/locus/bin
# Stamp releases from git; a plain checkout falls back to the development
# version. Only v* tags name versions (CI's build-<sha> tags do not).
VERSION      ?= $(shell git describe --tags --match 'v*' --always --dirty 2>/dev/null || echo 0.1.0-dev)
# The commit and its time order builds for auto-update: a release is only
# installed over an older commit.
COMMIT       ?= $(shell git rev-parse HEAD 2>/dev/null)
COMMIT_TIME  ?= $(shell git show -s --format=%cI HEAD 2>/dev/null)
LDFLAGS      := -s -w \
	-X github.com/teulaert/locus/internal/sync.BuildVersion=$(VERSION) \
	-X github.com/teulaert/locus/internal/update.BuildCommit=$(COMMIT) \
	-X github.com/teulaert/locus/internal/update.BuildTime=$(COMMIT_TIME)
# Release binaries: every platform CI publishes, cross-compiled (pure Go).
DIST              ?= dist
RELEASE_PLATFORMS ?= linux/amd64 linux/arm64 darwin/arm64 darwin/amd64
RELEASE           := $(GO) run ./cmd/locus-release
# staticcheck runs at a pinned version through `go run`, so it needs no global
# install. It requires a newer Go than go.mod does; GOTOOLCHAIN=auto lets the
# go command fetch that toolchain when the local one is older.
STATICCHECK_VERSION ?= v0.8.1
STATICCHECK  ?= GOTOOLCHAIN=auto $(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
# Tests never touch the real network: no targets derived from the
# routing table or `wg show`, and only loopback addresses on non-default
# ports are probed or listened on, so a test run cannot reach the daemon of
# the machine it runs on or the peers that daemon talks to.
TEST_ENV     := LOCUS_TEST_NO_NETWORK=1

.PHONY: all build install install-system dist release-manifest release-keygen release-sign release-verify test test-short test-race test-verbose vet fmt fmt-check staticcheck tidy-check lint check skill skill-check clean

all: check build

## build: compile the single locus binary into ./bin
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/locus

## install: install the binary for the current user (~/.local/bin/locus)
install: build
	@mkdir -p $(INSTALL_DIR)
	cp -f $(BIN) $(INSTALL_DIR)/locus
	@echo "installed $(INSTALL_DIR)/locus"
	@case ":$$PATH:" in \
		*":$(INSTALL_DIR):"*) ;; \
		*) echo "note: $(INSTALL_DIR) is not on PATH; add it or run $(INSTALL_DIR)/locus" ;; \
	esac

## install-system: install binary, system unit and config template (as root, after `make build`)
# It does not build, so `sudo` does not need a Go toolchain or leave
# root-owned build output, and it never replaces an existing config.json.
# The service binary goes to SERVICE_BIN_DIR, owned by SERVICE_USER when that
# user exists (create it first); SYSTEM_BIN_DIR/locus links to it.
install-system:
	@test -x $(BIN) || { echo "$(BIN) is missing; run make build first (as yourself, not root)"; exit 1; }
	install -D -m 0755 $(BIN) $(DESTDIR)$(SERVICE_BIN_DIR)/locus
	@if [ -z "$(DESTDIR)" ]; then \
		if id -u $(SERVICE_USER) >/dev/null 2>&1; then \
			chown $(SERVICE_USER):$(SERVICE_USER) $(SERVICE_BIN_DIR) $(SERVICE_BIN_DIR)/locus; \
		else \
			echo "note: user $(SERVICE_USER) does not exist yet; after useradd: chown -R $(SERVICE_USER):$(SERVICE_USER) $(SERVICE_BIN_DIR)"; \
		fi; \
	fi
	install -d $(DESTDIR)$(SYSTEM_BIN_DIR)
	ln -sfn $(SERVICE_BIN_DIR)/locus $(DESTDIR)$(SYSTEM_BIN_DIR)/locus
	install -D -m 0644 contrib/systemd/locus.service $(DESTDIR)$(SYSTEMD_UNIT_DIR)/locus.service
	install -D -m 0644 contrib/config.system.json $(DESTDIR)$(SYSTEM_CONFIG_DIR)/config.json.example
	@echo "installed $(DESTDIR)$(SERVICE_BIN_DIR)/locus (linked from $(DESTDIR)$(SYSTEM_BIN_DIR)/locus), $(DESTDIR)$(SYSTEMD_UNIT_DIR)/locus.service"
	@echo "next: useradd --system --home-dir /var/lib/locus --shell /usr/sbin/nologin locus   (if it does not exist)"
	@echo "      locus init --system --root <dir>   (or edit $(SYSTEM_CONFIG_DIR)/config.json.example into config.json)"
	@echo "      locus init --system --rendezvous   (instead, on the hub: discovery only, no roots)"
	@echo "      systemctl daemon-reload && systemctl enable --now locus"

## dist: cross-compile the release binaries into $(DIST)/locus-<os>-<arch>
dist:
	rm -rf $(DIST)
	mkdir -p $(DIST)
	@for p in $(RELEASE_PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "build $(DIST)/locus-$$os-$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(DIST)/locus-$$os-$$arch ./cmd/locus || exit 1; \
	done

## release-manifest: write $(DIST)/manifest.json and SHA256SUMS (CHANNEL=main|stable)
CHANNEL ?= main
release-manifest:
	$(RELEASE) manifest -dir $(DIST) -version "$(VERSION)" -commit "$(COMMIT)" -commit-time "$(COMMIT_TIME)" -channel $(CHANNEL)

## release-keygen: print a new release key pair (public hex to commit, private base64 to keep secret)
release-keygen:
	@$(RELEASE) keygen

## release-sign: sign $(DIST)/manifest.json with $$LOCUS_RELEASE_KEY_FILE or $$LOCUS_RELEASE_KEY
release-sign:
	$(RELEASE) sign -in $(DIST)/manifest.json -out $(DIST)/manifest.sig

## release-verify: verify $(DIST) with the key compiled into locus (PUBKEY=<hex> to override)
release-verify:
	$(RELEASE) verify -dir $(DIST) $(if $(PUBKEY),-pubkey $(PUBKEY))

## test: unit + acceptance scenarios (the scenarios build the real binary)
test:
	$(TEST_ENV) CGO_ENABLED=0 $(GO) test $(PKG) -count=1

test-verbose:
	$(TEST_ENV) CGO_ENABLED=0 $(GO) test $(PKG) -count=1 -v

## test-short: skip the slow 10k-document indexing scenario
test-short:
	$(TEST_ENV) CGO_ENABLED=0 $(GO) test $(PKG) -count=1 -short

## test-race: short suite under the race detector (needs cgo and a C compiler)
test-race:
	$(TEST_ENV) CGO_ENABLED=1 $(GO) test $(PKG) -count=1 -short -race

vet:
	$(GO) vet $(PKG)

fmt:
	$(GO) fmt $(PKG)

fmt-check:
	@out=$$(gofmt -l . 2>/dev/null); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

## staticcheck: run the pinned staticcheck over every package
staticcheck:
	$(STATICCHECK) $(PKG)

## tidy-check: fail if go.mod or go.sum differ from what `go mod tidy` writes
tidy-check:
	$(GO) mod tidy -diff

## lint: vet, formatting, documentation drift, go.mod tidiness and staticcheck
lint: vet fmt-check skill-check tidy-check staticcheck

## check: everything CI runs
check: lint test

# The agent skill is embedded in the binary (internal/agent/SKILL.md) and the
# repository root copy is generated from it, so the two can never disagree
# about what `locus skill` prints.

## skill: regenerate the root SKILL.md from the embedded copy
skill:
	cp internal/agent/SKILL.md SKILL.md
	@echo "wrote SKILL.md from internal/agent/SKILL.md"

## skill-check: fail if the root SKILL.md drifted from the embedded one
skill-check:
	@if ! cmp -s internal/agent/SKILL.md SKILL.md; then \
		echo "SKILL.md is out of date; run: make skill"; exit 1; \
	fi

clean:
	rm -rf bin $(DIST)
	$(GO) clean

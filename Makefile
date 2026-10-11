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
# Stamp releases from git; a plain checkout falls back to the development version.
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 0.1.0-dev)
# staticcheck runs at a pinned version through `go run`, so it needs no global
# install. It requires a newer Go than go.mod does; GOTOOLCHAIN=auto lets the
# go command fetch that toolchain when the local one is older.
STATICCHECK_VERSION ?= v0.8.1
STATICCHECK  ?= GOTOOLCHAIN=auto $(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)

.PHONY: all build install install-system test test-short test-race test-verbose vet fmt fmt-check staticcheck tidy-check lint check skill skill-check clean

all: check build

## build: compile the single locus binary into ./bin
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w -X github.com/teulaert/locus/internal/sync.BuildVersion=$(VERSION)" -o $(BIN) ./cmd/locus

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
install-system:
	@test -x $(BIN) || { echo "$(BIN) is missing; run make build first (as yourself, not root)"; exit 1; }
	install -D -m 0755 $(BIN) $(DESTDIR)$(SYSTEM_BIN_DIR)/locus
	install -D -m 0644 contrib/systemd/locus.service $(DESTDIR)$(SYSTEMD_UNIT_DIR)/locus.service
	install -D -m 0644 contrib/config.system.json $(DESTDIR)$(SYSTEM_CONFIG_DIR)/config.json.example
	@echo "installed $(DESTDIR)$(SYSTEM_BIN_DIR)/locus, $(DESTDIR)$(SYSTEMD_UNIT_DIR)/locus.service"
	@echo "next: useradd --system --home-dir /var/lib/locus --shell /usr/sbin/nologin locus"
	@echo "      locus init --system --root <dir>   (or edit $(SYSTEM_CONFIG_DIR)/config.json.example into config.json)"
	@echo "      systemctl daemon-reload && systemctl enable --now locus"

## test: unit + acceptance scenarios (the scenarios build the real binary)
test:
	CGO_ENABLED=0 $(GO) test $(PKG) -count=1

test-verbose:
	CGO_ENABLED=0 $(GO) test $(PKG) -count=1 -v

## test-short: skip the slow 10k-document indexing scenario
test-short:
	CGO_ENABLED=0 $(GO) test $(PKG) -count=1 -short

## test-race: short suite under the race detector (needs cgo and a C compiler)
test-race:
	CGO_ENABLED=1 $(GO) test $(PKG) -count=1 -short -race

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
	rm -rf bin
	$(GO) clean

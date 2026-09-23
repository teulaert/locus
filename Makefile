# Locus — local-first peer-to-peer documents, search and sync.
#
# The default runtime is pure Go (no cgo), so building and testing needs
# nothing but a Go toolchain.

GO           ?= go
BIN          ?= bin/locus
PKG          := ./...
# Where `make install` puts the binary for the current user.
INSTALL_DIR  ?= $(HOME)/.local/bin
# Stamp releases from git; a plain checkout falls back to the development version.
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 0.1.0-dev)

.PHONY: all build install test test-short test-verbose vet fmt fmt-check lint check skill clean

all: check build

## build: compile the single locus binary into ./bin
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w -X locus/internal/sync.BuildVersion=$(VERSION)" -o $(BIN) ./cmd/locus

## install: install the binary for the current user (~/.local/bin/locus)
install: build
	@mkdir -p $(INSTALL_DIR)
	cp -f $(BIN) $(INSTALL_DIR)/locus
	@echo "installed $(INSTALL_DIR)/locus"
	@case ":$$PATH:" in \
		*":$(INSTALL_DIR):"*) ;; \
		*) echo "note: $(INSTALL_DIR) is not on PATH; add it or run $(INSTALL_DIR)/locus" ;; \
	esac

## test: unit + acceptance scenarios (the scenarios build the real binary)
test:
	CGO_ENABLED=0 $(GO) test $(PKG) -count=1

test-verbose:
	CGO_ENABLED=0 $(GO) test $(PKG) -count=1 -v

## test-short: skip the slow 10k-document indexing scenario
test-short:
	CGO_ENABLED=0 $(GO) test $(PKG) -count=1 -short

vet:
	$(GO) vet $(PKG)

fmt:
	$(GO) fmt $(PKG)

fmt-check:
	@out=$$(gofmt -l . 2>/dev/null); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

## lint: vet plus the formatting and documentation-drift checks
lint: vet fmt-check skill-check

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

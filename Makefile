# Copyright (c) 2026 Go-Helix Authors
#
# This file is part of go-helix.
# Licensed under the MIT License. See the LICENSE file in the project root
# for the full license text.

.PHONY: build build-linux clean

BIN := bin

# Resolve the Go toolchain even when `go` is not on PATH, so `make build`
# works from a fresh terminal without a manual `export PATH=...`.
GO := $(shell command -v go 2>/dev/null)
ifeq ($(strip $(GO)),)
GO := $(firstword $(wildcard $(GOROOT)/bin/go $(HOME)/sdk/go1.27.1/bin/go /usr/local/go/bin/go $(HOME)/go/bin/go))
endif

# The project ships on Linux, so `make build` cross-compiles static Linux
# x86_64 binaries (CGO disabled). Outputs keep the fixed names bin/manager
# and bin/agent — no OS/arch suffix.
build:
ifeq ($(strip $(GO)),)
	$(error go toolchain not found; install Go or set GOROOT/PATH)
endif
	@mkdir -p $(BIN)
	@echo "Using Go: $$($(GO) version)"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN)/manager ./cmd/manager
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN)/agent ./cmd/agent

# Backwards-compatible alias; identical Linux output as `build`.
build-linux: build

clean:
	rm -rf $(BIN)/ data/ *.log
	pkill -f "$(BIN)/manager" || true
	pkill -f "$(BIN)/agent" || true

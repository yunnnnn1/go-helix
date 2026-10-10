#!/bin/bash

# Copyright (c) 2026 Go-Helix Authors
#
# This file is part of go-helix.
# Licensed under the MIT License. See the LICENSE file in the project root
# for the full license text.

# Build script for Manager and Agent.
# The project ships on Linux, so this always cross-compiles static Linux
# x86_64 binaries (CGO disabled) with fixed names: bin/manager, bin/agent
# — no OS/arch suffix.

set -e

# Locate the Go toolchain even when `go` is not on PATH, so this script works
# from a fresh terminal without a manual `export PATH=...`.
find_go_bin() {
    if command -v go >/dev/null 2>&1; then
        command -v go
        return
    fi
    if [ -n "$GOROOT" ] && [ -x "$GOROOT/bin/go" ]; then
        echo "$GOROOT/bin/go"
        return
    fi
    for dir in \
        "$HOME/sdk/go1.27.1" \
        /usr/local/go \
        /opt/homebrew \
        "$HOME/go"; do
        if [ -x "$dir/bin/go" ]; then
            echo "$dir/bin/go"
            return
        fi
    done
    echo ""
}

GO_BIN="$(find_go_bin)"
if [ -z "$GO_BIN" ]; then
    echo "Error: go toolchain not found. Install Go or set GOROOT/PATH." >&2
    exit 127
fi
export PATH="$(dirname "$GO_BIN"):$PATH"
echo "Using Go: $($GO_BIN version)"

export CGO_ENABLED=0
export GOOS=linux
export GOARCH=amd64

echo "Cleaning previous builds..."
rm -rf bin/
mkdir -p bin/

echo "Building Manager (linux/amd64)..."
"$GO_BIN" build -trimpath -ldflags="-s -w" -o bin/manager ./cmd/manager
echo "Manager built successfully."

echo "Building Agent (linux/amd64)..."
"$GO_BIN" build -trimpath -ldflags="-s -w" -o bin/agent ./cmd/agent
echo "Agent built successfully."

echo "All builds completed. Static linux/amd64 binaries are in the bin/ directory."

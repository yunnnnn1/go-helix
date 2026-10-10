#!/bin/bash

# Copyright (c) 2026 Go-Helix Authors
#
# This file is part of go-helix.
# Licensed under the MIT License. See the LICENSE file in the project root
# for the full license text.

# Start Agent Script

# Kill previous agent instances
echo "Stopping existing agents..."
pkill -f "bin/agent"

# Ensure binaries exist
if [ ! -f "bin/agent" ]; then
    echo "Error: bin/agent not found. Please run ./build.sh first."
    exit 1
fi

echo "Starting Agents..."

# Start Agent using config_agent.json
# Thanks to the new Auto-Discovery and Multi-Instance architecture, 
# a single Agent process can monitor multiple database instances configured in the 'instances' array,
# or automatically discover them if the array is empty.

echo "Starting Agent with config_agent.json..."
nohup ./bin/agent -config ./etc/config_agent.json  &
PID1=$!
echo "Agent started with PID $PID1"

echo "Agents started successfully!"
echo "Logs:"
echo "  - agent.log"
echo "You can tail agent.log to see heartbeat reporting and auto-discovery logs."

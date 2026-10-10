#!/bin/bash

# Copyright (c) 2026 Go-Helix Authors
#
# This file is part of go-helix.
# Licensed under the MIT License. See the LICENSE file in the project root
# for the full license text.

# Start Manager Cluster Script

# Kill previous manager instances
echo "Stopping existing managers..."
pkill -f "bin/manager"

# Ensure binaries exist
if [ ! -f "bin/manager" ]; then
    echo "Error: bin/manager not found. Please run ./build.sh first."
    exit 1
fi

echo "Starting Manager Cluster..."

# Start Manager 1 (Bootstrap / Leader)
echo "Starting Manager 1 (Leader) with config_manager.json..."
nohup ./bin/manager --config ./etc/config_manager.json &
PID1=$!
echo "Manager 1 started with PID $PID1"

# Start Manager 2
echo "Starting Manager 2 with config_manager2.json..."
nohup ./bin/manager --config ./etc/config_manager2.json &
PID2=$!
echo "Manager 2 started with PID $PID2"

# Start Manager 3
echo "Starting Manager 3 with config_manager3.json..."
nohup ./bin/manager --config ./etc/config_manager3.json &
PID3=$!
echo "Manager 3 started with PID $PID3"

echo "Manager Cluster started successfully!"
echo "Logs:"
echo "  - manager1.log"
echo "  - manager2.log"
echo "  - manager3.log"
echo "You can tail manager1.log to see the leader election process."

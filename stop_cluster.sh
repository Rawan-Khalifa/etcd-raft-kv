#!/bin/bash
# Stop all nodes

echo "Stopping Raft cluster..."
pkill -f "http_server.py"
sleep 1
echo "✅ Cluster stopped!"
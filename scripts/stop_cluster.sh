#!/bin/bash
# Stop all nodes

echo "Stopping Raft cluster..."
pkill -f "start_node1.py"
pkill -f "start_node2.py"
pkill -f "start_node3.py"
sleep 1
echo "✅ Cluster stopped!"
#!/bin/bash

echo "Starting 3-node Raft cluster..."

# Kill existing
pkill -f "start_node" 2>/dev/null
sleep 1

# Start nodes
python3 start_node1.py > node1.log 2>&1 &
echo "Started node1 (PID: $!)"

python3 start_node2.py > node2.log 2>&1 &
echo "Started node2 (PID: $!)"

python3 start_node3.py > node3.log 2>&1 &
echo "Started node3 (PID: $!)"

sleep 3

echo ""
echo "✅ Cluster started!"
echo ""
echo "Monitor with: python demo_visualizer.py"
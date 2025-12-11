#!/bin/bash

echo "Starting 3-node Raft cluster with gRPC..."
echo

# Kill existing processes
pkill -f "start_node" 2>/dev/null || true
sleep 1

# Compile proto files if needed
if [ ! -f "raft_pb2.py" ]; then
    echo "Compiling Protocol Buffers..."
    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
fi
echo

# Start nodes
echo "Starting nodes..."
.venv/bin/python start_node1.py > node1.log 2>&1 &
NODE1_PID=$!
echo "  Started node1 (PID: $NODE1_PID, gRPC: localhost:9001)"

.venv/bin/python start_node2.py > node2.log 2>&1 &
NODE2_PID=$!
echo "  Started node2 (PID: $NODE2_PID, gRPC: localhost:9002)"

.venv/bin/python start_node3.py > node3.log 2>&1 &
NODE3_PID=$!
echo "  Started node3 (PID: $NODE3_PID, gRPC: localhost:9003)"

sleep 3

echo
echo "================================================"
echo "✅ Cluster started with gRPC transport!"
echo "================================================"
echo
echo "Node Status:"
echo "  Node1: localhost:9001 (gRPC)"
echo "  Node2: localhost:9002 (gRPC)"
echo "  Node3: localhost:9003 (gRPC)"
echo
echo "Commands:"
echo "  Monitor:  python demo_visualizer.py"
echo "  View logs: tail -f node1.log"
echo "  Stop:     ./stop_cluster.sh"
echo
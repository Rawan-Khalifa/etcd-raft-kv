#!/bin/bash
# Compile Protocol Buffer definitions for gRPC

set -e

# Change to repo root
cd "$(dirname "$0")/.."

echo "Installing gRPC tools..."
pip install grpcio grpcio-tools

echo "Compiling Proto definitions..."
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft/proto/raft.proto

# Move generated files to the right location
mv raft/proto/raft_pb2.py raft/proto/generated/ 2>/dev/null || true
mv raft/proto/raft_pb2_grpc.py raft/proto/generated/ 2>/dev/null || true

echo "✓ Protocol buffers compiled successfully"
echo "Generated files:"
echo "  - raft/proto/generated/raft_pb2.py (protocol buffer messages)"
echo "  - raft/proto/generated/raft_pb2_grpc.py (gRPC service definitions)"

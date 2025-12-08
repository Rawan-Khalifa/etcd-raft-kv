#!/bin/bash
# Compile Protocol Buffer definitions for gRPC

set -e

echo "Installing gRPC tools..."
pip install grpcio grpcio-tools

echo "Compiling Proto definitions..."
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

echo "✓ Protocol buffers compiled successfully"
echo "Generated files:"
echo "  - raft_pb2.py (protocol buffer messages)"
echo "  - raft_pb2_grpc.py (gRPC service definitions)"

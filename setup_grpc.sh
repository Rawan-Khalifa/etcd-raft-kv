#!/bin/bash
# Complete gRPC setup script

set -e

echo "================================================"
echo "gRPC Migration Setup"
echo "================================================"
echo

# 1. Install dependencies
echo "1️⃣  Installing gRPC dependencies..."
pip install -q grpcio grpcio-tools google-protobuf
echo "   ✓ Dependencies installed"
echo

# 2. Compile proto files
echo "2️⃣  Compiling Protocol Buffer definitions..."
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
echo "   ✓ Generated raft_pb2.py"
echo "   ✓ Generated raft_pb2_grpc.py"
echo

# 3. Verify imports
echo "3️⃣  Verifying gRPC imports..."
python3 << 'EOF'
try:
    import raft_pb2
    import raft_pb2_grpc
    print("   ✓ raft_pb2 imported successfully")
    print("   ✓ raft_pb2_grpc imported successfully")
except ImportError as e:
    print(f"   ✗ Import error: {e}")
    exit(1)
EOF
echo

# 4. Test gRPC setup
echo "4️⃣  Testing gRPC setup..."
python3 << 'EOF'
try:
    from raft_grpc_client import RaftGRPCClient
    from raft_grpc_server import create_grpc_server
    print("   ✓ RaftGRPCClient imported")
    print("   ✓ create_grpc_server imported")
except ImportError as e:
    print(f"   ✗ Import error: {e}")
    exit(1)
EOF
echo

echo "================================================"
echo "✅ gRPC setup complete!"
echo "================================================"
echo
echo "Next steps:"
echo "1. Start cluster: ./start_cluster.sh"
echo "2. Monitor: python demo_visualizer.py"
echo "3. Test: python test_grpc_integration.py"
echo
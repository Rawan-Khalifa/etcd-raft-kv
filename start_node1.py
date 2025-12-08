#!/usr/bin/env python3
"""Start node1 with gRPC transport"""
import sys
import time
from raft_node import RaftNode

if __name__ == "__main__":
    # gRPC peer addresses (format: host:port)
    peers = [
        "localhost:9002",  # node2
        "localhost:9003",  # node3
    ]
    
    node = RaftNode(
        node_id='node1',
        peers=peers,
        address='localhost:9001',  # gRPC address
        enable_persistence=True,
        snapshot_interval=100
    )
    
    print("Starting node1 with gRPC...")
    print("  gRPC server: localhost:9001 (inter-node Raft RPC)")
    print("  HTTP server: localhost:9010 (client APIs)")
    print("  Peers: localhost:9002, localhost:9003")
    print()
    
    node.start()
    
    print("✓ Node1 started and running...")
    print()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down node1...")
        node.stop()
        print("Node1 stopped.")
        sys.exit(0)
#!/usr/bin/env python3
"""Start node2 with gRPC transport"""
import sys
import time
from raft_node import RaftNode

if __name__ == "__main__":
    peers = [
        "localhost:9001",  # node1
        "localhost:9003",  # node3
    ]
    
    node = RaftNode(
        node_id='node2',
        peers=peers,
        address='localhost:9002',
        enable_persistence=True,
        snapshot_interval=100
    )
    
    print("Starting node2 with gRPC...")
    print("  gRPC server: localhost:9002")
    print("  Peers: " + ", ".join(peers))
    print()
    
    node.start()
    
    print("✓ Node2 started and running...")
    print()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down node2...")
        node.stop()
        print("Node2 stopped.")
        sys.exit(0)
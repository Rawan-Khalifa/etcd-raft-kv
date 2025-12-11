#!/usr/bin/env python3
"""Start node3 with gRPC transport"""
import sys
import time
sys.path.insert(0, '/Users/rwankhalifa/Documents/etcd-raft-kv')
from raft import RaftNode

if __name__ == "__main__":
    peers = [
        "localhost:9001",  # node1
        "localhost:9002",  # node2
    ]
    
    node = RaftNode(
        node_id='node3',
        peers=peers,
        address='localhost:9003',
        enable_persistence=True,
        snapshot_interval=100
    )
    
    print("Starting node3 with gRPC...")
    print("  gRPC server: localhost:9003 (inter-node Raft RPC)")
    print("  HTTP server: localhost:9012 (client APIs)")
    print("  Peers: localhost:9001, localhost:9002")
    print()
    
    node.start()
    
    print("✓ Node3 started and running...")
    print()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down node3...")
        node.stop()
        print("Node3 stopped.")
        sys.exit(0)
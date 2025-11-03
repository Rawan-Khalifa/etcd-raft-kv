#!/usr/bin/env python3
from raft_node import RaftNode
import time

if __name__ == "__main__":
    node = RaftNode(
        'node1',
        ['http://localhost:9002', 'http://localhost:9003'],
        'http://localhost:9001'
    )
    
    print("Starting node1...")
    print("  Combined server: http://localhost:9001")
    
    node.start()  # This now starts both Raft RPC and HTTP API server
    
    print("✓ Node1 started and running...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping node1...")
        node.stop()
        print("Node1 stopped.")
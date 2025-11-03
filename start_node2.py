#!/usr/bin/env python3
from raft_node import RaftNode
import time


if __name__ == "__main__":
    node = RaftNode(
        'node2',
        ['http://localhost:9001', 'http://localhost:9003'],
        'http://localhost:9002'
    )
    
    print("Starting node2...")

    node.start()

    print("✓ Node2 started and running...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping node2...")
        node.stop()
        print("Node2 stopped.")
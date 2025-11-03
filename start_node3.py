#!/usr/bin/env python3
from raft_node import RaftNode
import time


if __name__ == "__main__":
    node = RaftNode(
        'node3',
        ['http://localhost:9001', 'http://localhost:9002'],
        'http://localhost:9003'
    )

    print("Starting node3...")

    node.start()

    print("✓ Node3 started and running...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping node3...")
        node.stop()
        print("Node3 stopped.")
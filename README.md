# Distributed Key-Value Store with Raft Consensus

A fault-tolerant, distributed key-value store implementing the Raft consensus algorithm from scratch in Python.

## Quick Start

```bash
# 1. Setup environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Start the cluster
chmod +x start_cluster.sh stop_cluster.sh
./start_cluster.sh

# 3. Monitor cluster state (in another terminal)
python demo_visualizer.py

# 4. Stop the cluster
./stop_cluster.sh
```

## Development Journey

1. **Thread-safe KV Store** - Dictionary-based storage with Get/Put/Delete
2. **HTTP Server** - Using Python's `http.server`
3. **Command Layer** - Serializable operations with Log and State Machine
4. **Coordinator Integration** - Commands flow through coordinator
5. **Single Raft Node** - State machine (FOLLOWER/CANDIDATE/LEADER) with timeouts
6. **Multi-Node Raft** - RPC system, leader election, and log replication

## Testing

```bash
python test_raft_multinode.py    # Full integration tests # ignore the replication error that happens while cleaning up the nodes, that's expected behavior
```

The other tests were based on that stage so they might not work given the code has been updated. If you want to try out these tests, try going to the designated point of the code by reviewing the PRs.

## Features

- Leader election with automatic failover  
- Log replication with strong consistency  
- Fault tolerance (survives minority failures)  
- Real-time cluster visualization and state inspection  

Built following the [Raft paper](https://raft.github.io/raft.pdf) specification.

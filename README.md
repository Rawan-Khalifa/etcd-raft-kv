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

## Project Structure
```
├── kvstore.py              # Thread-safe in-memory key-value store
├── command.py              # Command abstraction (PUT/GET/DELETE)
├── log.py                  # Replicated log for storing commands
├── state_machine.py        # Applies commands to KV store
├── raft_node.py            # Core Raft implementation (leader election, log replication)
├── rpc.py                  # RPC message definitions
├── rpc_client.py           # HTTP-based RPC client
├── raft_http_server.py     # RPC server for inter-node communication
├── coordinator.py          # High-level interface (optional wrapper)
├── http_server.py          # initial HTTP server (no longer needed given the raft-http-server)
├── start_node{1,2,3}.py    # Individual node starter scripts
└── demo_visualizer.py      # Real-time cluster state monitor
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

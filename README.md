# Raft Replicated Key Value Store: etcd-like implementation

An implementation of the Raft consensus algorithm, featuring leader election, log replication, snapshots, dynamic membership, and lease-based reads.

## What This Project Delivers

**Two ways to use this project:**

### 1. **Distributed Key-Value Store** (Run as Application)
- 3-node cluster with automatic leader election and failover
- Simple CLI tool for interacting with the cluster
- HTTP API for client operations
- Lease-based reads for 10x faster performance
- Prometheus metrics for monitoring

### 2. **Raft Library** (Import in Your Code)
Build your own distributed systems on top of Raft consensus:
```python
from raft import RaftNode, Command, CommandType
# Create custom distributed applications
```
## Quickstart

### Installation

```bash
git clone https://github.com/Rawan-Khalifa/etcd-raft-kv.git
cd etcd-raft-kv
python3 -m venv .venv
source .venv/bin/activate
pip install -e .  # Installs library + CLI tool
```

### Running the Cluster

```bash
# Start 3-node cluster
./scripts/start_cluster.sh

# Run the demo visualizer in another tap
python ./scripts/demo_visualizer.py

# Interact using the CLI
raft-cli status                    # Check cluster health
raft-cli put key1 AmazingValue     # Write data
raft-cli get key1                  # Read data
raft-cli members                   # List cluster members

# Or use curl directly
curl http://localhost:9010/kv/mykey

# Stop cluster
./scripts/stop_cluster.sh
```

### Using as a Library

```python
from raft import RaftNode, Command, CommandType

# Create a Raft node
node = RaftNode(
    node_id="node1",
    peers=["localhost:9002", "localhost:9003"],
    address="localhost:9001",
    enable_persistence=True
)
node.start()

# Write data
cmd = Command(CommandType.PUT, "key", "value")
node.propose_command(cmd)

# Read data
value = node.get("key")
```

## Architecture 

```
   +-------------+                 +----------------+
   |  CLI/HTTP   |  REST (901x)    |  Demo Visualizer|
   +-------------+-----------------+----------------+
       |                                   |
       v                                   |
   +----------------------+                     |
   |  HTTP API / KVStore  |  writes/reads       |
   +----------------------+                     |
       |                                  stats
       v                                   |
    +--------------------+   gRPC (900x)   +--------------------+
    |    Raft Node 1     |<--------------->|    Raft Node 2     |
    | (leader candidate) |<--------------->| (follower/leader)  |
    +--------------------+                 +--------------------+
        |   ^                               |   ^
        v   |                               v   |
   WAL + B-Tree storage             WAL + B-Tree storage
   (raft_data/node1)                (raft_data/node2)
```
**Key Components:**
- **HTTP Server**: Client-facing API (ports 9010-9012)
- **gRPC Server**: Inter-node Raft RPCs (ports 9001-9003)
- **KVStore**: B-tree based key-value storage with WAL
- **Snapshots**: Automatic log compaction (after 100 entries)
- **Lease Manager**: Fast follower reads with lease validation (5 seconds)

## Features

| Feature | Description |
|---------|-------------|
| **Leader Election** | Automatic failover when leader crashes |
| **Log Replication** | Strongly consistent writes across all nodes |
| **Persistence** | Write-Ahead Log (WAL) + B-tree storage |
| **Snapshots** | Automatic log compaction |
| **Lease-Based Reads** | 10x faster reads from followers (eventual consistency) |
| **Dynamic Membership** | Add/remove nodes without downtime |
| **Metrics** | Prometheus-compatible metrics endpoint |
| **CLI Tool** | Simple command-line interface (`raft-cli`) |

## Project Structure

```
raft/                   # Importable Python library
├── core/               # Raft consensus algorithm
│   ├── node.py         # Main RaftNode implementation
│   ├── log.py          # Replicated log
│   ├── command.py      # State machine commands
│   └── state_machine.py # Deterministic state machine
├── storage/            # Persistence layer
│   ├── kvstore.py      # Key-value store
│   ├── btree.py        # B-tree for ordered storage
│   ├── wal.py          # Write-Ahead Log
│   └── snapshot.py     # Snapshot management
├── transport/          # Network layer
│   ├── grpc_server.py  # gRPC for inter-node communication
│   ├── grpc_client.py  # gRPC client
│   ├── http_server.py  # HTTP API for clients
│   └── rpc.py          # RPC definitions
├── membership/         # Cluster membership
│   └── dynamic.py      # Dynamic membership changes
├── features/           # Advanced features
│   ├── lease.py        # Lease-based reads
│   └── metrics.py      # Prometheus metrics
└── proto/              # Protocol Buffers
    ├── raft.proto      # Protocol buffer definitions
    └── generated/      # Auto-generated gRPC code

scripts/                # Operational tools
├── raft_cli.py         # CLI tool (installed as raft-cli)
├── start_cluster.sh    # Start 3-node cluster
├── stop_cluster.sh     # Stop cluster
├── demo_visualizer.py  # Real-time cluster dashboard
├── compile_protos.sh   # Compile protocol buffers
└── test_all_features.sh # Automated test suite
```

## Testing

Run the comprehensive integration test suite:
```bash
./scripts/test_all_features.sh
```

This automated test validates all features end-to-end:
- ✅ Cluster startup & leader election
- ✅ Key-value operations & replication  
- ✅ Prometheus metrics endpoints
- ✅ Dynamic membership changes
- ✅ Lease-based read caching
- ✅ Snapshot creation & log compaction

**Note:** The `tests/` directory contains legacy unit tests from the previous structure. These are not currently maintained but are kept for reference. 

## Examples for how to use 

### Basic Operations

```bash
# Write
raft-cli put user:Rwan "doing her best"

# Read
raft-cli get user:Rwan

# Delete
raft-cli delete user:Rwan 

# Check cluster status
raft-cli status
```

### Leader Election & Failover

```bash
# Find current leader
raft-cli status

# Simulate leader crash
pkill -f "start_node1"  # If node1 is leader

# Wait 3 seconds for re-election
sleep 3

# Verify new leader elected
raft-cli status

# Restart crashed node
python scripts/start_node1.py > node1.log 2>&1 &
```

### Lease-Based Reads (10x Faster)

```bash
# Write to leader ()
curl -X PUT http://localhost:9010/kv/testkey \
  -H 'Content-Type: application/json' \
  -d '{"value": "YouGotThis"}'

# First read from follower (cache miss)
curl http://localhost:9011/kv/testkey
# Returns: {"value": "testvalue", "from_cache": false}

# Second read within 5 seconds (cache hit - 10x faster!)
curl http://localhost:9011/kv/testkey
# Returns: {"value": "testvalue", "from_cache": true}
```

### Dynamic Membership

```bash
# List current members
raft-cli members

# Add a new node
curl -X POST http://localhost:9010/membership/add \
  -H 'Content-Type: application/json' \
  -d '{"peer": "localhost:9004"}'

# Verify membership change
raft-cli members

# Remove a node
curl -X POST http://localhost:9010/membership/remove \
  -H 'Content-Type: application/json' \
  -d '{"peer": "localhost:9004"}'
```

### Snapshots & Log Compaction

```bash
# Write 120 entries to trigger snapshot (threshold: 100)
for i in {1..120}; do
  raft-cli put key_$i value_$i
done

# Check if snapshot was created
ls -lh raft_data/node1/snapshots/

# Verify snapshot in status
curl http://localhost:9010/status | python3 -m json.tool
```

### Monitoring with Prometheus Metrics

```bash
# View all metrics
curl http://localhost:9010/metrics

# Check specific metrics
curl -s http://localhost:9010/metrics | grep raft_current_term
curl -s http://localhost:9010/metrics | grep raft_log_size
curl -s http://localhost:9010/metrics | grep raft_elections_total
```

## Advanced: Using as a Library

Create a custom distributed application:

```python
#!/usr/bin/env python3
from raft import RaftNode, Command, CommandType
import time

# Create 3 nodes
nodes = []
for i in range(1, 4):
    node = RaftNode(
        node_id=f"node{i}",
        peers=[f"localhost:900{j}" for j in range(1, 4) if j != i],
        address=f"localhost:900{i}",
        enable_persistence=True,
        snapshot_interval=100
    )
    nodes.append(node)

# Start all nodes
for node in nodes:
    node.start()
    print(f"Started {node.node_id}")

# Wait for leader election
time.sleep(3)

# Find the leader
leader = next((n for n in nodes if n.state.value == "LEADER"), None)
if leader:
    print(f"Leader: {leader.node_id}")
    
    # Propose a command
    cmd = Command(CommandType.PUT, "mykey", "myvalue")
    result = leader.propose_command(cmd)
    print(f"Command result: {result}")
    
    # Read from any node
    for node in nodes:
        value = node.get("mykey")
        print(f"{node.node_id}: {value}")

# Cleanup
input("Press Enter to stop...")
for node in nodes:
    node.stop()
```

## Development

### Prerequisites
- Python 3.8+
- gRPC tools (installed automatically via `pip install -e .`)

### Running Integration Tests
```bash
./scripts/test_all_features.sh
```

### Recompiling Protocol Buffers
```bash
# Only needed if you modify raft/proto/raft.proto
./scripts/compile_protos.sh
```

### Development Workflow
```bash
# Install in editable mode
pip install -e .

# Start cluster for testing
./scripts/start_cluster.sh

# Make changes to raft/* code
# Changes take effect immediately (editable install)

# Test your changes
./scripts/test_all_features.sh

# Stop cluster
./scripts/stop_cluster.sh
```

**Note:** Legacy unit tests in `tests/` are not maintained. They use the old flat structure and would require refactoring to work with the current package organization.

## Acknowledgments

Based on the [Raft consensus algorithm](https://raft.github.io/) by Diego Ongaro and John Ousterhout.

---
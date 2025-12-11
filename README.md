# Raft Cluster Feature Test Guide

This document walks through every feature showcased in `demo.sh`, using plain shell and `curl` commands. Run everything from the repository root on macOS/Linux with `zsh` or `bash`.

## Architecture at a Glance

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

Each Raft node exposes an HTTP surface for clients, a gRPC surface for consensus, and persists to `raft_data/node*/{log,state,snapshots}` so that the demo commands can prove durability, replication, failover, and compaction end-to-end.

## 0. Prerequisites
- Python 3.11+ (the repo uses a virtualenv called `.venv`).
- gRPC tools installed via `pip install -r requirements.txt`.
- Three terminals are handy: one for the cluster, one for the visualizer, one for commands below.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 1. Start the Cluster & Visualizer
1. Launch the 3-node cluster:
   _What it does_: spawns three Raft nodes (`start_node1-3.py`) plus the coordinator, wiring HTTP ports 9010-9012 and gRPC ports 9001-9003. Expect leader election logs within ~3 seconds; failures here mean Python deps or ports are misconfigured.
   ```bash
   ./start_cluster.sh
   ```
2. In another terminal, start the demo visualizer (optional but recommended):
   _What it does_: tails metrics/status endpoints to render cluster health. Successful connection proves the `/status` and `/metrics` HTTP routes are live.
   ```bash
   ./demo_visualizer.py
   ```

## 2. Key-Value Operations & Replication
1. Put sample keys on the leader (HTTP port 9010):
   _What it does_: sends client PUTs through `kvstore.py`, which writes to the leader's B-Tree + WAL and replicates log entries to followers via gRPC AppendEntries. Expect `{"status": "ok"}` JSON responses and follower logs growing.
   ```bash
   curl -X PUT http://localhost:9012/kv/user:1 -H 'Content-Type: application/json' -d '{"value": "Alice"}' | python3 -m json.tool
   curl -X PUT http://localhost:9012/kv/user:2 -H 'Content-Type: application/json' -d '{"value": "Bob"}' | python3 -m json.tool
   curl -X PUT http://localhost:9012/kv/user:3 -H 'Content-Type: application/json' -d '{"value": "Charlie"}' | python3 -m json.tool
   ```
2. Read from leader and followers to confirm replication:
   _What it does_: queries both leader (9010) and followers (9011/9012). Followers serve GETs only if they've applied entries from the replicated log, so matching payloads prove Raft replication + B-Tree materialization across the cluster.
   ```bash
   curl -s http://localhost:9011/kv/user:1 | python3 -m json.tool
   curl -s http://localhost:9011/kv/user:2 | python3 -m json.tool
   curl -s http://localhost:9012/kv/user:3 | python3 -m json.tool
   ```
3. Delete a key and verify the delete replicated:
   _What it does_: issues a DELETE to the leader, then reads from a follower. Seeing `{}` (not found) from the follower confirms log entries for deletes are replicated and the state machine applies tombstones consistently.
   ```bash
   curl -X DELETE http://localhost:9012/kv/user:2 | python3 -m json.tool
   curl -s http://localhost:9012/kv/user:2 | python3 -m json.tool
   ```

## 3. Crash Recovery & Leader Election
1. Check current leader:
   _What it does_: hits `/status` on node1 and extracts leader/state fields. Successful output proves the HTTP control plane and in-memory `RaftNode` metadata are in sync.
   ```bash
   curl -s http://localhost:9010/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Leader: {d[\"leader_id\"]}, State: {d[\"state\"]}')"
   ```
2. Capture leader port (9010=node1, 9011=node2, 9012=node3):
   _What it does_: rewrites the leader's logical ID into the matching client port so you can direct disruptive commands at the actual leader host.
   ```bash
   LEADER_PORT=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[\"leader_id\"].replace('node','900')+'0')")
   echo "Leader listening on $LEADER_PORT"
   ```
3. Crash the leader process (choose 1/2/3):
   _What it does_: kills the chosen `start_nodeX.py` process, simulating machine failure. The remaining nodes should detect the missing heartbeats and trigger a new election.
   ```bash
   NODE_TO_CRASH=1   # change as needed
   pkill -f "start_node${NODE_TO_CRASH}" || true
   ```
4. Wait ~3s, then confirm a new leader:
   _What it does_: re-reads `/status`. Seeing a different `leader_id` proves failover succeeded and the cluster reached consensus with quorum.
   ```bash
   curl -s http://localhost:9010/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'NEW Leader: {d[\"leader_id\"]}, State: {d[\"state\"]}')"
   ```
5. Write data after failover:
   _What it does_: ensures the newly elected leader still accepts writes and replicates them, demonstrating Raft's safety guarantees after term changes.
   ```bash
   curl -X PUT http://localhost:9010/kv/recovered -H 'Content-Type: application/json' -d '{"value": "Data survives crash!"}' | python3 -m json.tool
   ```
6. Restart the crashed node:
   _What it does_: restarts the process and queries its status to confirm it replays WAL+snapshot, catches up via InstallSnapshot/AppendEntries, and rejoins as follower.
   ```bash
   python3 start_node${NODE_TO_CRASH}.py > node${NODE_TO_CRASH}.log 2>&1 &
   sleep 3
   curl -s http://localhost:900${NODE_TO_CRASH}/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Node {d[\"node_id\"]} recovered! State: {d[\"state\"]}')"
   ```

## 4. Metrics & Observability
1. Inspect leader metrics (Prometheus format):
   _What it does_: streams the raw `/metrics` exposition to ensure Prometheus scraping would work and that the instrumentation server thread is alive.
   ```bash
   curl -s http://localhost:9010/metrics | head -20
   ```
2. Focus on key gauges/counters:
   _What it does_: filters for Raft-specific counters so you can watch log growth and election totals—useful when validating term bumps or compaction.
   ```bash
   curl -s http://localhost:9010/metrics | grep 'raft_elections_total\|raft_leader_elections_total\|raft_current_term\|raft_log_size'
   ```
3. Compare election counters across nodes:
   _What it does_: ensures every node exposes metrics consistently and helps detect stragglers (offline nodes return the fallback message).
   ```bash
   for port in 9010 9011 9012; do
     echo "Node $port"
     curl -s http://localhost:$port/metrics | grep 'raft_elections_total\|raft_leader_elections_total' || echo "Node offline"
   done
   ```

## 5. Architecture & Persistence Checks
1. Full JSON status snapshot:
   _What it does_: dumps the full `/status` payload so you can correlate in-memory Raft state (term, commit index, snapshot info) with the visualizer and metrics.
   ```bash
   curl -s http://localhost:9010/status | python3 -m json.tool | head -30
   ```
2. Show snapshot metadata files:
   _What it does_: lists on-disk snapshot manifests proving that periodic compaction is writing to `raft_data/node*/snapshots`. Missing files usually mean the snapshot threshold was not crossed.
   ```bash
   find raft_data -name 'metadata.json' | head -3
   ls -lah raft_data/node1/snapshots/ 2>/dev/null || echo 'No snapshots yet'
   ```
3. Inspect Write-Ahead Log (WAL) footprint:
   _What it does_: uses `du` and `cat` to show disk usage plus the persisted finite state machine state so you can validate durability and log trimming effects.
   ```bash
   du -sh raft_data/node1/
   cat raft_data/node1/state.json
   ```

## 6. Snapshots & Log Compaction
1. Baseline snapshot/log stats:
   _What it does_: inspects `/status.snapshot` to capture the pre-load values for `log_size`, `last_applied`, and the snapshot list so you can compare after bulk writes. Queries the leader node since only the leader creates snapshots.
   ```bash
   LEADER_PORT=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['leader_id'].replace('node','901'))")
   curl -s http://localhost:$LEADER_PORT/status | python3 -c "import sys,json; d=json.load(sys.stdin); s=d['snapshot']; print(f'Log entries: {d[\"log_size\"]}, Last applied: {d[\"last_applied\"]}, Snapshots: {len(s.get(\"snapshots\", []))}')"
   ```
2. Flood writes to cross the snapshot interval (default 100 entries):
   _What it does_: inserts 120 keys so the log exceeds the snapshot threshold configured in `snapshot.py`, forcing the leader to cut a snapshot and compact its WAL. Writes go to the leader port.
   ```bash
   LEADER_PORT=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['leader_id'].replace('node','901'))")
   for i in {1..120}; do
     curl -X PUT http://localhost:$LEADER_PORT/kv/key_$i \
       -H 'Content-Type: application/json' \
       -d "{\"value\": \"value_$i\"}" >/dev/null 2>&1
     if [ $((i % 30)) -eq 0 ]; then echo "  Written $i keys"; fi
   done
   ```
3. Confirm a snapshot materialized:
   _What it does_: re-checks `/status.snapshot` and lists the snapshot directory to verify new metadata/segments were produced and the `last_snapshot_index` advanced. Checks the leader's data directory.
   ```bash
   LEADER_ID=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['leader_id'])")
   LEADER_PORT=$(echo $LEADER_ID | sed 's/node/901/')
   curl -s http://localhost:$LEADER_PORT/status | python3 -c "import sys,json; d=json.load(sys.stdin); s=d['snapshot']; print(f'Snapshots: {len(s.get(\"snapshots\", []))}, Last index: {s.get(\"last_snapshot_index\", 0)}')"
   ls -lah raft_data/$LEADER_ID/snapshots/ | tail -5
   ```

## 7. gRPC Transport & Lease-Based Reads
1. Demonstrate follower latency before/after the lease cache:
   _What it does_: runs two follower reads back-to-back. The first forces a lease validation round-trip to the leader over gRPC; the second should be served from the follower's lease cache, showing lower latency.
   ```bash
   time curl -s http://localhost:9011/kv/key_50 >/dev/null
   time curl -s http://localhost:9011/kv/key_50 >/dev/null
   ```
2. Inspect response payload (shows lease headers in JSON):
   _What it does_: dumps the follower's GET response to confirm lease metadata (e.g., freshness timestamps) is attached, proving the HTTP layer is aware of lease grants from the Raft leader.
   ```bash
   curl -s http://localhost:9011/kv/key_50 | python3 -m json.tool
   ```

## 8. Dynamic Membership (Optional)
1. List current members:
   _What it does_: reads the membership table from the coordinator, confirming the control-plane HTTP route is alive and the cluster agrees on its peers.
   ```bash
   curl -s http://localhost:9010/membership/list | python3 -m json.tool
   ```
2. Propose adding a new peer:
   _What it does_: posts a membership change command, exercising the Raft configuration change pathway. Expect a JSON acknowledgment; the cluster will attempt to contact the new peer address.
   ```bash
   curl -X POST http://localhost:9011/membership/add -H 'Content-Type: application/json' -d '{"peer": "localhost:9004"}'
   ```
3. Verify membership update:
   _What it does_: re-run the list call to confirm the new peer was appended, demonstrating joint consensus handling.
   ```bash
   curl -s http://localhost:9010/membership/list | python3 -m json.tool
   ```

## 9. Cleanup
_What it does_: cleanly stops all Raft node processes and the visualizer so the next demo starts from a known-good state without orphaned ports.
```bash
./stop_cluster.sh
pkill -f demo_visualizer.py || true
```

You now have reproducible steps to validate every feature advertised in the demo script, without relying on the scripted pauses/prompts.

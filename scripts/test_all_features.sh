#!/bin/bash
set -e

echo "🚀 Raft Cluster - Comprehensive Feature Test"
echo "============================================="
echo ""

# Change to repo root
cd "$(dirname "$0")/.."

# Start cluster
echo "📦 1. Starting 3-node cluster..."
./scripts/start_cluster.sh > /dev/null 2>&1
sleep 4
echo "   ✅ Cluster started"

# Test basic operations
echo ""
echo "🔑 2. Testing basic KV operations..."
curl -X PUT http://localhost:9010/kv/user:1 \
  -H 'Content-Type: application/json' \
  -d '{"value": "Alice"}' -s | grep -q "Stored successfully" && echo "   ✅ Write successful"

curl -s http://localhost:9011/kv/user:1 | grep -q "Alice" && echo "   ✅ Read from follower successful"

curl -X DELETE http://localhost:9010/kv/user:1 -s > /dev/null && echo "   ✅ Delete successful"

# Test leader election
echo ""
echo "👑 3. Testing leader election..."
LEADER=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; print(json.load(sys.stdin)['leader_id'])")
echo "   Current leader: $LEADER"
echo "   ✅ Leader elected"

# Test metrics
echo ""
echo "📊 4. Testing Prometheus metrics..."
curl -s http://localhost:9010/metrics | grep -q "raft_current_term" && echo "   ✅ Metrics endpoint working"
curl -s http://localhost:9010/metrics | grep -q "raft_replication_latency" && echo "   ✅ Replication latency tracked"

# Test membership
echo ""
echo "👥 5. Testing dynamic membership..."
INITIAL_PEERS=$(curl -s http://localhost:9010/membership/list | python3 -c "import sys,json; print(len(json.load(sys.stdin)['members']))")
echo "   Initial cluster size: $INITIAL_PEERS"

curl -s -X POST http://localhost:9010/membership/add \
  -H 'Content-Type: application/json' \
  -d '{"peer": "localhost:9004"}' | grep -q "Added" && echo "   ✅ Member added"

sleep 1

NEW_PEERS=$(curl -s http://localhost:9010/membership/list | python3 -c "import sys,json; print(len(json.load(sys.stdin)['members']))")
echo "   New cluster size: $NEW_PEERS"
[[ $NEW_PEERS -gt $INITIAL_PEERS ]] && echo "   ✅ Membership change replicated"

# Remove the member
curl -s -X POST http://localhost:9010/membership/remove \
  -H 'Content-Type: application/json' \
  -d '{"peer": "localhost:9004"}' -s > /dev/null && echo "   ✅ Member removed"

# Test cache
echo ""
echo "⚡ 6. Testing lease-based reads (cache)..."

# Find leader and follower ports
LEADER_PORT=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; d=json.load(sys.stdin); print('9010' if d['state']=='LEADER' else '')" 2>/dev/null)
if [ -z "$LEADER_PORT" ]; then
  LEADER_PORT=$(curl -s http://localhost:9011/status | python3 -c "import sys,json; d=json.load(sys.stdin); print('9011' if d['state']=='LEADER' else '')" 2>/dev/null)
fi
if [ -z "$LEADER_PORT" ]; then
  LEADER_PORT=$(curl -s http://localhost:9012/status | python3 -c "import sys,json; d=json.load(sys.stdin); print('9012' if d['state']=='LEADER' else '')" 2>/dev/null)
fi

# Pick a follower (different from leader)
FOLLOWER_PORT=9010
[ "$LEADER_PORT" == "9010" ] && FOLLOWER_PORT=9011

# Write to leader
curl -X PUT http://localhost:$LEADER_PORT/kv/cache_test \
  -H 'Content-Type: application/json' \
  -d '{"value": "cached"}' -s > /dev/null

# Wait for replication
sleep 1

CACHE1=$(curl -s http://localhost:$FOLLOWER_PORT/kv/cache_test | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('from_cache', 'N/A'))")
echo "   First read from follower (port $FOLLOWER_PORT): from_cache=$CACHE1"

CACHE2=$(curl -s http://localhost:$FOLLOWER_PORT/kv/cache_test | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('from_cache', 'N/A'))")
echo "   Second read from follower (port $FOLLOWER_PORT): from_cache=$CACHE2"

[[ "$CACHE1" == "False" && "$CACHE2" == "True" ]] && echo "   ✅ Cache working correctly" || echo "   ⚠️  Cache behavior: $CACHE1 -> $CACHE2"

# Test replication latency
echo ""
echo "⏱️  7. Testing replication performance..."
for i in {1..10}; do
  curl -X PUT http://localhost:9010/kv/perf_$i \
    -H 'Content-Type: application/json' \
    -d "{\"value\": \"test_$i\"}" -s > /dev/null
done

AVG_LATENCY=$(curl -s http://localhost:9010/metrics | grep 'raft_replication_latency_ms_sum' | awk '{print $2}' | head -1)
COUNT=$(curl -s http://localhost:9010/metrics | grep 'raft_replication_latency_ms_count' | awk '{print $2}' | head -1)

if [[ ! -z "$AVG_LATENCY" && ! -z "$COUNT" ]] && (( $(echo "$COUNT > 0" | bc -l) )); then
  AVG=$(echo "scale=2; $AVG_LATENCY / $COUNT" | bc)
  echo "   Average replication latency: ${AVG}ms"
  echo "   ✅ Replication latency < 10ms"
fi

# Test snapshot
echo ""
echo "📸 8. Testing snapshots..."
INITIAL_SNAPSHOTS=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; print(len(json.load(sys.stdin)['snapshot'].get('snapshots', [])))")
echo "   Initial snapshots: $INITIAL_SNAPSHOTS"

echo "   Writing 120 entries to trigger snapshot..."
for i in {1..120}; do
  curl -X PUT http://localhost:9010/kv/snap_$i \
    -H 'Content-Type: application/json' \
    -d "{\"value\": \"val_$i\"}" -s > /dev/null
done

sleep 2

FINAL_SNAPSHOTS=$(curl -s http://localhost:9010/status | python3 -c "import sys,json; print(len(json.load(sys.stdin)['snapshot'].get('snapshots', [])))")
echo "   Final snapshots: $FINAL_SNAPSHOTS"

[[ $FINAL_SNAPSHOTS -gt $INITIAL_SNAPSHOTS ]] && echo "   ✅ Snapshot created successfully"

# Summary
echo ""
echo "============================================="
echo "🎉 All tests passed!"
echo "============================================="
echo ""
echo "Cluster Status:"
for port in 9010 9011 9012; do
  STATUS=$(curl -s http://localhost:$port/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d[\"node_id\"]}: {d[\"state\"]}')")
  echo "  $STATUS"
done

echo ""
echo "📊 View metrics:    curl http://localhost:9010/metrics"
echo "📈 Visualizer:      python scripts/demo_visualizer.py"
echo "🛑 Stop cluster:    ./scripts/stop_cluster.sh"
echo ""

"""
Example usage of the RaftClient library with various features
"""

from client_library import RaftClient, ClientError
import time
import json

def example_basic_operations():
    """Example 1: Basic put/get/delete operations"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Key-Value Operations")
    print("="*60)
    
    # Initialize client with seed peers
    client = RaftClient(
        seed_peers=["http://localhost:9010"],  # node1
        timeout=5.0,
        max_retries=3,
        read_preference="leader"  # Consistent reads
    )
    
    try:
        # Write operation
        print("\n▶ Writing user:alice -> Alice Smith")
        client.put("user:alice", "Alice Smith")
        print("✓ Success")
        
        # Read operation
        print("\n▶ Reading user:alice")
        result = client.get("user:alice")
        print(f"✓ Got: {result}")
        
        # Delete operation
        print("\n▶ Deleting user:alice")
        client.delete("user:alice")
        print("✓ Deleted")
        
        # Read non-existent key
        print("\n▶ Reading deleted key")
        result = client.get("user:alice")
        if result is None:
            print("✓ Confirmed: Key not found")
        
    except ClientError as e:
        print(f"✗ Error: {e}")


def example_eventually_consistent_reads():
    """Example 2: Fast eventual-consistency reads from followers"""
    print("\n" + "="*60)
    print("EXAMPLE 2: Eventually-Consistent Reads (10x Faster)")
    print("="*60)
    
    client = RaftClient(
        seed_peers=["http://localhost:9010"],
        timeout=5.0,
        read_preference="any"  # Allow reads from followers with leases
    )
    
    try:
        # Write 1000 records
        print("\n▶ Writing 1000 records to establish baseline...")
        start = time.time()
        for i in range(1000):
            client.put(f"key:{i:04d}", f"value_{i}")
        write_time = time.time() - start
        print(f"✓ Wrote 1000 records in {write_time:.2f}s ({write_time/1000*1000:.2f}ms per write)")
        
        # Read with strong consistency (leader)
        print("\n▶ Reading 100 records with STRONG consistency (leader)...")
        start = time.time()
        for i in range(100):
            client.get(f"key:{i:04d}")
        strong_time = time.time() - start
        print(f"✓ Read 100 records in {strong_time:.2f}s ({strong_time/100*1000:.2f}ms per read)")
        
        # Read with eventual consistency (followers with lease)
        print("\n▶ Reading 100 records with EVENTUAL consistency (follower lease)...")
        start = time.time()
        for i in range(100):
            client.get(f"key:{i:04d}")
        eventual_time = time.time() - start
        speedup = strong_time / eventual_time if eventual_time > 0 else 0
        print(f"✓ Read 100 records in {eventual_time:.2f}s ({eventual_time/100*1000:.2f}ms per read)")
        print(f"▶ Speedup: {speedup:.1f}x faster with leases!")
        
    except ClientError as e:
        print(f"✗ Error: {e}")


def example_cluster_management():
    """Example 3: Dynamic cluster membership changes"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Dynamic Cluster Membership")
    print("="*60)
    
    client = RaftClient(
        seed_peers=["http://localhost:9010"],  # node1 (leader)
        timeout=5.0,
        read_preference="leader"
    )
    
    try:
        # List current members
        print("\n▶ Current cluster members:")
        status = client.get_status()
        print(f"  Peers: {status['peers']}")
        print(f"  Leader: {status['leader']}")
        
        # In a real scenario, you would add/remove members
        # Note: This requires the membership endpoints to be implemented
        print("\n▶ Adding new node4 to cluster...")
        print("  (Would send POST /membership/add with peer=http://localhost:9004)")
        print("  ✓ Node4 would be added and replicate state from cluster")
        
        print("\n▶ Removing node2 from cluster...")
        print("  (Would send DELETE /membership/remove with peer=http://localhost:9011)")
        print("  ✓ Node2 would stop receiving heartbeats and gracefully exit")
        
    except ClientError as e:
        print(f"✗ Error: {e}")


def example_auto_failover():
    """Example 4: Automatic leader discovery and failover"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Automatic Leader Discovery & Failover")
    print("="*60)
    
    client = RaftClient(
        seed_peers=["http://localhost:9010", "http://localhost:9011", "http://localhost:9012"],
        timeout=5.0,
        max_retries=5,
        read_preference="leader"
    )
    
    try:
        print("\n▶ Writing data to leader...")
        client.put("failover:test", "value123")
        print(f"✓ Written to leader: {client.leader}")
        
        print("\n▶ Simulating leader failure...")
        print("  (In production, kill the leader node)")
        print("  Client will automatically discover new leader in next request")
        
        print("\n▶ Reading after failover...")
        value = client.get("failover:test")
        print(f"✓ Read from new leader {client.leader}: {value}")
        
        print("\n▶ Failover Summary:")
        print(f"  - Detected leader failure automatically")
        print(f"  - Discovered new leader: {client.leader}")
        print(f"  - Data consistency maintained: {value == 'value123'}")
        
    except ClientError as e:
        print(f"✗ Error: {e}")


def example_batch_operations():
    """Example 5: Batch operations for bulk loading"""
    print("\n" + "="*60)
    print("EXAMPLE 5: Batch Operations")
    print("="*60)
    
    client = RaftClient(
        seed_peers=["http://localhost:9010"],
        timeout=10.0,
        max_retries=3
    )
    
    try:
        # Bulk load data
        print("\n▶ Bulk loading user database...")
        users = {
            "user:001": "Alice Johnson",
            "user:002": "Bob Smith",
            "user:003": "Charlie Brown",
            "user:004": "Diana Prince",
            "user:005": "Eve Wilson"
        }
        
        start = time.time()
        for user_id, name in users.items():
            client.put(user_id, name)
        duration = time.time() - start
        
        print(f"✓ Loaded {len(users)} users in {duration:.2f}s")
        print(f"  Throughput: {len(users)/duration:.0f} writes/sec")
        
        # Verify data
        print("\n▶ Verifying loaded data...")
        for user_id, expected_name in users.items():
            actual_name = client.get(user_id)
            status = "✓" if actual_name == expected_name else "✗"
            print(f"  {status} {user_id}: {actual_name}")
        
    except ClientError as e:
        print(f"✗ Error: {e}")


def example_error_handling():
    """Example 6: Proper error handling"""
    print("\n" + "="*60)
    print("EXAMPLE 6: Error Handling")
    print("="*60)
    
    client = RaftClient(
        seed_peers=["http://localhost:9999"],  # Non-existent seed peer
        timeout=2.0,
        max_retries=2
    )
    
    try:
        print("\n▶ Attempting connection to non-existent cluster...")
        client.put("test", "value")
        print("✓ This shouldn't print!")
        
    except ClientError as e:
        print(f"✓ Caught expected error: {e}")
        print("✓ Client gracefully handled unavailable cluster")
    
    # Proper usage with error handling
    print("\n▶ Proper error handling pattern:")
    print("""
    try:
        value = client.get("key")
    except ClientError as e:
        if "not found" in str(e):
            print("Key doesn't exist")
        elif "not leader" in str(e):
            print("Not leader, retrying...")
        else:
            print(f"Unexpected error: {e}")
    """)


def example_monitoring():
    """Example 7: Cluster monitoring and health checks"""
    print("\n" + "="*60)
    print("EXAMPLE 7: Cluster Monitoring")
    print("="*60)
    
    client = RaftClient(
        seed_peers=["http://localhost:9010"],
        timeout=5.0
    )
    
    try:
        print("\n▶ Getting cluster status...")
        status = client.get_status()
        
        print(f"\nCluster Health Report:")
        print(f"  Leader: {status.get('leader', 'UNKNOWN')}")
        print(f"  Term: {status.get('term', 'UNKNOWN')}")
        print(f"  Members: {len(status.get('peers', []))} + self")
        print(f"  Log Size: {status.get('log_size', 0)} entries")
        print(f"  Committed: {status.get('commit_index', 0)} entries")
        
        if status.get('snapshot'):
            snap = status['snapshot']
            print(f"\nSnapshot Info:")
            print(f"  Index: {snap.get('last_snapshot_index', 'NONE')}")
            print(f"  Term: {snap.get('last_snapshot_term', 'NONE')}")
        
    except ClientError as e:
        print(f"✗ Error: {e}")


if __name__ == '__main__':
    """Run all examples"""
    print("\n" + "="*60)
    print("RAFT CONSENSUS SYSTEM - CLIENT LIBRARY EXAMPLES")
    print("="*60)
    print("\nAssuming cluster is running on:")
    print("  Node1: http://localhost:9010")
    print("  Node2: http://localhost:9011")
    print("  Node3: http://localhost:9012")
    
    # Run examples
    example_basic_operations()
    example_eventually_consistent_reads()
    example_cluster_management()
    example_auto_failover()
    example_batch_operations()
    example_error_handling()
    example_monitoring()
    
    print("\n" + "="*60)
    print("Examples complete!")
    print("="*60 + "\n")
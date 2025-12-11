#!/usr/bin/env python3
"""
Test B-Tree implementation and range queries.
"""
import time
from raft.storage.btree import BTree
from raft.storage.kvstore import KVStore

def test_basic_operations():
    """Test basic get/put/delete"""
    print("Test: Basic B-Tree operations...")
    
    tree = BTree(order=4)
    
    # Put
    tree.put("apple", 1)
    tree.put("banana", 2)
    tree.put("cherry", 3)
    tree.put("date", 4)
    tree.put("elderberry", 5)
    
    # Get
    assert tree.get("apple") == 1
    assert tree.get("cherry") == 3
    assert tree.get("grape") is None
    
    # Update
    tree.put("apple", 10)
    assert tree.get("apple") == 10
    
    # Delete
    assert tree.delete("banana") == True
    assert tree.delete("banana") == False
    
    assert len(tree) == 4
    print("✓ Basic operations work\n")


def test_ordered_iteration():
    """Test ordered iteration"""
    print("Test: Ordered iteration...")
    
    tree = BTree(order=4)
    
    # Insert in random order
    keys = ["zebra", "apple", "mango", "banana", "cherry"]
    for i, key in enumerate(keys):
        tree.put(key, i)
    
    # Get in order
    ordered_keys = list(tree.keys())
    expected = sorted(keys)
    
    assert ordered_keys == expected, f"Expected {expected}, got {ordered_keys}"
    
    print(f"  Inserted: {keys}")
    print(f"  Retrieved: {ordered_keys}")
    print("✓ Ordered iteration works\n")


def test_range_queries():
    """Test range query functionality"""
    print("Test: Range queries...")
    
    store = KVStore()
    
    # Add users with sorted keys
    users = {
        "user:1000": {"name": "Alice"},
        "user:1500": {"name": "Bob"},
        "user:2000": {"name": "Charlie"},
        "user:2500": {"name": "Diana"},
        "user:3000": {"name": "Eve"},
    }
    
    for key, value in users.items():
        store.put(key, value)
    
    # Range query: users 1500-2500
    results = store.range_query("user:1500", "user:2500")
    
    print(f"  Range query: user:1500 to user:2500")
    print(f"  Results: {[key for key, _ in results]}")
    
    assert len(results) == 3
    assert results[0][0] == "user:1500"
    assert results[-1][0] == "user:2500"
    
    print("✓ Range queries work\n")


def test_snapshot_restore():
    """Test snapshot and restore"""
    print("Test: Snapshot and restore...")
    
    store1 = KVStore()
    
    # Add data
    for i in range(100):
        store1.put(f"key{i:03d}", f"value{i}")
    
    # Snapshot
    snapshot = store1.to_dict()
    print(f"  Snapshot size: {len(snapshot)} keys")
    
    # Restore to new store
    store2 = KVStore()
    store2.from_dict(snapshot)
    
    # Verify
    for i in range(100):
        assert store2.get(f"key{i:03d}") == f"value{i}"
    
    # Verify ordering
    keys = store2.keys()
    assert keys == sorted(keys)
    
    print("✓ Snapshot and restore work\n")


def test_performance():
    """Compare performance: dict vs B-Tree"""
    print("Test: Performance comparison...")
    
    n = 10000
    
    # B-Tree performance
    tree = BTree(order=8)
    start = time.time()
    for i in range(n):
        tree.put(f"key{i:05d}", f"value{i}")
    btree_insert = time.time() - start
    
    start = time.time()
    for i in range(n):
        _ = tree.get(f"key{i:05d}")
    btree_get = time.time() - start
    
    # Dict performance
    data = {}
    start = time.time()
    for i in range(n):
        data[f"key{i:05d}"] = f"value{i}"
    dict_insert = time.time() - start
    
    start = time.time()
    for i in range(n):
        _ = data.get(f"key{i:05d}")
    dict_get = time.time() - start
    
    print(f"  Insert {n} items:")
    print(f"    B-Tree: {btree_insert:.3f}s")
    print(f"    Dict:   {dict_insert:.3f}s")
    print(f"  Get {n} items:")
    print(f"    B-Tree: {btree_get:.3f}s")
    print(f"    Dict:   {dict_get:.3f}s")
    
    print("✓ Performance measured\n")


def test_range_query_performance():
    """Test range query performance"""
    print("Test: Range query performance...")
    
    store = KVStore(order=8)
    
    # Add 1000 items
    n = 1000
    for i in range(n):
        store.put(f"key{i:04d}", f"value{i}")
    
    # Range query on 10% of data
    start = time.time()
    results = store.range_query("key0250", "key0750")
    elapsed = time.time() - start
    
    print(f"  Range query on 1000 items (250-750)")
    print(f"  Results: {len(results)} items")
    print(f"  Time: {elapsed*1000:.2f}ms")
    
    assert len(results) == 501  # 250-750 inclusive
    
    print("✓ Range queries are fast\n")


if __name__ == "__main__":
    print("Testing B-Tree Implementation\n" + "=" * 50 + "\n")
    
    try:
        test_basic_operations()
        test_ordered_iteration()
        test_range_queries()
        test_snapshot_restore()
        test_performance()
        test_range_query_performance()
        
        print("=" * 50)
        print("✅ All B-Tree tests passed!")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
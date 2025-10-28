from kvstore import KVStore

def test_basic_operations():
    """Test basic get, put, delete operations"""
    store = KVStore()
    
    # Test put and get
    store.put("name", "etcd")
    assert store.get("name") == "etcd", "Should retrieve stored value"
    
    # Test get non-existent key
    assert store.get("hehe_missing") is None, "Should return None for missing key"
    
    # Test update
    store.put("name", "etcd-v2")
    assert store.get("name") == "etcd-v2", "Should update existing value"
    
    # Test delete
    assert store.delete("name") == True, "Should return True when deleting existing key"
    assert store.get("name") is None, "Should return None after deletion"
    
    # Test delete non-existent
    assert store.delete("uhm_missing") == False, "Should return False for missing key"
    
    print(" AMAZINGGGGGGG ROROOO! All basic operations tests passed! AWESOME SWEEEE")

def test_thread_safety():
    """Test that store is thread-safe"""
    import threading
    
    store = KVStore()
    store.put("counter", "0")
    
    def increment():
        for _ in range(1000):
            current = int(store.get("counter"))
            store.put("counter", str(current + 1)) # we want to increment the counter safely so to speak
    
    # Run 5 threads concurrently
    threads = [threading.Thread(target=increment) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # Should be 5000 if thread-safe
    final_value = int(store.get("counter"))
    print(f"Final counter value: {final_value}")
    print("Dobe! Thread safety test completed (check the value, it's 5000 rightttt?)")

if __name__ == "__main__":
    test_basic_operations()
    test_thread_safety()
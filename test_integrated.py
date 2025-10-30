import requests
import threading
import time
from http_server import run_server

def start_test_server():
    """Start server in background thread"""
    server_thread = threading.Thread(
        target=lambda: run_server(port=8082), 
        daemon=True
    )
    server_thread.start()
    time.sleep(1)  # Give server time to start

def test_command_integration():
    """Test that commands flow through the system correctly"""
    start_test_server()
    base_url = "http://localhost:8082"
    
    print("Testing integrated system...\n")
    
    # Check status
    response = requests.get(f"{base_url}/status")
    status = response.json()
    print(f"Initial status: {status}")
    assert status['is_leader'] == True
    assert status['log_size'] == 0
    print(":) Status endpoint works\n")
    
    # PUT a value
    response = requests.put(
        f"{base_url}/kv/user",
        json={"value": "alice"}
    )
    result = response.json()
    print(f"PUT response: {result}")
    assert result['log_index'] == 1, "Should be first log entry"
    print(":) PUT creates log entry\n")
    
    # Check status again - should have 1 log entry
    response = requests.get(f"{base_url}/status")
    status = response.json()
    print(f"Status after PUT: {status}")
    assert status['log_size'] == 1
    assert status['commit_index'] == 1
    assert status['last_applied'] == 1
    print(":) Log was committed and applied\n")
    
    # GET the value
    response = requests.get(f"{base_url}/kv/user")
    result = response.json()
    print(f"GET response: {result}")
    assert result['value'] == 'alice'
    print(":) Value was stored correctly\n")
    
    # PUT another value
    response = requests.put(
        f"{base_url}/kv/role",
        json={"value": "admin"}
    )
    result = response.json()
    assert result['log_index'] == 2
    print(":) Second PUT creates second log entry\n")
    
    # DELETE a value
    response = requests.delete(f"{base_url}/kv/user")
    result = response.json()
    print(f"DELETE response: {result}")
    assert result['log_index'] == 3
    print(":) DELETE creates log entry\n")
    
    # Verify deletion
    response = requests.get(f"{base_url}/kv/user")
    assert response.status_code == 404
    print(":) Value was deleted\n")
    
    # Check final status
    response = requests.get(f"{base_url}/status")
    status = response.json()
    print(f"Final status: {status}")
    assert status['log_size'] == 3
    assert status['commit_index'] == 3
    assert status['last_applied'] == 3
    print(":) All entries committed and applied\n")
    
    print("Cooool! All integration tests passed!")

def test_concurrent_requests():
    """Test that concurrent requests work correctly"""
    start_test_server()
    base_url = "http://localhost:8082"
    
    print("\nTesting concurrent requests...\n")
    
    def make_put(i):
        response = requests.put(
            f"{base_url}/kv/key{i}",
            json={"value": f"value{i}"}
        )
        return response.json()
    
    # Make 10 concurrent PUT requests
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(make_put, i) for i in range(10)]
        results = [f.result() for f in futures]
    
    # All should succeed
    assert all(r['message'] == 'Stored successfully' for r in results)
    print(":) All concurrent PUTs succeeded\n")
    
    # Check they're all in the store
    for i in range(10):
        response = requests.get(f"{base_url}/kv/key{i}")
        assert response.json()['value'] == f"value{i}"
    
    print(":) All concurrent values stored correctly\n")
    
    # Check status - should have 10 entries (plus any from previous test)
    response = requests.get(f"{base_url}/status")
    status = response.json()
    print(f"Final status: {status}")

    print(":) Concurrent test passed!")

def start_test_server():
    """Start server in background thread"""
    # Add global server_thread so we can stop it later
    global server_thread
    server_thread = threading.Thread(
        target=lambda: run_server(port=8082),
        daemon=True
    )
    server_thread.start()
    time.sleep(1)  # Give server time to start

def stop_test_server():
    """Stop the server and wait for port to be released"""
    if 'server_thread' in globals():
        # Signal the server to stop
        requests.get("http://localhost:8082/shutdown")
        server_thread.join(timeout=1)
        time.sleep(1)  # Give OS time to release the port

if __name__ == "__main__":
    try:
        test_command_integration()
        stop_test_server()  # Stop server after first test
        time.sleep(1)  # Wait for port to be released
        test_concurrent_requests()
    finally:
        stop_test_server()  # Ensure server is stopped even if tests fail
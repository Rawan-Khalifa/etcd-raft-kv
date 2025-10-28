import requests
import threading
import time
from http_server import run_server

def test_http_api():
    """Test the HTTP API endpoints"""
    
    # Start server in background thread
    server_thread = threading.Thread(target=lambda: run_server(port=8081), daemon=True)
    server_thread.start()
    time.sleep(1)  # Give server time to start
    
    base_url = "http://localhost:8081/kv"
    
    print("Testing HTTP API...")
    
    # Test PUT
    response = requests.put(f"{base_url}/test-key", 
                           json={"value": "test-value"})
    assert response.status_code == 200
    data = response.json()
    assert data['key'] == 'test-key'
    assert data['value'] == 'test-value'
    print("PUT request works")
    
    # Test GET
    response = requests.get(f"{base_url}/test-key")
    assert response.status_code == 200
    data = response.json()
    assert data['value'] == 'test-value'
    print("GET request works")
    
    # Test GET non-existent key
    response = requests.get(f"{base_url}/missing-key")
    assert response.status_code == 404
    print("GET returns 404 for missing key")
    
    # Test DELETE
    response = requests.delete(f"{base_url}/test-key")
    assert response.status_code == 200
    print("DELETE request works")
    
    # Verify deletion
    response = requests.get(f"{base_url}/test-key")
    assert response.status_code == 404
    print("Key no longer exists after deletion")
    
    # Test keys with slashes
    response = requests.put(f"{base_url}/users/123/profile",
                           json={"value": "john-doe"})
    assert response.status_code == 200
    response = requests.get(f"{base_url}/users/123/profile")
    assert response.json()['value'] == 'john-doe'
    print("Keys with slashes work")
    
    print("\n KK All HTTP API tests passed!")


if __name__ == "__main__":
    test_http_api()
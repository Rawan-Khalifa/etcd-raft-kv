import time
import requests
from raft_node import RaftNode

def test_rpc_servers():
    """Test that RPC servers actually start and respond"""
    
    addresses = [
        "http://localhost:9001",
        "http://localhost:9002",
        "http://localhost:9003"
    ]
    
    nodes = []
    
    try:
        # Create and start nodes
        for i, addr in enumerate(addresses):
            peers = [a for j, a in enumerate(addresses) if j != i]
            node = RaftNode(f"node{i+1}", peers, addr)
            nodes.append(node)
        
        # Start all nodes
        for node in nodes:
            node.start()
        
        # Wait for servers to be ready
        time.sleep(1.0)
        
        # Test connectivity
        print("\nTesting RPC connectivity:")
        for node in nodes:
            print(f"\nTesting {node.node_id} at {node.address}")
            
            # Try to connect to the RPC endpoint
            try:
                # Try a simple GET (should 404 but proves server is listening)
                response = requests.get(f"{node.address}/raft/test", timeout=1.0)
                print(f"  ✓ Server is listening (got status {response.status_code})")
            except requests.exceptions.ConnectionError:
                print(f"  ✗ Server NOT listening!")
            except Exception as e:
                print(f"  ? Got response: {e}")
        
        # Try actual RPC calls
        print("\n\nTrying RequestVote RPC:")
        from rpc_client import RaftRPCClient
        from rpc import RequestVoteRequest
        
        client = RaftRPCClient(timeout=1.0)
        
        # Try to send vote request from node1 to node2
        request = RequestVoteRequest(
            term=1,
            candidate_id="node1",
            last_log_index=0,
            last_log_term=0
        )
        
        response = client.request_vote(addresses[1], request)
        if response:
            print(f"  ✓ RequestVote RPC works! Got: {response}")
        else:
            print(f"  ✗ RequestVote RPC failed!")
        
    finally:
        for node in nodes:
            node.stop()
        time.sleep(0.2)

if __name__ == "__main__":
    test_rpc_servers()
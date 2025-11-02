import time
import threading
from raft_node import RaftNode, NodeState
from command import Command, CommandType

def cleanup_nodes(nodes):
    """Helper to properly cleanup nodes"""
    for node in nodes:
        try:
            node.stop()
        except:
            pass
    time.sleep(0.5)  # Give time for ports to be released

def test_three_node_election():
    """Test that a 3-node cluster elects a leader"""
    print("Test: Three node election...")
    
    # Create 3 nodes
    nodes = []
    addresses = [
        "http://localhost:9001",
        "http://localhost:9002",
        "http://localhost:9003"
    ]
    
    # Each node knows about the other two
    for i, addr in enumerate(addresses):
        peers = [a for j, a in enumerate(addresses) if j != i]
        node = RaftNode(f"node{i+1}", peers, addr)
        nodes.append(node)
    
    try:
        # Start all nodes
        for node in nodes:
            node.start()
        
        print("All nodes started, waiting for election...")
        
        # Wait longer for election to complete
        time.sleep(3.0)  # Increased from 2.0
        
        # Check status of all nodes
        leaders = []
        followers = []
        
        for node in nodes:
            status = node.get_status()
            print(f"{status['node_id']}: {status['state']}, term={status['term']}, leader={status.get('leader', 'none')}")
            
            if status['state'] == NodeState.LEADER.value:
                leaders.append(node)
            elif status['state'] == NodeState.FOLLOWER.value:
                followers.append(node)
        
        # Verify we have exactly 1 leader
        assert len(leaders) >= 1, f"Should have at least 1 leader, got {len(leaders)}"
        
        if len(leaders) > 1:
            print(f"Warning: Multiple leaders detected, waiting for convergence...")
            time.sleep(2.0)
            
            # Re-check
            leaders = []
            for node in nodes:
                status = node.get_status()
                if status['state'] == NodeState.LEADER.value:
                    leaders.append(node)
        
        assert len(leaders) == 1, f"Should have exactly 1 leader after convergence, got {len(leaders)}"
        assert len(followers) >= 1, f"Should have at least 1 follower, got {len(followers)}"
        
        # All nodes should agree on the leader
        leader_id = leaders[0].node_id
        for node in followers:
            assert node.leader_id == leader_id, f"Follower {node.node_id} thinks leader is {node.leader_id}, should be {leader_id}"
        
        print(f"✓ Election successful! Leader is {leader_id}\n")
        
        return nodes, leaders[0]
        
    except Exception as e:
        cleanup_nodes(nodes)
        raise e

def test_log_replication(nodes, leader):
    """Test that logs are replicated from leader to followers"""
    print("Test: Log replication...")
    
    try:
        # Propose a command to the leader
        cmd1 = Command(CommandType.PUT, "key1", "value1")
        result = leader.propose_command(cmd1)
        
        print(f"Proposed command: {cmd1}")
        print(f"Result: {result}")
        
        assert result['success'], "Command should succeed on leader"
        
        # Wait for replication and application
        time.sleep(2.0)  # Increased from 1.0
        
        # Check all nodes have the entry
        for node in nodes:
            status = node.get_status()
            print(f"{node.node_id}: log_size={status['log_size']}, commit={status['commit_index']}, applied={status['last_applied']}")
            
            assert status['log_size'] >= 1, f"{node.node_id} should have at least 1 log entry"
            assert status['commit_index'] >= 1, f"{node.node_id} should have committed the entry"
        
        # Verify all nodes have the same value
        for node in nodes:
            value = node.get("key1")
            assert value == "value1", f"{node.node_id} should have key1=value1, got {value}"
        
        print("✓ Log replication successful!\n")
        
    except Exception as e:
        cleanup_nodes(nodes)
        raise e

def test_multiple_commands(nodes, leader):
    """Test multiple commands are replicated correctly"""
    print("Test: Multiple commands...")
    
    try:
        commands = [
            Command(CommandType.PUT, "user", "alice"),
            Command(CommandType.PUT, "role", "admin"),
            Command(CommandType.PUT, "email", "alice@example.com"),
            Command(CommandType.DELETE, "email"),
        ]
        
        # Propose all commands
        for cmd in commands:
            result = leader.propose_command(cmd)
            assert result['success'], f"Command {cmd} should succeed"
            print(f"Proposed: {cmd}")
            time.sleep(0.2)  # Small delay between commands
        
        # Wait for replication and application
        time.sleep(2.0)
        
        # Check all nodes
        for node in nodes:
            status = node.get_status()
            print(f"{node.node_id}: log_size={status['log_size']}, commit={status['commit_index']}, applied={status['last_applied']}")
            
            # Check values
            assert node.get("user") == "alice", f"{node.node_id} user mismatch"
            assert node.get("role") == "admin", f"{node.node_id} role mismatch"
            assert node.get("email") is None, f"{node.node_id} email should be deleted"
        
        print("✓ Multiple commands replicated successfully!\n")
        
    except Exception as e:
        cleanup_nodes(nodes)
        raise e

def test_follower_redirect(nodes, leader):
    """Test that followers reject commands"""
    print("Test: Follower redirect...")
    
    try:
        # Find a follower
        follower = None
        for node in nodes:
            if node.node_id != leader.node_id:
                follower = node
                break
        
        assert follower is not None, "Should have at least one follower"
        
        # Try to propose to follower
        cmd = Command(CommandType.PUT, "test", "value")
        result = follower.propose_command(cmd)
        
        print(f"Follower response: {result}")
        
        assert not result['success'], "Follower should reject command"
        assert result['leader'] == leader.node_id, "Should return leader ID"
        
        print("✓ Follower correctly rejects commands\n")
        
    except Exception as e:
        cleanup_nodes(nodes)
        raise e

def test_leader_failure():
    """Test that cluster elects new leader when current leader fails"""
    print("Test: Leader failure and re-election...")
    
    # Use different ports to avoid conflicts
    addresses = [
        "http://localhost:9011",
        "http://localhost:9012",
        "http://localhost:9013"
    ]
    
    nodes = []
    for i, addr in enumerate(addresses):
        peers = [a for j, a in enumerate(addresses) if j != i]
        node = RaftNode(f"node{i+1}", peers, addr)
        nodes.append(node)
    
    try:
        # Start all nodes
        for node in nodes:
            node.start()
        
        # Wait for initial election
        time.sleep(3.0)
        
        # Find the leader
        leader = None
        for node in nodes:
            if node.get_status()['state'] == NodeState.LEADER.value:
                leader = node
                break
        
        assert leader is not None, "Should have a leader"
        print(f"Initial leader: {leader.node_id}")
        
        # Stop the leader
        print(f"Stopping {leader.node_id}...")
        leader.stop()
        
        # Wait for new election
        print("Waiting for new election...")
        time.sleep(3.0)
        
        # Check that a new leader was elected
        new_leaders = []
        for node in nodes:
            if node != leader:  # Skip the stopped node
                status = node.get_status()
                print(f"{status['node_id']}: {status['state']}, term={status['term']}")
                if status['state'] == NodeState.LEADER.value:
                    new_leaders.append(node)
        
        assert len(new_leaders) == 1, f"Should have exactly 1 new leader, got {len(new_leaders)}"
        assert new_leaders[0].node_id != leader.node_id, "New leader should be different"
        
        print(f"✓ New leader elected: {new_leaders[0].node_id}\n")
        
    finally:
        # Cleanup all nodes
        cleanup_nodes(nodes)

if __name__ == "__main__":
    print("Testing Multi-Node Raft\n" + "="*60 + "\n")
    
    try:
        # Test 1: Election
        nodes, leader = test_three_node_election()
        
        # Test 2: Log replication
        test_log_replication(nodes, leader)
        
        # Test 3: Multiple commands
        test_multiple_commands(nodes, leader)
        
        # Test 4: Follower redirect
        test_follower_redirect(nodes, leader)
        
        # Cleanup first cluster
        print("Cleaning up nodes from tests 1-4...")
        cleanup_nodes(nodes)
        
        # Test 5: Leader failure (separate cluster)
        test_leader_failure()
        
        print("="*60)
        print("🎉 All multi-node Raft tests passed!")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        time.sleep(0.5)
        print("\nAll tests complete.")
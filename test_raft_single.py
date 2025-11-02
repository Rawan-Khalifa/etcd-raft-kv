import time
from raft_node import RaftNode, NodeState
from command import Command, CommandType

def test_single_node_startup():
    """Test that a single node becomes leader"""
    node = RaftNode("node1", peers=[])
    node.start()
    
    # Give it time to timeout and elect itself
    time.sleep(0.5)
    
    status = node.get_status()
    print(f"Status: {status}")
    
    assert status['state'] == NodeState.LEADER.value, "Single node should become leader"
    assert status['leader'] == 'node1', "Should recognize itself as leader"
    assert status['term'] > 0, "Term should have incremented"
    
    print("✓ Single node became leader\n")
    
    node.stop()

def test_single_node_command():
    """Test proposing commands to a single-node cluster"""
    node = RaftNode("node1", peers=[])
    node.start()
    
    # Wait for election
    time.sleep(0.5)
    
    # Propose a command
    cmd = Command(CommandType.PUT, "name", "alice")
    result = node.propose_command(cmd)
    
    print(f"Propose result: {result}")
    assert result['success'], "Command should succeed"
    assert result['index'] == 1, "Should be first log entry"
    
    # Give time for apply
    time.sleep(0.1)
    
    # Check that it was applied
    value = node.get("name")
    assert value == "alice", "Command should be applied to store"
    
    print("✓ Command was proposed and applied\n")
    
    node.stop()

def test_single_node_multiple_commands():
    """Test multiple commands in sequence"""
    node = RaftNode("node1", peers=[])
    node.start()
    
    time.sleep(0.5)  # Wait for election
    
    # Propose multiple commands
    commands = [
        Command(CommandType.PUT, "key1", "value1"),
        Command(CommandType.PUT, "key2", "value2"),
        Command(CommandType.PUT, "key3", "value3"),
        Command(CommandType.DELETE, "key1"),
    ]
    
    for cmd in commands:
        result = node.propose_command(cmd)
        assert result['success'], f"Command {cmd} should succeed"
        print(f"Proposed: {cmd}")
    
    # Wait for application
    time.sleep(0.2)
    
    # Verify results
    assert node.get("key1") is None, "key1 should be deleted"
    assert node.get("key2") == "value2", "key2 should exist"
    assert node.get("key3") == "value3", "key3 should exist"
    
    status = node.get_status()
    print(f"\nFinal status: {status}")
    assert status['log_size'] == 4, "Should have 4 log entries"
    assert status['commit_index'] == 4, "All should be committed"
    assert status['last_applied'] == 4, "All should be applied"
    
    print("✓ Multiple commands work correctly\n")
    
    node.stop()

def test_follower_rejects_commands():
    """Test that followers reject commands (redirect to leader)"""
    # Create node but don't let it become leader
    node = RaftNode("node1", peers=["node2", "node3"])  # Has peers
    node.start()
    
    # Don't wait long enough for election - stay as follower
    time.sleep(0.05)
    
    status = node.get_status()
    print(f"Status: {status}")
    assert status['state'] == NodeState.FOLLOWER.value, "Should still be follower"
    
    # Try to propose command as follower
    cmd = Command(CommandType.PUT, "test", "value")
    result = node.propose_command(cmd)
    
    print(f"Follower propose result: {result}")
    assert not result['success'], "Follower should reject commands"
    assert 'Not the leader' in result['error']
    
    print("✓ Follower correctly rejects commands\n")
    
    node.stop()

if __name__ == "__main__":
    print("Testing Single-Node Raft\n" + "="*50 + "\n")
    test_single_node_startup()
    test_single_node_command()
    test_single_node_multiple_commands()
    test_follower_rejects_commands()
    print("All single-node Raft tests passed!")
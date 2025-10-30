from command import Command, CommandType
from log import Log, LogEntry
from kvstore import KVStore
from state_machine import StateMachine

def test_command_serialization():
    """Test that commands can be serialized and deserialized"""
    # Create a PUT command
    cmd1 = Command(CommandType.PUT, "name", "etcd")
    json_str = cmd1.to_json()
    cmd2 = Command.from_json(json_str)
    
    assert cmd1 == cmd2, "Deserialized command should equal original"
    print(":) Command serialization works")

def test_log_operations():
    """Test basic log operations"""
    log = Log()
    
    # Append some commands
    cmd1 = Command(CommandType.PUT, "key1", "value1")
    cmd2 = Command(CommandType.PUT, "key2", "value2")
    cmd3 = Command(CommandType.DELETE, "key1")
    
    entry1 = log.append(term=1, command=cmd1)
    entry2 = log.append(term=1, command=cmd2)
    entry3 = log.append(term=2, command=cmd3)
    
    assert len(log) == 3, "Log should have 3 entries"
    assert log.last_index() == 3, "Last index should be 3"
    assert log.last_term() == 2, "Last term should be 2"
    
    # Retrieve entries
    retrieved = log.get(2)
    assert retrieved.command == cmd2, "Should retrieve correct entry"

    print(":) Log operations work")

def test_state_machine():
    """Test state machine applying commands"""
    store = KVStore()
    log = Log()
    sm = StateMachine(store, log)
    
    # Add commands to log
    cmd1 = Command(CommandType.PUT, "user", "alice")
    cmd2 = Command(CommandType.PUT, "role", "admin")
    
    log.append(term=1, command=cmd1)
    log.append(term=1, command=cmd2)
    
    # Initially, nothing is applied
    assert store.get("user") is None, "Store should be empty initially"
    
    # Simulate committing entries
    log.set_commit_index(2)
    
    # Apply committed entries
    sm.apply_committed_entries()
    
    # Now they should be in the store
    assert store.get("user") == "alice", "First command should be applied"
    assert store.get("role") == "admin", "Second command should be applied"
    assert log.get_last_applied() == 2, "Last applied should be updated"

    print(":) State machine applies commands correctly")

def test_state_machine_delete():
    """Test state machine DELETE command"""
    store = KVStore()
    log = Log()
    sm = StateMachine(store, log)
    
    # Put then delete
    cmd1 = Command(CommandType.PUT, "temp", "data")
    cmd2 = Command(CommandType.DELETE, "temp")
    
    log.append(term=1, command=cmd1)
    log.append(term=1, command=cmd2)
    log.set_commit_index(2)
    
    sm.apply_committed_entries()
    
    assert store.get("temp") is None, "Key should be deleted"
    print(":) State machine handles DELETE correctly")

def test_incremental_apply():
    """Test that state machine applies entries incrementally"""
    store = KVStore()
    log = Log()
    sm = StateMachine(store, log)
    
    # Add 5 commands
    for i in range(5):
        cmd = Command(CommandType.PUT, f"key{i}", f"value{i}")
        log.append(term=1, command=cmd)
    
    # Commit first 3
    log.set_commit_index(3)
    sm.apply_committed_entries()
    
    assert log.get_last_applied() == 3
    assert store.get("key2") == "value2"
    assert store.get("key3") is None, "Uncommitted entry shouldn't be applied"
    
    # Commit remaining 2
    log.set_commit_index(5)
    sm.apply_committed_entries()
    
    assert log.get_last_applied() == 5
    assert store.get("key4") == "value4"
    
    print(":) Incremental apply works correctly")

if __name__ == "__main__":
    test_command_serialization()
    test_log_operations()
    test_state_machine()
    test_state_machine_delete()
    test_incremental_apply()
    print("\n COOL :) All command/log tests passed!")
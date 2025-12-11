from raft.storage.kvstore import KVStore
from raft.core.command import Command, CommandType
from raft.core.log import Log
import threading

class StateMachine:
    """
    The state machine that applies commands to the KV store.
    
    This is what Raft is replicating - all nodes run the same
    state machine and apply the same commands in the same order.
    """
    
    def __init__(self, store: KVStore, log: Log, raft_node=None):
        """
        Initialize the state machine.
        
        Args:
            store: The KVStore to apply commands to
            log: The replicated log containing commands
            raft_node: Optional reference to RaftNode for membership changes
        """
        self.store = store
        self.log = log
        self.raft_node = raft_node  # For membership operations
        self._lock = threading.RLock() # yup for safety
    
    def apply_command(self, command: Command) -> any:
        """
        Apply a single command to the store.     
        Args:
            command: The command to execute
            
        Returns:
            The result of the operation (value for PUT, True/False for DELETE)
        """
        with self._lock:
            if command.type == CommandType.PUT:
                self.store.put(command.key, command.value)
                return command.value
            
            elif command.type == CommandType.DELETE:
                return self.store.delete(command.key)
            
            elif command.type == CommandType.MEMBERSHIP_ADD:
                if self.raft_node and hasattr(self.raft_node, 'dynamic_membership'):
                    peer_address = command.key
                    self.raft_node.dynamic_membership.apply_membership_change('add', peer_address)
                    # Update peers list
                    self.raft_node.peers = list(self.raft_node.dynamic_membership.current_peers)
                    print(f"[StateMachine] Added member {peer_address}, peers now: {self.raft_node.peers}")
                    return True
                return False
            
            elif command.type == CommandType.MEMBERSHIP_REMOVE:
                if self.raft_node and hasattr(self.raft_node, 'dynamic_membership'):
                    peer_address = command.key
                    self.raft_node.dynamic_membership.apply_membership_change('remove', peer_address)
                    # Update peers list
                    self.raft_node.peers = list(self.raft_node.dynamic_membership.current_peers)
                    print(f"[StateMachine] Removed member {peer_address}, peers now: {self.raft_node.peers}")
                    return True
                return False
            
            else:
                raise ValueError(f"Unknown command type: {command.type}")
    
    def apply_committed_entries(self):
        """
        Apply all committed but not-yet-applied entries from the log.
        
        will be called periodically to keep the state machine in sync
        with the committed log entries.
        """
        with self._lock:
            uncommitted = self.log.get_uncommitted_entries() # entries with index > last_applied and <= commit_index
            
            for entry in uncommitted:
                # Apply the command
                self.apply_command(entry.command)
                
                # Update last applied index
                self.log.set_last_applied(entry.index)
                
                print(f"[StateMachine] Applied entry {entry.index}: {entry.command}")
    
    def get(self, key: str):
        """
        Read operation - doesn't go through Raft.
        
        Reads can be served directly from the store since they
        don't modify state. (Note: this provides "relaxed" consistency)
        """
        return self.store.get(key)
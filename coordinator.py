import threading
import time
from kvstore import KVStore
from log import Log
from state_machine import StateMachine
from command import Command, CommandType
from raft_node import RaftNode


class Coordinator:
    """
    Coordinates commands, log, and state machine. Wraps a RaftNode.
    
    This provides a clean interface for the HTTP server
    while hiding Raft details.
    
    
    """
    
    def __init__(self, node_id: str = "node1", peers: list = None):
        """
        Initialize coordinator with a Raft node.
        
        Args:
            node_id: ID for this node
            peers: List of peer node IDs (empty for single-node)
        """
        if peers is None:
            peers = []
        
        self.raft_node = RaftNode(node_id, peers)
        self.raft_node.start()
    
    def propose_command(self, command: Command) -> dict:
        """Propose a command through Raft"""
        result = self.raft_node.propose_command(command)
        
        # If not leader, return error
        if not result['success']:
            return result
        
        # Wait briefly for command to be applied
        import time
        time.sleep(0.05)
        
        return result
        
    def get(self, key: str):
        """Read a value"""
        return self.raft_node.get(key)
    
    def get_status(self) -> dict:
        """Get status"""
        return self.raft_node.get_status()
    
    def shutdown(self):
        """Shutdown the coordinator"""
        self.raft_node.stop()
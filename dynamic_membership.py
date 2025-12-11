"""
Dynamic membership changes - add/remove nodes without restart
"""
import time
from typing import List, Set
from enum import Enum

class MembershipChange(Enum):
    ADD_SERVER = "add"
    REMOVE_SERVER = "remove"

class DynamicMembership:
    """Manages dynamic cluster membership"""
    
    def __init__(self, current_peers: List[str]):
        self.current_peers = set(current_peers)
        self.pending_changes = []
        self.change_log = []
    
    def add_server(self, peer_address: str) -> dict:
        """
        Request to add a server to cluster
        
        Returns:
            Status dict with success/error
        """
        if peer_address in self.current_peers:
            return {
                'success': False,
                'error': 'Server already in cluster'
            }
        
        self.pending_changes.append({
            'type': MembershipChange.ADD_SERVER,
            'server': peer_address,
            'timestamp': time.time()
        })
        
        return {
            'success': True,
            'message': f'Requested to add {peer_address}',
            'will_replicate': True
        }
    
    def remove_server(self, peer_address: str) -> dict:
        """
        Request to remove a server from cluster
        
        Returns:
            Status dict with success/error
        """
        if peer_address not in self.current_peers:
            return {
                'success': False,
                'error': 'Server not in cluster'
            }
        
        if len(self.current_peers) <= 1:
            return {
                'success': False,
                'error': 'Cannot remove last server'
            }
        
        self.pending_changes.append({
            'type': MembershipChange.REMOVE_SERVER,
            'server': peer_address,
            'timestamp': time.time()
        })
        
        return {
            'success': True,
            'message': f'Requested to remove {peer_address}',
            'will_replicate': True
        }
    
    def apply_membership_change(self, change_type: str, server: str):
        """Apply a membership change to current peers"""
        if change_type == MembershipChange.ADD_SERVER.value:
            self.current_peers.add(server)
            self.change_log.append(f"Added {server}")
        elif change_type == MembershipChange.REMOVE_SERVER.value:
            self.current_peers.discard(server)
            self.change_log.append(f"Removed {server}")
    
    def get_peers(self) -> List[str]:
        """Get current list of peers"""
        return list(self.current_peers)
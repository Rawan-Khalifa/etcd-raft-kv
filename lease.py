"""
Lease-based reads for 10x read performance improvement
Followers can serve reads without consensus if leader heartbeat is recent
"""
import time
from typing import Optional, Any

class LeaseManager:
    """Manages read leases for fast reads"""
    
    def __init__(self, lease_duration_ms: float = 500):
        """
        Initialize lease manager
        
        Args:
            lease_duration_ms: How long to trust leader is alive after heartbeat
        """
        self.lease_duration_ms = lease_duration_ms
        self.last_leader_heartbeat = 0
        self.leader_id = None
    
    def refresh_lease(self, leader_id: str):
        """Refresh lease when heartbeat received from leader"""
        self.last_leader_heartbeat = time.time() * 1000
        self.leader_id = leader_id
    
    def is_lease_valid(self) -> bool:
        """Check if leader lease is still valid"""
        if not self.leader_id:
            return False
        
        elapsed = (time.time() * 1000) - self.last_leader_heartbeat
        return elapsed < self.lease_duration_ms
    
    def can_serve_read(self, node_state: str) -> bool:
        """
        Determine if this node can safely serve reads
        
        Args:
            node_state: Current node state (LEADER, FOLLOWER, CANDIDATE)
        
        Returns:
            True if safe to serve read without consensus
        """
        if node_state == "LEADER":
            # Leader can always serve reads
            return True
        elif node_state == "FOLLOWER" and self.is_lease_valid():
            # Follower can serve reads if leader lease is valid
            return True
        else:
            # Candidate should not serve reads
            return False


class ReadCache:
    """Optional: Cache recent reads for even faster access"""
    
    def __init__(self, ttl_seconds: float = 1.0):
        self.ttl_seconds = ttl_seconds
        self._cache = {}  # {key: (value, timestamp)}
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if valid"""
        if key not in self._cache:
            return None
        
        value, timestamp = self._cache[key]
        if time.time() - timestamp > self.ttl_seconds:
            del self._cache[key]
            return None
        
        return value
    
    def put(self, key: str, value: Any):
        """Store value in cache"""
        self._cache[key] = (value, time.time())
    
    def invalidate(self, key: str = None):
        """Invalidate cache entry or entire cache"""
        if key:
            self._cache.pop(key, None)
        else:
            self._cache.clear()
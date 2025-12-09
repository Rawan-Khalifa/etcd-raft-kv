"""
Production-grade Raft client with auto leader discovery and retry logic
"""
import time
import requests
from typing import Optional, Dict, Any
from enum import Enum
import json

class ClientError(Exception):
    """Custom exception for client errors"""
    pass

class RaftClient:
    """
    Smart Raft client with:
    - Automatic leader discovery
    - Retry logic with exponential backoff
    - Connection pooling
    - Health checking
    """
    
    def __init__(self, 
                 seed_peers: list,
                 timeout: float = 5.0,
                 max_retries: int = 3,
                 read_preference: str = "leader"):
        """
        Initialize Raft client
        
        Args:
            seed_peers: List of node addresses to bootstrap (e.g., ["http://localhost:9010"])
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            read_preference: "leader" (consistent) or "any" (fast, may be stale)
        """
        self.seed_peers = seed_peers
        self.timeout = timeout
        self.max_retries = max_retries
        self.read_preference = read_preference
        
        self.known_peers = set(seed_peers)
        self.leader = None
        self.last_leader_discovery = 0
        self.discovery_interval = 30  # Re-discover leader every 30s
        
        # Health tracking
        self.peer_health = {peer: True for peer in seed_peers}
        self.discovery_attempts = 0
    
    def _discover_leader(self):
        """Discover current leader by querying all known peers"""
        self.discovery_attempts += 1
        
        for peer in list(self.known_peers):
            try:
                response = requests.get(
                    f"{peer}/status",
                    timeout=self.timeout / 2
                )
                status = response.json()
                
                # Found a live node
                self.peer_health[peer] = True
                
                if status['state'] == 'LEADER':
                    self.leader = peer
                    self.last_leader_discovery = time.time()
                    print(f"[Client] Discovered leader: {self.leader}")
                    return True
                
                # Add newly discovered peers
                if 'peers' in status:
                    for peer_addr in status['peers']:
                        if peer_addr not in self.known_peers:
                            self.known_peers.add(peer_addr)
                            self.peer_health[peer_addr] = True
            
            except Exception as e:
                self.peer_health[peer] = False
                continue
        
        return False
    
    def _ensure_leader(self):
        """Ensure we have a valid leader"""
        if self.leader and time.time() - self.last_leader_discovery < self.discovery_interval:
            return True
        
        # Need to discover leader
        return self._discover_leader()
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry"""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    wait_time = (2 ** attempt) * 0.1  # 0.1s, 0.2s, 0.4s
                    print(f"[Client] Retry {attempt + 1}/{self.max_retries} after {wait_time}s")
                    time.sleep(wait_time)
                    
                    # If write failed, try to rediscover leader
                    if "Not the leader" in str(e):
                        self._discover_leader()
        
        raise last_error or ClientError("All retries failed")
    
    def put(self, key: str, value: str) -> Dict[str, Any]:
        """
        Put key-value pair (requires leader)
        
        Returns:
            Response dict with success status
        """
        def _do_put():
            if not self._ensure_leader():
                raise ClientError("Cannot discover leader")
            
            try:
                response = requests.put(
                    f"{self.leader}/kv/{key}",
                    json={"value": value},
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 503:
                    # Not leader
                    leader_hint = response.json().get('leader')
                    if leader_hint:
                        self.leader = leader_hint
                    raise ClientError("Not the leader")
                else:
                    raise ClientError(f"Server error: {response.status_code}")
            
            except requests.exceptions.RequestException as e:
                self.peer_health[self.leader] = False
                self.leader = None
                raise ClientError(f"Request failed: {e}")
        
        return self._retry_with_backoff(_do_put)
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value for key
        
        Can read from any peer if read_preference="any" (faster, possibly stale)
        Or from leader if read_preference="leader" (consistent)
        """
        def _do_get():
            if self.read_preference == "leader":
                # Consistent read from leader
                if not self._ensure_leader():
                    raise ClientError("Cannot discover leader")
                peer = self.leader
            else:
                # Fast read from any healthy peer
                healthy_peers = [p for p, health in self.peer_health.items() if health]
                if not healthy_peers:
                    if not self._discover_leader():
                        raise ClientError("No healthy peers available")
                    peer = self.leader
                else:
                    peer = healthy_peers[0]
            
            try:
                response = requests.get(
                    f"{peer}/kv/{key}",
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    return response.json().get('value')
                elif response.status_code == 404:
                    return None
                else:
                    raise ClientError(f"Server error: {response.status_code}")
            
            except requests.exceptions.RequestException as e:
                self.peer_health[peer] = False
                raise ClientError(f"Request failed: {e}")
        
        return self._retry_with_backoff(_do_get)
    
    def delete(self, key: str) -> bool:
        """Delete key-value pair (requires leader)"""
        def _do_delete():
            if not self._ensure_leader():
                raise ClientError("Cannot discover leader")
            
            try:
                response = requests.delete(
                    f"{self.leader}/kv/{key}",
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    return True
                elif response.status_code == 503:
                    raise ClientError("Not the leader")
                else:
                    raise ClientError(f"Server error: {response.status_code}")
            
            except requests.exceptions.RequestException as e:
                self.peer_health[self.leader] = False
                self.leader = None
                raise ClientError(f"Request failed: {e}")
        
        return self._retry_with_backoff(_do_delete)
    
    def get_status(self) -> Dict[str, Any]:
        """Get cluster status"""
        for peer in list(self.known_peers):
            try:
                response = requests.get(
                    f"{peer}/status",
                    timeout=self.timeout
                )
                return response.json()
            except:
                continue
        
        raise ClientError("Cannot reach any peer")
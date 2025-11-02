import threading
import time
import random
from enum import Enum
from typing import Optional, List, Set
from log import Log
from kvstore import KVStore
from state_machine import StateMachine
from command import Command

class NodeState(Enum):
    """The three possible states a Raft node can be in"""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

class RaftNode:
    """
    A Raft consensus node.
    
    This implements the Raft protocol for distributed consensus.
    Each node maintains a replicated log and participates in
    leader election and log replication.
    """
    
    def __init__(self, node_id: str, peers: List[str]):
        """
        Initialize a Raft node.
        
        Args:
            node_id: Unique identifier for this node (e.g., "node1")
            peers: List of peer node IDs (e.g., ["node2", "node3"])
        """
        self.node_id = node_id
        self.peers = peers  # Other nodes in the cluster
        
        # Raft state - Persistent (should be saved to disk in production)
        self.current_term = 0
        self.voted_for: Optional[str] = None  # Who we voted for in current term
        self.log = Log()
        
        # Raft state - Volatile (on all servers)
        self.commit_index = 0  # Highest log entry known to be committed
        self.last_applied = 0  # Highest log entry applied to state machine
        
        # Raft state - Volatile (on leaders only, reinitialized after election)
        self.next_index = {}   # For each peer: index of next log entry to send
        self.match_index = {}  # For each peer: index of highest log entry known to be replicated
        
        # Node state
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None
        
        # State machine
        self.store = KVStore()
        self.state_machine = StateMachine(self.store, self.log)
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
        
        # Threading
        self._lock = threading.RLock()
        self._running = False
        self._election_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._apply_thread: Optional[threading.Thread] = None
        
        print(f"[{self.node_id}] Initialized as FOLLOWER in term 0")
    
    def _random_election_timeout(self) -> float:
        """
        Generate a random election timeout between 150-300ms.
        
        Randomization prevents split votes - different nodes
        timeout at different times.
        """
        return random.uniform(0.15, 0.30)
    
    def start(self):
        """Start the Raft node (begin election timer and other loops)"""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            
            # Start election timer thread
            self._election_thread = threading.Thread(
                target=self._election_timer_loop,
                daemon=True
            )
            self._election_thread.start()
            
            # Start apply loop thread
            self._apply_thread = threading.Thread(
                target=self._apply_loop,
                daemon=True
            )
            self._apply_thread.start()
            
            print(f"[{self.node_id}] Started")
    
    def stop(self):
        """Stop the Raft node"""
        with self._lock:
            print(f"[{self.node_id}] Stopping...")
            self._running = False
            
            # Wait for threads to finish
            if self._election_thread:
                self._election_thread.join(timeout=1.0)
            if self._heartbeat_thread:
                self._heartbeat_thread.join(timeout=1.0)
            if self._apply_thread:
                self._apply_thread.join(timeout=1.0)
    
    def _become_follower(self, term: int):
        """
        Transition to follower state.
        
        Args:
            term: The term to transition to
        """
        with self._lock:
            self.state = NodeState.FOLLOWER
            self.current_term = term
            self.voted_for = None
            self.leader_id = None
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()
            
            print(f"[{self.node_id}] Became FOLLOWER in term {term}")
    
    def _become_candidate(self):
        """Transition to candidate state and start election"""
        with self._lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id  # Vote for ourselves
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()
            
            print(f"[{self.node_id}] Became CANDIDATE in term {self.current_term}")
            
            # Start election
            self._start_election()
    
    def _become_leader(self):
        """Transition to leader state"""
        with self._lock:
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            
            # Initialize leader state
            last_log_index = self.log.last_index()
            for peer in self.peers:
                self.next_index[peer] = last_log_index + 1
                self.match_index[peer] = 0
            
            print(f"[{self.node_id}] Became LEADER in term {self.current_term}")
            
            # Start sending heartbeats
            if self._heartbeat_thread is None or not self._heartbeat_thread.is_alive():
                self._heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop,
                    daemon=True
                )
                self._heartbeat_thread.start()
    
    def _election_timer_loop(self):
        """
        Background thread that monitors election timeout.
        
        If we don't hear from a leader within the timeout,
        we start an election.
        """
        while self._running:
            time.sleep(0.01)  # Check every 10ms
            
            with self._lock:
                # Only followers and candidates have election timeouts
                if self.state == NodeState.LEADER:
                    continue
                
                # Check if election timeout has elapsed
                elapsed = time.time() - self.last_heartbeat
                if elapsed >= self.election_timeout:
                    print(f"[{self.node_id}] Election timeout! (elapsed: {elapsed:.3f}s)")
                    self._become_candidate()
    
    def _heartbeat_loop(self):
        """
        Background thread for leader to send periodic heartbeats.
        
        Heartbeats are empty AppendEntries messages that prevent
        followers from starting elections.
        """
        while self._running:
            with self._lock:
                if self.state != NodeState.LEADER:
                    break
            
            # Send heartbeats to all peers
            self._send_heartbeats()
            
            # Send heartbeats every 50ms (well before election timeout)
            time.sleep(0.05)
    
    def _apply_loop(self):
        """
        Background thread that applies committed entries to state machine.
        """
        while self._running:
            time.sleep(0.01)
            
            with self._lock:
                # Check if there are committed entries to apply
                if self.commit_index > self.last_applied:
                    # Apply entries from last_applied+1 to commit_index
                    for i in range(self.last_applied + 1, self.commit_index + 1):
                        entry = self.log.get(i)
                        if entry:
                            self.state_machine.apply_command(entry.command)
                            self.last_applied = i
                            print(f"[{self.node_id}] Applied entry {i}: {entry.command}")
    
    def _start_election(self):
        """
        Start an election by requesting votes from all peers.
        
        This is called when a node becomes a candidate.
        """
        with self._lock:
            term = self.current_term
            candidate_id = self.node_id
            last_log_index = self.log.last_index()
            last_log_term = self.log.last_term()
        
        print(f"[{self.node_id}] Starting election for term {term}")
        
        # In a real implementation, we'd send RequestVote RPCs to all peers
        # For now, we'll simulate this in a single-node or multi-node setup
        
        # Count votes (we already voted for ourselves)
        votes_received = 1
        votes_needed = (len(self.peers) + 1) // 2 + 1  # Majority
        
        print(f"[{self.node_id}] Election: got {votes_received} votes, need {votes_needed}")
        
        # TODO: Send RequestVote RPC to all peers and count responses
        # For now, in single-node mode, we win immediately
        if len(self.peers) == 0:
            print(f"[{self.node_id}] Single-node cluster, won election!")
            self._become_leader()
    
    def _send_heartbeats(self):
        """
        Send heartbeat (empty AppendEntries) to all followers.
        """
        # TODO: Implement actual RPC calls
        # For now, just log that we're sending heartbeats
        with self._lock:
            if self.state == NodeState.LEADER:
                pass  # In multi-node, would send AppendEntries RPC here
    
    def propose_command(self, command: Command) -> dict:
        """
        Propose a command to be added to the log.
        
        Only the leader can accept commands. Followers redirect to leader.
        
        Args:
            command: The command to propose
            
        Returns:
            dict with success status and details
        """
        with self._lock:
            # Only leader can accept commands
            if self.state != NodeState.LEADER:
                return {
                    'success': False,
                    'error': 'Not the leader',
                    'leader': self.leader_id
                }
            
            # Append to our log
            entry = self.log.append(self.current_term, command)
            print(f"[{self.node_id}] Proposed {command} at index {entry.index}")
            
            # TODO: Replicate to followers
            # For single-node, immediately commit
            if len(self.peers) == 0:
                self.commit_index = entry.index
                return {
                    'success': True,
                    'index': entry.index,
                    'term': entry.term
                }
            
            # For multi-node, would wait for replication
            return {
                'success': True,
                'index': entry.index,
                'term': entry.term
            }
    
    def get(self, key: str):
        """Read operation (doesn't need consensus)"""
        return self.store.get(key)
    
    def get_status(self) -> dict:
        """Get node status for debugging"""
        with self._lock:
            return {
                'node_id': self.node_id,
                'state': self.state.value,
                'term': self.current_term,
                'leader': self.leader_id,
                'log_size': len(self.log),
                'commit_index': self.commit_index,
                'last_applied': self.last_applied,
                'peers': self.peers
            }
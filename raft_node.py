import sys
import threading
import time
import random
from enum import Enum
from typing import Optional, List, Set
from log import Log
from kvstore import KVStore
from state_machine import StateMachine
from command import Command
from wal import WriteAheadLog

from rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    LogEntryData
)
from raft_grpc_client import RaftGRPCClient

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
    
    def __init__(self, node_id: str, peers: List[str], address: str, enable_persistence=True, snapshot_interval=100):
        """
        Initialize a Raft node with snapshot support.
        
        Args:
            node_id: Unique identifier for this node (e.g., "node1")
            peers: List of peer addresses (e.g., ["http://localhost:8081", ...])
            address: This node's address (e.g., "http://localhost:8080")
            enable_persistence: Whether to persist state to disk
            snapshot_interval: Create snapshot every N log entries
        """
        self.node_id = node_id
        self.peers = peers  # Other nodes in the cluster
        self.address = address  # This node's address

        # Create per-node random generator for election timeouts
        import hashlib
        seed = int(hashlib.md5(f"{node_id}{address}{time.time()}".encode()).hexdigest()[:8], 16)
        self._random = random.Random(seed)

        # Create gRPC client
        self.rpc_client = RaftGRPCClient()

        # Persistence - Initialize BEFORE loading state
        self.enable_persistence = enable_persistence
        self.wal = WriteAheadLog(node_id, snapshot_interval=snapshot_interval) if enable_persistence else None

        # Snapshot tracking
        self.snapshot_interval = snapshot_interval
        self.last_snapshot_index = 0
        self.last_snapshot_term = 0
    
        # Raft state - Persistent (load from disk if available)
        self.current_term = 0
        self.voted_for: Optional[str] = None  # Who we voted for in current term
        self.log = Log()
        
        # Load persistent state from disk BEFORE creating log
        if self.wal:
            persistent_state = self.wal.load_persistent_state()
            self.current_term = persistent_state.current_term
            self.voted_for = persistent_state.voted_for
            
            print(f"[{self.node_id}] Recovered persistent state: term={self.current_term}, voted_for={self.voted_for}")
            
            # Recover log entries from disk
            log_entries = self.wal.get_all_entries()
            print(f"[{self.node_id}] Recovering {len(log_entries)} log entries from disk")
            
            for entry in log_entries:
                cmd = Command.from_dict(entry['command'])
                self.log.append(entry['term'], cmd)

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
        
        # Load snapshot state if available (MUST be after KVStore creation)
        if self.wal:
            snapshot_state = self.wal.load_snapshot()
            if snapshot_state:
                print(f"[{self.node_id}] Restoring KV store from snapshot ({len(snapshot_state)} keys)")
                self.store._data = snapshot_state
                
                # Get snapshot metadata
                snap_info = self.wal.get_snapshot_info()
                self.last_snapshot_index = snap_info['last_snapshot_index']
                self.last_snapshot_term = snap_info['last_snapshot_term']
                
                print(f"[{self.node_id}] Restored from snapshot at index {self.last_snapshot_index}")
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()
        
        # Threading
        self._lock = threading.RLock()
        self._running = False
        self._election_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._apply_thread: Optional[threading.Thread] = None
        self._server_thread: Optional[threading.Thread] = None

        print(f"[{self.node_id}] Initialized - log_size={self.log.last_index()}, term={self.current_term}, snapshot_index={self.last_snapshot_index}")


    def _random_election_timeout(self) -> float:
        """
        Generate a random election timeout.
        
        Raft paper recommends: "election timeout should be an order 
        of magnitude larger than heartbeat interval"
        
        With 20ms heartbeats, we use 300-600ms with good variance.
        """
        return random.uniform(0.2, 1.5)  # Back to 300-600ms but ensure proper randomization

    def start(self):
        """Start the Raft node with recovery"""
        with self._lock:
            if self._running:
                print(f"[{self.node_id}] Already running, ignoring start()")
                return
            
            self._running = True
            
            # Recover log from disk if not already done in __init__
            if self.wal and self.log.last_index() == 0:
                entries = self.wal.get_all_entries()
                print(f"[{self.node_id}] Recovering {len(entries)} log entries from disk")
                
                for entry in entries:
                    cmd = Command.from_dict(entry['command'])
                    self.log.append(entry['term'], cmd)
        
            # Start BOTH gRPC (for inter-node Raft) and HTTP (for client APIs)
            from raft_grpc_server import create_grpc_server
            from raft_http_server import create_raft_rpc_server
            from urllib.parse import urlparse
            
            parsed = urlparse(self.address)
            host = parsed.hostname or 'localhost'
            port = parsed.port

            if port is None:
                print(f"[{self.node_id}] ERROR: No port in address {self.address}")
                port = 8080

            print(f"[{self.node_id}] Starting gRPC server on {host}:{port}")
            
            try:
                # Start gRPC server for inter-node Raft RPC
                grpc_server = create_grpc_server(self, 'localhost', self._get_port_from_address())
                self._grpc_server = grpc_server
                print(f"[{self.node_id}] ✓ gRPC server started (Raft RPC)")
                
                # ALSO start HTTP server for client APIs (/status, /kv/*)
                print(f"[{self.node_id}] Starting HTTP server for client APIs...")
                http_server = create_raft_rpc_server(self, host, port)
                self._server = http_server
                
                self._server_thread = threading.Thread(
                    target=self._server.serve_forever,
                    daemon=True
                )
                self._server_thread.start()
                
                # Give server time to start
                time.sleep(0.2)
                
                # Test if the HTTP server is listening
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    print(f"[{self.node_id}] ✓ HTTP server listening on {host}:{port}")
                else:
                    print(f"[{self.node_id}] ✗ WARNING: HTTP server may not be listening")

            except Exception as e:
                print(f"[{self.node_id}] ✗ ERROR starting servers: {e}")
                import traceback
                traceback.print_exc()
                raise
    
        # Start other threads...
        self._election_thread = threading.Thread(
            target=self._election_timer_loop,
            daemon=True
        )
        self._election_thread.start()
        
        self._apply_thread = threading.Thread(
            target=self._apply_loop,
            daemon=True
        )
        self._apply_thread.start()
        
        print(f"[{self.node_id}] Started")

    def _get_port_from_address(self) -> int:
        """Extract port number from address string"""
        # address format: "localhost:9001" or "127.0.0.1:9001"
        parts = self.address.split(':')
        return int(parts[-1])
    
    def stop(self):
        """Stop the Raft node"""
        print(f"[{self.node_id}] Stopping...")
        
        with self._lock:
            if not self._running:
                return
            self._running = False
        
        # Shutdown server first
        if hasattr(self, '_grpc_server'):
            try:
                self._grpc_server.stop(grace=2)
            except:
                pass
    
        # Give threads a moment to notice _running = False
        time.sleep(0.2)
        
        # Don't wait forever for threads
        threads_to_join = [
            ('election', self._election_thread),
            ('heartbeat', self._heartbeat_thread),
            ('apply', self._apply_thread),
            ('server', self._server_thread)
        ]
        
        for name, thread in threads_to_join:
            if thread and thread.is_alive():
                thread.join(timeout=0.5)  # Max 0.5s per thread
                if thread.is_alive():
                    print(f"[{self.node_id}] Warning: {name} thread didn't stop cleanly")
    
        print(f"[{self.node_id}] Stopped")
        
    def _become_follower(self, term: int):
        """Transition to follower state"""
        with self._lock:
            old_state = self.state
            old_term = self.current_term
            
            self.state = NodeState.FOLLOWER
            self.current_term = term
            self.voted_for = None
            self.leader_id = None
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()
            
            # Persist state to disk
            if self.wal:
                self.wal.save_persistent_state(self.current_term, self.voted_for)
            
            if old_state != NodeState.FOLLOWER or old_term != term:
                print(f"[{self.node_id}] Became FOLLOWER in term {term}")
            

    def _become_candidate(self):
        """Transition to candidate state and start election"""
        with self._lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id  # Vote for ourselves
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()
            
            # Persist vote to disk BEFORE starting election
            if self.wal:
                self.wal.save_persistent_state(self.current_term, self.voted_for)
            
            print(f"[{self.node_id}] Became CANDIDATE in term {self.current_term}")
    
        # Start election outside the lock
        self._start_election()


    def _become_leader(self):
        """Transition to leader state"""
        with self._lock:
            # Safety check - make sure we actually won
            if self.voted_for != self.node_id:
                print(f"[{self.node_id}] ERROR: Cannot become leader, voted for {self.voted_for}")
                return
            
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            
            # Initialize leader state
            last_log_index = self.log.last_index()
            for peer in self.peers:
                self.next_index[peer] = last_log_index + 1
                self.match_index[peer] = 0
            
            # ADD THIS DEBUG LINE:
            print(f"[{self.node_id}] Leader initialized with peers: {self.peers}")
            print(f"[{self.node_id}] next_index: {self.next_index}")
            
            # Always start a NEW heartbeat thread
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                daemon=True,
                name=f"{self.node_id}-heartbeat-term{self.current_term}"
            )
            self._heartbeat_thread.start()
            
            print(f"[{self.node_id}] Started heartbeat thread")
    
        # Send immediate heartbeats outside lock
        # This is CRITICAL - establish leadership immediately
        print(f"[{self.node_id}] Sending initial heartbeats")
        try:
            self._send_heartbeats()
            # Give a moment for heartbeats to be sent
            time.sleep(0.01)
        except Exception as e:
            print(f"[{self.node_id}] Error sending initial heartbeats: {e}")
    
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
        """Background thread for leader to send periodic heartbeats"""
        print(f"[{self.node_id}] Heartbeat loop started")
        
        while self._running:
            # Check if we're still leader
            with self._lock:
                if self.state != NodeState.LEADER:
                    print(f"[{self.node_id}] Heartbeat loop exiting - no longer leader (state={self.state.value})")
                    return
            
            # Send heartbeats
            try:
                self._send_heartbeats()
            except Exception as e:
                print(f"[{self.node_id}] Error sending heartbeats: {e}")
            
            # Sleep before next heartbeat - FASTER!
            time.sleep(0.02)  # Changed from 0.05 to 0.02 (20ms instead of 50ms)
        
        print(f"[{self.node_id}] Heartbeat loop exiting - node stopping")
    
    # Updated to trigger snapshots
    def _apply_loop(self):
        """
        Background thread that applies committed entries to state machine.
        Also triggers periodic snapshots.
        """
        snapshot_check_interval = 0.5  # Check every 500ms
        last_snapshot_check = time.time()
        
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
                
                # Periodically check if we should snapshot
                current_time = time.time()
                if current_time - last_snapshot_check >= snapshot_check_interval:
                    last_snapshot_check = current_time
                    
                    # Check if enough entries since last snapshot
                    if self.wal and self.wal.snapshot_manager.should_snapshot(self.log.last_index()):
                        self._create_snapshot()

    # Add new method for snapshot creation:
    def _create_snapshot(self):
        """Create a snapshot of current state and compact log"""
        with self._lock:
            if not self.wal:
                return
            
            try:
                # Only snapshot if we have applied entries beyond last snapshot
                if self.last_applied <= self.last_snapshot_index:
                    return
                
                # Get current state
                log_index = self.last_applied  # Snapshot includes all applied entries
                
                # Get the term of the entry we're snapshotting
                entry = self.log.get(log_index)
                if not entry:
                    print(f"[{self.node_id}] Cannot snapshot - entry {log_index} not found")
                    return
                
                log_term = entry.term

                # Get KV store state
                store_state = dict(self.store._data)
                
                # Create snapshot
                success = self.wal.create_snapshot(log_index, log_term, store_state)
                
                if success:
                    # Update our tracking
                    self.last_snapshot_index = log_index
                    self.last_snapshot_term = log_term
                    
                    # Now we can discard log entries before this snapshot
                    # Keep a few entries for safety (in case of edge cases)
                    min_keep_index = max(1, log_index - 10)
                    self.wal.truncate_log(min_keep_index)
                    
                    # Clean up old snapshots (keep only 3 most recent)
                    self.wal.snapshot_manager.cleanup_old_snapshots(keep_count=3)
                    
                    print(f"[{self.node_id}] Snapshot created at index {log_index}")
                    print(f"[{self.node_id}]   Log now starts at index {min_keep_index}")
                    print(f"[{self.node_id}]   Snapshot contains {len(store_state)} keys")
                    
            except Exception as e:
                print(f"[{self.node_id}] ERROR creating snapshot: {e}")
                import traceback
                traceback.print_exc()
    
    def _start_election(self, term: int = None):
        """Start an election by requesting votes from all peers"""
        with self._lock:
            # Use provided term or current term
            if term is None:
                term = self.current_term
            
            # Verify we're still in this term and still a candidate
            if self.current_term != term or self.state != NodeState.CANDIDATE:
                print(f"[{self.node_id}] Election aborted - term changed or not candidate anymore")
                return
            
            candidate_id = self.node_id
            last_log_index = self.log.last_index()
            last_log_term = self.log.last_term()
    
        print(f"[{self.node_id}] Starting election for term {term}")
        
        # Create vote request
        vote_request = RequestVoteRequest(
            term=term,
            candidate_id=candidate_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )
        
        # Count votes
        votes_received = 1  # Vote for ourselves
        votes_needed = (len(self.peers) // 2) + 1  # Majority needed
        
        print(f"[{self.node_id}] Need {votes_needed} votes from {len(self.peers)} peers")
        
        # If no peers, we're the only node - become leader immediately
        if len(self.peers) == 0:
            print(f"[{self.node_id}] No peers - single node cluster, becoming leader immediately")
            with self._lock:
                self._become_leader()
            return
        
        # Send RequestVote RPCs to all peers in parallel
        import concurrent.futures
        
        def request_vote_from_peer(peer_address):
            try:
                response = self.rpc_client.request_vote(peer_address, vote_request)
                return (peer_address, response)
            except Exception as e:
                print(f"[{self.node_id}] Exception requesting vote from {peer_address}: {e}")
                return (peer_address, None)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.peers)) as executor:
            futures = [executor.submit(request_vote_from_peer, peer) for peer in self.peers]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    peer_address, response = future.result()
                except Exception as e:
                    print(f"[{self.node_id}] Error in election: {e}")
                    continue
                
                if response is None:
                    print(f"[{self.node_id}] No response from {peer_address}")
                    continue
                
                with self._lock:
                    # If we discover a higher term, become follower immediately
                    if response.term > self.current_term:
                        print(f"[{self.node_id}] Discovered higher term {response.term}, stepping down")
                        self._become_follower(response.term)
                        return
                    
                    # Check if we're still a candidate in the same term
                    if self.state != NodeState.CANDIDATE or self.current_term != term:
                        print(f"[{self.node_id}] State changed during election (state={self.state.value}, term={self.current_term}), aborting")
                        return
                    
                    # Count vote
                    if response.vote_granted:
                        votes_received += 1
                        print(f"[{self.node_id}] Got vote from {peer_address} ({votes_received}/{votes_needed})")
                        
                        # Check if we won
                        if votes_received >= votes_needed:
                            print(f"[{self.node_id}] Won election with {votes_received} votes! Becoming leader...")
                            self._become_leader()
                            print(f"[{self.node_id}] Leader transition complete. State is now {self.state.value}")
                            return
    
        # Didn't win election
        print(f"[{self.node_id}] Lost election (got {votes_received}/{votes_needed} votes)")
        
        with self._lock:
            # Reset for next election attempt
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()
    
    def _send_heartbeats(self):
        """Send heartbeats/log entries to all followers"""
        with self._lock:
            if self.state != NodeState.LEADER:
                return
            
            leader_commit = self.commit_index
            peers_list = list(self.peers)
            current_term = self.current_term
            
            # ADD THIS:
            print(f"[{self.node_id}] About to send heartbeats to {len(peers_list)} peers: {peers_list}")
    
        # Send to each peer (outside lock)
        success_count = 0
        for peer_address in peers_list:
            print(f"[{self.node_id}] Attempting to replicate to {peer_address}...")  # ADD THIS
            try:
                result = self._replicate_to_peer(peer_address, leader_commit)
                if result:
                    success_count += 1
                    print(f"[{self.node_id}] ✓ Success replicating to {peer_address}")  # ADD THIS
                else:
                    print(f"[{self.node_id}] ✗ Failed replicating to {peer_address}")  # ADD THIS
            except Exception as e:
                print(f"[{self.node_id}] EXCEPTION replicating to {peer_address}: {e}")
                import traceback
                traceback.print_exc()  # ADD THIS to see full stack trace
        
        # Periodically log heartbeat status
        if not hasattr(self, '_heartbeat_count'):
            self._heartbeat_count = 0
        self._heartbeat_count += 1
        
        if self._heartbeat_count % 25 == 0:  # Every 25 heartbeats (~500ms)
            print(f"[{self.node_id}] Heartbeat status: {success_count}/{len(peers_list)} peers responding (term {current_term})")
            
    def _replicate_to_peer(self, peer_address: str, leader_commit: int) -> bool:
        """
        Replicate log entries to a specific peer.
        
        This implements the core log replication mechanism.
        Returns True if successful, False otherwise.
        """
        with self._lock:
            # Only proceed if we're still leader
            if self.state != NodeState.LEADER:
                return False
            
            # Save current term to detect if it changes during RPC
            current_term = self.current_term
            node_id = self.node_id
            
            # Get the next index to send to this peer
            next_idx = self.next_index.get(peer_address, 1)
            
            # Get prev log entry info
            prev_log_index = next_idx - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_entry = self.log.get(prev_log_index)
                if prev_entry:
                    prev_log_term = prev_entry.term
            
            # Get entries to send (from next_idx to end of log)
            entries_to_send = []
            last_log_index = self.log.last_index()
            
            if next_idx <= last_log_index:
                # There are entries to send
                for i in range(next_idx, last_log_index + 1):
                    entry = self.log.get(i)
                    if entry:
                        entries_to_send.append(LogEntryData(
                            index=entry.index,
                            term=entry.term,
                            command=entry.command.to_dict()
                        ))
            
            # Create request
            request = AppendEntriesRequest(
                term=current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries_to_send,
                leader_commit=leader_commit
            )
        # Log what we're sending
        print(f"[{node_id}] >>> Sending AppendEntries to {peer_address} (term={request.term})")
    
        
        # Send RPC (outside lock to avoid blocking)
        response = self.rpc_client.append_entries(peer_address, request)
        
        if response is None:
            # Log why it failed occasionally
            if not hasattr(self, f'_fail_count_{peer_address}'):
                setattr(self, f'_fail_count_{peer_address}', 0)
            fail_count = getattr(self, f'_fail_count_{peer_address}')
            if fail_count % 5 == 0:  # Log every 5th failure
                print(f"[{self.node_id}] Failed to replicate to {peer_address} ({fail_count} failures)")
            setattr(self, f'_fail_count_{peer_address}', fail_count + 1)
            return False
        
        # Reset failure count on success
        setattr(self, f'_fail_count_{peer_address}', 0)
        
        with self._lock:
            # If we discover a higher term, step down immediately
            if response.term > self.current_term:
                print(f"[{self.node_id}] Discovered higher term {response.term} from {peer_address}, stepping down")
                self._become_follower(response.term)
                return False  # FIXED: was "return" (None), now returns False
            
            # If our term changed while RPC was in flight, ignore this response
            if self.current_term != current_term:
                return False  # FIXED: was "return" (None), now returns False
            
            # Only process if we're still leader in the same term
            if self.state != NodeState.LEADER:
                return False  # FIXED: was "return" (None), now returns False
            
            if response.success:
                # Update next_index and match_index for follower
                self.match_index[peer_address] = response.match_index
                self.next_index[peer_address] = response.match_index + 1
                
                # Check if we can advance commit_index
                self._advance_commit_index()
                return True  # SUCCESS!
            else:
                # Replication failed, decrement next_index and retry
                self.next_index[peer_address] = max(1, self.next_index[peer_address] - 1)
                return False  # Failed but will retry

    def _advance_commit_index(self):
        """
        Advance commit_index based on what's been replicated to a majority.
        
        Leader commits an entry once it's replicated to a majority of servers.
        """
        # Count how many nodes have each index
        for n in range(self.commit_index + 1, self.log.last_index() + 1):
            # Count ourselves
            count = 1
            
            # Count peers that have this entry
            for peer_address in self.peers:
                if self.match_index.get(peer_address, 0) >= n:
                    count += 1
            
            # If majority has this entry, and it's from current term, commit it
            majority = (len(self.peers) + 1) // 2 + 1
            if count >= majority:
                entry = self.log.get(n)
                if entry and entry.term == self.current_term:
                    self.commit_index = n
                    print(f"[{self.node_id}] Advanced commit_index to {n}")
        
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
            
            # Append to log in memory
            entry = self.log.append(self.current_term, command)
            
            # Persist to disk BEFORE replicating
            if self.wal:
                if not self.wal.append_entry(entry.index, entry.term, command):
                    print(f"[{self.node_id}] ERROR: Failed to persist log entry {entry.index}")
                    return {
                        'success': False,
                        'error': 'Failed to persist to disk'
                    }
            
            print(f"[{self.node_id}] Proposed {command} at index {entry.index}")
            
            # For single-node clusters, immediately commit
            if len(self.peers) == 0:
                self.commit_index = entry.index
                print(f"[{self.node_id}] Single-node cluster: immediately committed entry {entry.index}")
        
            return {
                'success': True,
                'index': entry.index,
                'term': entry.term
            }

    def get(self, key: str):
        """Read operation (doesn't need consensus)"""
        return self.store.get(key)
    
    # Update get_status to include snapshot info:
    def get_status(self):
        """Get current node status for monitoring"""
        with self._lock:
            snapshot_info = {}
            if self.wal:
                snapshot_info = self.wal.get_snapshot_info()
            
            return {
                'node_id': self.node_id,
                'state': self.state.value,
                'term': self.current_term,
                'voted_for': self.voted_for,
                'leader_id': self.leader_id,
                'log_size': self.log.last_index(),
                'commit_index': self.commit_index,
                'last_applied': self.last_applied,
                'peers': list(self.peers) if self.peers else [],
                'snapshot': snapshot_info  # Add snapshot info
            }
        
    def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        """Handle RequestVote RPC from candidate with persistence"""
        with self._lock:
            # If candidate's term is older, reject
            if request.term < self.current_term:
                return RequestVoteResponse(
                    term=self.current_term,
                    vote_granted=False
                )
            
            # If candidate's term is newer, become follower
            if request.term > self.current_term:
                print(f"[{self.node_id}] Discovered higher term {request.term} in vote request, stepping down")
                self._become_follower(request.term)
            
            # If we're a leader in the same term, reject
            if request.term == self.current_term and self.state == NodeState.LEADER:
                print(f"[{self.node_id}] Rejecting vote - already leader in term {request.term}")
                return RequestVoteResponse(
                    term=self.current_term,
                    vote_granted=False
                )
            
            # Check if we can vote for this candidate
            can_vote = (
                (self.voted_for is None or self.voted_for == request.candidate_id) and
                self._is_log_up_to_date(request.last_log_index, request.last_log_term)
            )
            
            if can_vote:
                if self.voted_for != request.candidate_id:
                    print(f"[{self.node_id}] Granted vote to {request.candidate_id} in term {request.term}")
                
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()
                self.election_timeout = self._random_election_timeout()
                
                # Persist vote to disk
                if self.wal:
                    self.wal.save_persistent_state(self.current_term, self.voted_for)
        
            return RequestVoteResponse(
                term=self.current_term,
                vote_granted=can_vote
            )

    def _is_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """
        Check if candidate's log is at least as up-to-date as ours.
        
        Raft determines which of two logs is more up-to-date by comparing
        the index and term of the last entries in the logs.
        """
        our_last_term = self.log.last_term()
        our_last_index = self.log.last_index()
        
        # If terms differ, the log with later term is more up-to-date
        if last_log_term != our_last_term:
            return last_log_term > our_last_term
        
        # If terms are the same, the longer log is more up-to-date
        return last_log_index >= our_last_index

    def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle AppendEntries RPC from leader with persistence"""
        
        import sys
        sys.stdout.flush()
        print(f"[{self.node_id}] <<< RECEIVED AppendEntries from {request.leader_id} (term={request.term})", flush=True)
        
        if not hasattr(self, '_ae_count'):
            self._ae_count = 0
        self._ae_count += 1
        
        if self._ae_count % 25 == 0 or len(request.entries) > 0:
            print(f"[{self.node_id}] Received AppendEntries from {request.leader_id} (term {request.term}, entries={len(request.entries)})")
        
        with self._lock:
            # Reply false if term < currentTerm
            if request.term < self.current_term:
                return AppendEntriesResponse(
                    term=self.current_term,
                    success=False,
                    match_index=0
                )
            
            # Step down if we discover equal or higher term leader
            if request.term > self.current_term:
                self._become_follower(request.term)
            elif request.term == self.current_term:
                if self.state != NodeState.FOLLOWER:
                    print(f"[{self.node_id}] Stepping down - discovered leader {request.leader_id} in term {request.term}")
                    self._become_follower(request.term)
            
            # CRITICAL: ALWAYS set leader_id when receiving valid AppendEntries
            self.leader_id = request.leader_id
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()
            
            # Check log consistency
            if request.prev_log_index > 0:
                prev_entry = self.log.get(request.prev_log_index)
                
                if prev_entry is None:
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        match_index=0
                    )
                
                if prev_entry.term != request.prev_log_term:
                    self._delete_entries_from(request.prev_log_index)
                    return AppendEntriesResponse(
                        term=self.current_term,
                        success=False,
                        match_index=0
                    )
            
            # Append new entries
            for entry_data in request.entries:
                existing = self.log.get(entry_data.index)
                
                if existing is None:
                    cmd = Command.from_dict(entry_data.command)
                    self.log.append(entry_data.term, cmd)
                    
                    # Persist to WAL
                    if self.wal:
                        self.wal.append_entry(entry_data.index, entry_data.term, cmd)
                    
                    print(f"[{self.node_id}] Appended entry {entry_data.index}: {cmd}")
                    
                elif existing.term != entry_data.term:
                    self._delete_entries_from(entry_data.index)
                    # Delete from WAL
                    if self.wal:
                        self.wal.delete_entries_from(entry_data.index)
                    
                    cmd = Command.from_dict(entry_data.command)
                    self.log.append(entry_data.term, cmd)
                    
                    # Persist to WAL
                    if self.wal:
                        self.wal.append_entry(entry_data.index, entry_data.term, cmd)
                    
                    print(f"[{self.node_id}] Replaced entry {entry_data.index}: {cmd}")
        
            # Update commit index
            if request.leader_commit > self.commit_index:
                old_commit = self.commit_index
                self.commit_index = min(request.leader_commit, self.log.last_index())
                print(f"[{self.node_id}] Updated commit_index: {old_commit} -> {self.commit_index}")
        
        return AppendEntriesResponse(
            term=self.current_term,
            success=True,
            match_index=self.log.last_index()
        )


    def _delete_entries_from(self, index: int):
        """Delete log entries from index onwards"""
        with self._lock:
            if index <= len(self.log._entries):
                # Delete from index onwards (convert to 0-based)
                self.log._entries = self.log._entries[:index - 1]
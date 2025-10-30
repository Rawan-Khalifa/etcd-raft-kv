import threading
import time
from kvstore import KVStore
from log import Log
from state_machine import StateMachine
from command import Command, CommandType

class Coordinator:
    """
    Coordinates commands, log, and state machine.
    
    This is a simplified version that will eventually become
    a full Raft node. For now, it just:
    - Accepts commands
    - Adds them to the log
    - Commits them immediately (no consensus yet)
    - Applies them to the state machine
    """
    
    def __init__(self):
        """Initialize the coordinator"""
        self.store = KVStore()
        self.log = Log()
        self.state_machine = StateMachine(self.store, self.log)
        
        # For now, we're always in "term 1" and we're the leader
        # This will change when we implement Raft
        self.current_term = 1
        self.is_leader = True
        
        self._lock = threading.RLock()
        
        # Start background thread to apply committed entries
        self._running = True
        self._applier_thread = threading.Thread(target=self._apply_loop, daemon=True)
        self._applier_thread.start()
    
    def propose_command(self, command: Command) -> dict:
        """
        Propose a command to be executed.
        
        In single-node mode: immediately commit and apply.
        With Raft: would send to followers for consensus.
        
        Args:
            command: The command to execute
            
        Returns:
            dict with result information
        """
        with self._lock:
            # Check if we're the leader (always true for now)
            if not self.is_leader:
                return {
                    'success': False,
                    'error': 'Not the leader',
                    'leader': None  # Would return actual leader address with Raft
                }
            
            # Add command to log
            entry = self.log.append(self.current_term, command)
            
            print(f"[Coordinator] Proposed {command} at index {entry.index}")
            
            # In single-node mode, immediately commit
            # With Raft, we'd wait for majority acknowledgment
            self.log.set_commit_index(entry.index)
            
            # Wait for it to be applied (with timeout)
            max_wait = 1.0  # 1 second timeout
            start_time = time.time()
            
            while self.log.get_last_applied() < entry.index:
                if time.time() - start_time > max_wait:
                    return {
                        'success': False,
                        'error': 'Timeout waiting for apply'
                    }
                time.sleep(0.01)  # Small sleep to avoid busy waiting
            
            # Command has been applied!
            return {
                'success': True,
                'index': entry.index,
                'term': entry.term
            }
    
    def get(self, key: str):
        """
        Read a value (doesn't need to go through log).
        
        Args:
            key: The key to read
            
        Returns:
            The value, or None if not found
        """
        return self.state_machine.get(key)
    
    def _apply_loop(self):
        """
        Background thread that continuously applies committed entries.
        
        This runs in a loop checking if there are committed entries
        that haven't been applied yet.
        """
        while self._running:
            try:
                # Check if there are committed entries to apply
                if self.log.get_commit_index() > self.log.get_last_applied():
                    self.state_machine.apply_committed_entries()
                
                # Sleep briefly to avoid busy loop
                time.sleep(0.01)
                
            except Exception as e:
                print(f"[Coordinator] Error in apply loop: {e}")
                time.sleep(0.1)
    
    def shutdown(self):
        """Shutdown the coordinator"""
        print("[Coordinator] Shutting down...")
        self._running = False
        self._applier_thread.join(timeout=1.0)
    
    def get_status(self) -> dict:
        """Get status information (useful for debugging)"""
        return {
            'term': self.current_term,
            'is_leader': self.is_leader,
            'log_size': len(self.log),
            'commit_index': self.log.get_commit_index(),
            'last_applied': self.log.get_last_applied()
        }
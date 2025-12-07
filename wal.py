"""
Write-Ahead Log (WAL) for persistent Raft state
Ensures durability and crash recovery
"""
import json
import os
import threading
from pathlib import Path
from dataclasses import dataclass, asdict
from command import Command, CommandType
from log import LogEntry
from snapshot import SnapshotManager

@dataclass
class RaftPersistentState:
    """Persistent Raft state that must survive crashes"""
    current_term: int = 0
    voted_for: str = None
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

class WriteAheadLog:
    """Write-Ahead Log with snapshot support"""
    
    def __init__(self, node_id, data_dir="./raft_data", snapshot_interval=100):
        self.node_id = node_id
        self.data_dir = Path(data_dir) / node_id
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.log_file = self.data_dir / "log.json"
        self.state_file = self.data_dir / "state.json"
        
        self._lock = threading.Lock()
        
        # Initialize snapshot manager
        self.snapshot_manager = SnapshotManager(node_id, data_dir, snapshot_interval)
        
        print(f"[{self.node_id}] WAL initialized at {self.data_dir}")
    
    def save_persistent_state(self, current_term: int, voted_for: str = None):
        """Save Raft persistent state to disk"""
        with self._lock:
            state = RaftPersistentState(
                current_term=current_term,
                voted_for=voted_for
            )
            
            try:
                with open(self.state_file, 'w') as f:
                    json.dump(state.to_dict(), f)
            except Exception as e:
                print(f"[{self.node_id}] ERROR saving state: {e}")
    
    def load_persistent_state(self):
        """Load Raft persistent state from disk"""
        with self._lock:
            if not self.state_file.exists():
                return RaftPersistentState()
            
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    return RaftPersistentState.from_dict(data)
            except Exception as e:
                print(f"[{self.node_id}] ERROR loading state: {e}")
                return RaftPersistentState()
    
    def append_entry(self, index: int, term: int, command: Command):
        """Append a log entry to disk (write-ahead)"""
        with self._lock:
            try:
                # Load existing log
                entries = self._load_log_entries()
                
                # Append new entry
                entry = {
                    'index': index,
                    'term': term,
                    'command': {
                        'type': command.type.value,
                        'key': command.key,
                        'value': command.value
                    }
                }
                entries.append(entry)
                
                # Write to disk
                with open(self.log_file, 'w') as f:
                    json.dump(entries, f)
                
                return True
            except Exception as e:
                print(f"[{self.node_id}] ERROR appending entry: {e}")
                return False
    
    def delete_entries_from(self, index: int):
        """Delete entries from index onwards (for follower conflicts)"""
        with self._lock:
            try:
                entries = self._load_log_entries()
                
                # Keep entries before index
                entries = [e for e in entries if e['index'] < index]
                
                # Write to disk
                with open(self.log_file, 'w') as f:
                    json.dump(entries, f)
                
                return True
            except Exception as e:
                print(f"[{self.node_id}] ERROR deleting entries: {e}")
                return False
    
    def get_all_entries(self):
        """Load all log entries from disk"""
        with self._lock:
            return self._load_log_entries()
    
    def get_entry(self, index: int):
        """Get a specific log entry by index"""
        with self._lock:
            entries = self._load_log_entries()
            for entry in entries:
                if entry['index'] == index:
                    return entry
            return None
    
    def get_last_entry(self):
        """Get the last log entry"""
        with self._lock:
            entries = self._load_log_entries()
            return entries[-1] if entries else None
    
    def _load_log_entries(self):
        """Internal: Load all entries from log file"""
        if not self.log_file.exists():
            return []
        
        try:
            with open(self.log_file, 'r') as f:
                return json.load(f)
        except:
            return []
    
    def save_snapshot(self, snapshot_data: dict):
        """Save a state machine snapshot (for log compaction)"""
        with self._lock:
            try:
                with open(self.snapshot_file, 'w') as f:
                    json.dump(snapshot_data, f)
                return True
            except Exception as e:
                print(f"[{self.node_id}] ERROR saving snapshot: {e}")
                return False
    
    def load_snapshot(self):
        """Load state machine snapshot"""
        with self._lock:
            if not self.snapshot_file.exists():
                return {}
            
            try:
                with open(self.snapshot_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"[{self.node_id}] ERROR loading snapshot: {e}")
                return {}
    
    def cleanup(self):
        """Clean up all persistent files"""
        try:
            if self.log_file.exists():
                self.log_file.unlink()
            if self.state_file.exists():
                self.state_file.unlink()
            if self.snapshot_file.exists():
                self.snapshot_file.unlink()
        except Exception as e:
            print(f"[{self.node_id}] ERROR during cleanup: {e}")
    
    def get_size(self):
        """Get total size of persistent data"""
        total = 0
        for file in [self.log_file, self.state_file, self.snapshot_file]:
            if file.exists():
                total += file.stat().st_size
        return total
    
    def get_snapshot_info(self):
        """Get snapshot information"""
        return self.snapshot_manager.get_snapshot_info()
    
    def create_snapshot(self, log_index: int, log_term: int, kv_store_state: dict):
        """Create a snapshot and return whether log should be truncated"""
        return self.snapshot_manager.create_snapshot(log_index, log_term, kv_store_state)
    
    def load_snapshot(self):
        """Load KV store state from latest snapshot"""
        return self.snapshot_manager.load_latest_snapshot()
    
    def truncate_log(self, index: int) -> bool:
        """Delete log entries up to (not including) the given index"""
        with self._lock:
            try:
                entries = self._load_log_entries()
                
                # Keep only entries at or after index
                entries = [e for e in entries if e['index'] >= index]
                
                # Write back
                with open(self.log_file, 'w') as f:
                    json.dump(entries, f)
                
                print(f"[{self.node_id}] Truncated log: keeping entries from index {index}")
                return True
                
            except Exception as e:
                print(f"[{self.node_id}] ERROR truncating log: {e}")
                return False
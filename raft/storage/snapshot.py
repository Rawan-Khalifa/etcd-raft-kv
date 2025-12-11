"""
Snapshot management for Raft log compaction.
Periodically saves state machine state to disk and discards old log entries.
"""
import json
import time
import threading
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Dict, Any

@dataclass
class SnapshotMetadata:
    """Metadata about a snapshot"""
    snapshot_index: int      # Log index of last included entry
    snapshot_term: int       # Term of last included entry
    timestamp: float         # When snapshot was created
    kv_store_size: int      # Number of key-value pairs
    
    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)

class SnapshotManager:
    """
    Manages snapshots for log compaction.
    
    Strategy:
    - Every N operations, create a snapshot
    - Save complete KV store state to disk
    - Delete log entries before snapshot
    - New followers can install snapshot instead of replaying log
    """
    
    def __init__(self, node_id, data_dir="./raft_data", snapshot_interval=100):
        """
        Initialize snapshot manager.
        
        Args:
            node_id: Node identifier
            data_dir: Directory for persistent data
            snapshot_interval: Create snapshot every N log entries
        """
        self.node_id = node_id
        self.data_dir = Path(data_dir) / node_id
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.snapshot_dir = self.data_dir / "snapshots"
        self.snapshot_dir.mkdir(exist_ok=True)
        
        self.snapshot_interval = snapshot_interval
        self.last_snapshot_index = 0
        self.last_snapshot_term = 0
        
        self._lock = threading.Lock()
        
        # Load metadata of latest snapshot
        self._load_latest_snapshot_metadata()
        
        print(f"[{self.node_id}] Snapshot manager initialized")
        print(f"[{self.node_id}]   Last snapshot at index {self.last_snapshot_index}, term {self.last_snapshot_term}")
        print(f"[{self.node_id}]   Snapshot interval: {snapshot_interval} entries")
    
    def should_snapshot(self, current_log_size: int) -> bool:
        """Check if it's time to create a snapshot"""
        entries_since_snapshot = current_log_size - self.last_snapshot_index
        return entries_since_snapshot >= self.snapshot_interval
    
    def create_snapshot(self, log_index: int, log_term: int, kv_store_state: Dict[str, Any]) -> bool:
        """
        Create a snapshot at the given log index.
        
        Args:
            log_index: Index of last entry included in snapshot
            log_term: Term of last entry included in snapshot
            kv_store_state: Complete state of KV store
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            try:
                # Create snapshot directory
                timestamp = int(time.time() * 1000)  # Millisecond precision
                snapshot_name = f"snapshot_{log_index}_{timestamp}"
                snapshot_path = self.snapshot_dir / snapshot_name
                snapshot_path.mkdir(exist_ok=True)
                
                # Save KV store state
                store_file = snapshot_path / "store.json"
                with open(store_file, 'w') as f:
                    json.dump(kv_store_state, f)
                
                # Save metadata
                metadata = SnapshotMetadata(
                    snapshot_index=log_index,
                    snapshot_term=log_term,
                    timestamp=time.time(),
                    kv_store_size=len(kv_store_state)
                )
                
                metadata_file = snapshot_path / "metadata.json"
                with open(metadata_file, 'w') as f:
                    json.dump(metadata.to_dict(), f)
                
                # Update last snapshot info
                self.last_snapshot_index = log_index
                self.last_snapshot_term = log_term
                
                print(f"[{self.node_id}] Created snapshot at index {log_index}")
                print(f"[{self.node_id}]   Stored {len(kv_store_state)} key-value pairs")
                print(f"[{self.node_id}]   Snapshot size: {store_file.stat().st_size} bytes")
                
                return True
                
            except Exception as e:
                print(f"[{self.node_id}] ERROR creating snapshot: {e}")
                return False
    
    def load_latest_snapshot(self) -> Dict[str, Any]:
        """Load the latest snapshot state"""
        with self._lock:
            if self.last_snapshot_index == 0:
                return {}
            
            try:
                # Find the snapshot directory
                snapshots = sorted(self.snapshot_dir.iterdir(), reverse=True)
                if not snapshots:
                    return {}
                
                latest = snapshots[0]
                store_file = latest / "store.json"
                
                if store_file.exists():
                    with open(store_file, 'r') as f:
                        return json.load(f)
                
                return {}
                
            except Exception as e:
                print(f"[{self.node_id}] ERROR loading snapshot: {e}")
                return {}
    
    def _load_latest_snapshot_metadata(self):
        """Load metadata from the latest snapshot"""
        try:
            if not self.snapshot_dir.exists():
                return
            
            snapshots = sorted(self.snapshot_dir.iterdir(), reverse=True)
            if not snapshots:
                return
            
            latest = snapshots[0]
            metadata_file = latest / "metadata.json"
            
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    data = json.load(f)
                    metadata = SnapshotMetadata.from_dict(data)
                    self.last_snapshot_index = metadata.snapshot_index
                    self.last_snapshot_term = metadata.snapshot_term
        except Exception as e:
            print(f"[{self.node_id}] ERROR loading snapshot metadata: {e}")
    
    def get_snapshot_info(self):
        """Get information about the latest snapshot"""
        with self._lock:
            return {
                'last_snapshot_index': self.last_snapshot_index,
                'last_snapshot_term': self.last_snapshot_term,
                'snapshot_interval': self.snapshot_interval
            }
    
    def cleanup_old_snapshots(self, keep_count: int = 3):
        """
        Delete old snapshots, keeping only the most recent ones.
        
        Args:
            keep_count: Number of snapshots to keep
        """
        with self._lock:
            try:
                if not self.snapshot_dir.exists():
                    return
                
                snapshots = sorted(self.snapshot_dir.iterdir(), reverse=True)
                
                # Delete all but the keep_count most recent
                for snapshot in snapshots[keep_count:]:
                    import shutil
                    shutil.rmtree(snapshot)
                    print(f"[{self.node_id}] Deleted old snapshot: {snapshot.name}")
                    
            except Exception as e:
                print(f"[{self.node_id}] ERROR cleaning up snapshots: {e}")
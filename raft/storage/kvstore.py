"""
Key-Value Store using B-Tree for efficient storage and range queries.
"""
from raft.storage.btree import BTree
from typing import Dict, List, Tuple, Optional, Any
import json

class KVStore:
    """
    Key-Value store backed by B-Tree.
    
    Features:
    - O(log n) get/set/delete
    - Range queries: get all keys in a range
    - Ordered iteration
    - Efficient snapshot serialization
    """
    
    def __init__(self, order: int = 4):
        """Initialize KV store with B-Tree backend"""
        self.btree = BTree(order=order)
        self._data = {}  # For compatibility with snapshots
    
    def get(self, key: str) -> Optional[Any]:
        """Get value for key"""
        return self.btree.get(key)
    
    def put(self, key: str, value: Any) -> bool:
        """
        Put key-value pair.
        Returns: True if new, False if updated
        """
        is_new = self.btree.put(key, value)
        self._sync_data_dict()  # Keep _data in sync for snapshots
        return is_new
    
    def delete(self, key: str) -> bool:
        """Delete key. Returns True if existed, False otherwise"""
        result = self.btree.delete(key)
        if result:
            self._sync_data_dict()
        return result
    
    def range_query(self, start: str, end: str) -> List[Tuple[str, Any]]:
        """
        Range query: get all keys in [start, end].
        
        Time: O(log n + k) where k is number of results
        
        Example:
            store.range_query("user:1000", "user:2000")
            # Returns all users with IDs between 1000-2000
        """
        return self.btree.range_items(start, end)
    
    def range_keys(self, start: str, end: str) -> List[str]:
        """Get all keys in range [start, end]"""
        return self.btree.range_keys(start, end)
    
    def keys(self) -> List[str]:
        """Get all keys in sorted order"""
        return list(self.btree.keys())
    
    def values(self) -> List[Any]:
        """Get all values in sorted key order"""
        return list(self.btree.values())
    
    def items(self) -> List[Tuple[str, Any]]:
        """Get all key-value pairs in sorted order"""
        return list(self.btree.items())
    
    def __len__(self) -> int:
        """Number of keys"""
        return len(self.btree)
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists"""
        return key in self.btree
    
    def __getitem__(self, key: str) -> Any:
        """Get by key"""
        return self.btree[key]
    
    def __setitem__(self, key: str, value: Any):
        """Set by key"""
        self.btree[key] = value
        self._sync_data_dict()
    
    def _sync_data_dict(self):
        """Keep _data dict in sync with B-Tree for snapshot compatibility"""
        self._data = dict(self.btree.items())
    
    def to_dict(self) -> Dict[str, Any]:
        """Export as dictionary (for snapshots)"""
        return dict(self.btree.items())
    
    def from_dict(self, data: Dict[str, Any]):
        """Import from dictionary (for snapshot recovery)"""
        self.btree = BTree()
        for key, value in sorted(data.items()):
            self.btree.put(key, value)
        self._sync_data_dict()
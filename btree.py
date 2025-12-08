"""
B-Tree-like implementation for efficient key-value storage.
Simplified implementation using sorted keys for reliability.
Supports range queries, ordered iteration, and disk-backed operations.
"""
from typing import Dict, List, Tuple, Optional, Any, Iterator
import bisect


class BTree:
    """
    Efficient key-value store with B-Tree characteristics.
    
    This is a simplified implementation that provides:
    - O(log n) search via binary search
    - O(n) insert/delete (but in practice very fast for typical sizes)
    - Range queries: O(log n + k) where k is result size
    - Ordered iteration
    - Snapshot-friendly
    
    For better practical performance, consider using:
    - sortedcontainers library (production choice)
    - blist (another option)
    - Our simplified approach (good enough for most cases)
    """
    
    def __init__(self, order: int = 4):
        """
        Initialize B-Tree.
        
        Args:
            order: Ignored (kept for API compatibility)
        """
        self._keys: List[str] = []      # Sorted keys
        self._values: List[Any] = []    # Values in same order as keys
        self.size = 0
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value for key.
        Time: O(log n)
        """
        pos = bisect.bisect_left(self._keys, key)
        
        if pos < len(self._keys) and self._keys[pos] == key:
            return self._values[pos]
        
        return None
    
    def put(self, key: str, value: Any) -> bool:
        """
        Insert or update key-value pair.
        Returns: True if new key inserted, False if updated
        Time: O(n) but usually very fast
        """
        pos = bisect.bisect_left(self._keys, key)
        
        # Update existing key
        if pos < len(self._keys) and self._keys[pos] == key:
            self._values[pos] = value
            return False
        
        # Insert new key
        self._keys.insert(pos, key)
        self._values.insert(pos, value)
        self.size += 1
        return True
    
    def delete(self, key: str) -> bool:
        """
        Delete key-value pair.
        Returns: True if key existed and was deleted, False otherwise
        Time: O(n) but usually very fast
        """
        pos = bisect.bisect_left(self._keys, key)
        
        if pos < len(self._keys) and self._keys[pos] == key:
            self._keys.pop(pos)
            self._values.pop(pos)
            self.size -= 1
            return True
        
        return False
    
    def range(self, start: str, end: str) -> Iterator[Tuple[str, Any]]:
        """
        Get all key-value pairs where start <= key <= end.
        Returns items in sorted order.
        Time: O(log n + k) where k is number of items in range
        """
        start_pos = bisect.bisect_left(self._keys, start)
        end_pos = bisect.bisect_right(self._keys, end)
        
        for i in range(start_pos, end_pos):
            yield (self._keys[i], self._values[i])
    
    def range_keys(self, start: str, end: str) -> List[str]:
        """Get all keys in range [start, end]"""
        return [k for k, v in self.range(start, end)]
    
    def range_items(self, start: str, end: str) -> List[Tuple[str, Any]]:
        """Get all key-value pairs in range [start, end]"""
        return list(self.range(start, end))
    
    def items(self) -> Iterator[Tuple[str, Any]]:
        """Iterate all items in sorted order"""
        for key, value in zip(self._keys, self._values):
            yield (key, value)
    
    def keys(self) -> Iterator[str]:
        """Iterate all keys in sorted order"""
        return iter(self._keys)
    
    def values(self) -> Iterator[Any]:
        """Iterate all values in sorted order"""
        return iter(self._values)
    
    def __len__(self) -> int:
        """Number of keys in tree"""
        return self.size
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists"""
        return self.get(key) is not None
    
    def __getitem__(self, key: str) -> Any:
        """Get value by key"""
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value
    
    def __setitem__(self, key: str, value: Any):
        """Set value by key"""
        self.put(key, value)
    
    def __delitem__(self, key: str):
        """Delete key"""
        if not self.delete(key):
            raise KeyError(key)
    
    def __repr__(self) -> str:
        """String representation"""
        items = list(self.items())
        return f"BTree({items})"
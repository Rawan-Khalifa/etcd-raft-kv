"""
B-Tree implementation for efficient key-value storage.
Supports range queries, ordered iteration, and disk-backed operations.
"""
from typing import Dict, List, Tuple, Optional, Any, Iterator
from dataclasses import dataclass
import bisect

@dataclass
class BTreeNode:
    """A single node in the B-Tree"""
    keys: List[str]           # Sorted keys in this node
    values: List[Any]         # Values corresponding to keys
    children: List['BTreeNode']  # Child nodes (None if leaf)
    is_leaf: bool = True
    
    def __init__(self, is_leaf: bool = True):
        self.keys = []
        self.values = []
        self.children = [] if not is_leaf else []
        self.is_leaf = is_leaf


class BTree:
    """
    B-Tree data structure for efficient key-value storage.
    
    Properties:
    - Order M: each node has at most M children
    - All leaves at same depth
    - O(log n) search, insert, delete
    - Supports range queries and ordered iteration
    """
    
    def __init__(self, order: int = 4):
        """
        Initialize B-Tree.
        
        Args:
            order: Maximum number of children per node (default 4)
        """
        self.order = order
        self.root = BTreeNode(is_leaf=True)
        self.size = 0
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value for key.
        
        Time: O(log n)
        """
        node = self.root
        
        while True:
            # Find position of key in node
            pos = bisect.bisect_left(node.keys, key)
            
            # Key found
            if pos < len(node.keys) and node.keys[pos] == key:
                return node.values[pos]
            
            # Key not in this node
            if node.is_leaf:
                return None
            
            # Go to next level
            node = node.children[pos]
    
    def put(self, key: str, value: Any) -> bool:
        """
        Insert or update key-value pair.
        
        Returns: True if new key inserted, False if updated
        
        Time: O(log n)
        """
        # Check if key exists
        is_new_key = self.get(key) is None
        
        if self.root.keys.__len__() >= self.order - 1:
            # Root is full, need to split
            old_root = self.root
            self.root = BTreeNode(is_leaf=False)
            self.root.children.append(old_root)
            self._split_child(self.root, 0)
        
        self._insert_non_full(self.root, key, value)
        
        if is_new_key:
            self.size += 1
        
        return is_new_key
    
    def delete(self, key: str) -> bool:
        """
        Delete key-value pair.
        
        Returns: True if key existed and was deleted, False otherwise
        
        Time: O(log n)
        """
        if self.get(key) is None:
            return False
        
        self._delete_from_node(self.root, key)
        
        # If root is empty, make its only child the new root
        if len(self.root.keys) == 0:
            if not self.root.is_leaf and len(self.root.children) > 0:
                self.root = self.root.children[0]
        
        self.size -= 1
        return True
    
    def range(self, start: str, end: str) -> Iterator[Tuple[str, Any]]:
        """
        Get all key-value pairs where start <= key <= end.
        
        Returns items in sorted order.
        
        Time: O(log n + k) where k is number of items in range
        """
        yield from self._range_search(self.root, start, end)
    
    def range_keys(self, start: str, end: str) -> List[str]:
        """Get all keys in range [start, end]"""
        return [k for k, v in self.range(start, end)]
    
    def range_items(self, start: str, end: str) -> List[Tuple[str, Any]]:
        """Get all key-value pairs in range [start, end]"""
        return list(self.range(start, end))
    
    def items(self) -> Iterator[Tuple[str, Any]]:
        """Iterate all items in sorted order"""
        yield from self._iterate_node(self.root)
    
    def keys(self) -> Iterator[str]:
        """Iterate all keys in sorted order"""
        for key, _ in self.items():
            yield key
    
    def values(self) -> Iterator[Any]:
        """Iterate all values in sorted order"""
        for _, value in self.items():
            yield value
    
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
    
    # Internal helper methods
    
    def _insert_non_full(self, node: BTreeNode, key: str, value: Any):
        """Insert into node that is not full"""
        pos = bisect.bisect_left(node.keys, key)
        
        # Update existing key
        if pos < len(node.keys) and node.keys[pos] == key:
            node.values[pos] = value
            return
        
        if node.is_leaf:
            # Insert in leaf node
            node.keys.insert(pos, key)
            node.values.insert(pos, value)
        else:
            # Go to appropriate child
            child = node.children[pos]
            
            if len(child.keys) >= self.order - 1:
                # Child is full, split it
                self._split_child(node, pos)
                
                # After split, decide which child to go to
                if key > node.keys[pos]:
                    pos += 1
                elif key == node.keys[pos]:
                    node.values[pos] = value
                    return
            
            self._insert_non_full(node.children[pos], key, value)
    
    def _split_child(self, parent: BTreeNode, pos: int):
        """Split full child at position pos in parent"""
        full_child = parent.children[pos]
        mid_idx = (len(full_child.keys) - 1) // 2
        
        # Create new node
        new_node = BTreeNode(is_leaf=full_child.is_leaf)
        
        # Move second half of keys/values to new node
        new_node.keys = full_child.keys[mid_idx + 1:]
        new_node.values = full_child.values[mid_idx + 1:]
        
        if not full_child.is_leaf:
            new_node.children = full_child.children[mid_idx + 1:]
            full_child.children = full_child.children[:mid_idx + 1]
        
        # Keep first half in full_child
        full_child.keys = full_child.keys[:mid_idx]
        full_child.values = full_child.values[:mid_idx]
        
        # Move middle key up to parent
        parent.keys.insert(pos, new_node.keys[0] if new_node.keys else full_child.keys[-1] if full_child.keys else "")
        parent.values.insert(pos, new_node.values[0] if new_node.values else full_child.values[-1] if full_child.values else None)
        parent.children.insert(pos + 1, new_node)
    
    def _delete_from_node(self, node: BTreeNode, key: str):
        """Delete key from node"""
        pos = bisect.bisect_left(node.keys, key)
        
        if pos < len(node.keys) and node.keys[pos] == key:
            if node.is_leaf:
                # Delete from leaf
                node.keys.pop(pos)
                node.values.pop(pos)
            else:
                # Delete from internal node
                self._delete_from_internal(node, pos, key)
        elif not node.is_leaf:
            # Key might be in child
            is_in_subtree = (pos == len(node.keys))
            
            if len(node.children[pos].keys) < self.order - 1:
                # Child has minimum keys, need to fix before recurse
                self._fill_child(node, pos)
            
            if is_in_subtree and pos > len(node.keys):
                self._delete_from_node(node.children[pos - 1], key)
            else:
                self._delete_from_node(node.children[pos], key)
    
    def _delete_from_internal(self, node: BTreeNode, pos: int, key: str):
        """Delete key from internal node"""
        key_to_delete = node.keys[pos]
        
        if len(node.children[pos].keys) >= self.order:
            # Get predecessor
            predecessor = node.children[pos]
            while not predecessor.is_leaf:
                predecessor = predecessor.children[-1]
            pred_key = predecessor.keys[-1]
            pred_value = predecessor.values[-1]
            
            self._delete_from_node(node.children[pos], pred_key)
            node.keys[pos] = pred_key
            node.values[pos] = pred_value
        else:
            # Merge with right sibling
            self._merge(node, pos)
            self._delete_from_node(node.children[pos], key)
    
    def _fill_child(self, node: BTreeNode, pos: int):
        """Fill child at pos if it has minimum keys"""
        if pos != 0 and len(node.children[pos - 1].keys) >= self.order:
            # Borrow from left sibling
            self._borrow_from_left(node, pos)
        elif pos != len(node.children) - 1 and len(node.children[pos + 1].keys) >= self.order:
            # Borrow from right sibling
            self._borrow_from_right(node, pos)
        else:
            # Merge with sibling
            if pos != len(node.children) - 1:
                self._merge(node, pos)
            else:
                self._merge(node, pos - 1)
    
    def _borrow_from_left(self, node: BTreeNode, child_pos: int):
        """Borrow key from left sibling"""
        child = node.children[child_pos]
        left_sibling = node.children[child_pos - 1]
        
        child.keys.insert(0, node.keys[child_pos - 1])
        child.values.insert(0, node.values[child_pos - 1])
        
        node.keys[child_pos - 1] = left_sibling.keys.pop()
        node.values[child_pos - 1] = left_sibling.values.pop()
        
        if not child.is_leaf:
            child.children.insert(0, left_sibling.children.pop())
    
    def _borrow_from_right(self, node: BTreeNode, child_pos: int):
        """Borrow key from right sibling"""
        child = node.children[child_pos]
        right_sibling = node.children[child_pos + 1]
        
        child.keys.append(node.keys[child_pos])
        child.values.append(node.values[child_pos])
        
        node.keys[child_pos] = right_sibling.keys.pop(0)
        node.values[child_pos] = right_sibling.values.pop(0)
        
        if not child.is_leaf:
            child.children.append(right_sibling.children.pop(0))
    
    def _merge(self, node: BTreeNode, pos: int):
        """Merge child at pos with its right sibling"""
        child = node.children[pos]
        right_sibling = node.children[pos + 1]
        
        # Move key from parent to child
        child.keys.append(node.keys[pos])
        child.values.append(node.values[pos])
        
        # Copy keys/values from right sibling
        child.keys.extend(right_sibling.keys)
        child.values.extend(right_sibling.values)
        
        if not child.is_leaf:
            child.children.extend(right_sibling.children)
        
        # Remove key from parent
        node.keys.pop(pos)
        node.values.pop(pos)
        node.children.pop(pos + 1)
    
    def _range_search(self, node: BTreeNode, start: str, end: str) -> Iterator[Tuple[str, Any]]:
        """Search for all keys in range [start, end]"""
        for i, key in enumerate(node.keys):
            if key >= start:
                if not node.is_leaf:
                    yield from self._range_search(node.children[i], start, end)
                
                if key <= end:
                    yield (key, node.values[i])
                else:
                    return
        
        if not node.is_leaf and len(node.children) > len(node.keys):
            yield from self._range_search(node.children[-1], start, end)
    
    def _iterate_node(self, node: BTreeNode) -> Iterator[Tuple[str, Any]]:
        """In-order traversal of tree"""
        for i, key in enumerate(node.keys):
            if not node.is_leaf:
                yield from self._iterate_node(node.children[i])
            yield (key, node.values[i])
        
        if not node.is_leaf and len(node.children) > len(node.keys):
            yield from self._iterate_node(node.children[-1])
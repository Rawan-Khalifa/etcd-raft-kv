import threading

class KVStore:
    """
    A simple in-memory key-value store.
    Thread-safe using a lock.
    """
    
    def __init__(self):
        """Initialize an empty store"""
        self._data = {}  # Our key-value storage (using dict)
        self._lock = threading.RLock()  # Reentrant lock for thread safety
    
    def get(self, key):
        """
        Get a value by key.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if key exists, None otherwise
        """
        with self._lock:  # Acquire lock, auto-releases when block exits
            return self._data.get(key)  # dict.get() returns None if key missing
    
    def put(self, key, value):
        """
        Store a key-value pair.
        
        Args:
            key: The key to store
            value: The value to store
        """
        with self._lock:
            self._data[key] = value
    
    def delete(self, key):
        """
        Delete a key-value pair.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key existed and was deleted, False otherwise
        """
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False
    
    def __len__(self):
        """Return the number of key-value pairs"""
        with self._lock:
            return len(self._data)
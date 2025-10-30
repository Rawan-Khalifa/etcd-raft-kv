import threading
from typing import List, Optional
from command import Command

class LogEntry:
    """
    A single entry in the Raft log.
    Contains a command and metadata.
    """
    
    def __init__(self, index: int, term: int, command: Command):
        """
        Create a log entry.
        
        Args:
            index: Position in the log (1-indexed)
            term: Raft term when entry was created
            command: The command to execute
        """
        self.index = index
        self.term = term
        self.command = command
    
    def to_dict(self):
        """Serialize to dictionary"""
        return {
            'index': self.index,
            'term': self.term,
            'command': self.command.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        """Deserialize from dictionary"""
        return cls(
            index=data['index'],
            term=data['term'],
            command=Command.from_dict(data['command'])
        )
    
    def __repr__(self):
        return f"LogEntry(index={self.index}, term={self.term}, {self.command})"


class Log:
    """
    The replicated log that stores all commands.
    This is the heart of Raft - ensuring all nodes have the same log.
    """
    
    def __init__(self):
        """Initialize an empty log"""
        self._entries: List[LogEntry] = []
        self._lock = threading.RLock()
        
        # Track what's been applied to the state machine
        self._last_applied = 0  # Index of last entry applied to state machine
        self._commit_index = 0  # Index of last committed entry
    
    def append(self, term: int, command: Command) -> LogEntry:
        """
        Append a new command to the log.
        
        Args:
            term: Current Raft term
            command: The command to append
            
        Returns:
            The created LogEntry
        """
        with self._lock:
            # Log indices are 1-based (index 0 is reserved)
            next_index = len(self._entries) + 1
            entry = LogEntry(next_index, term, command)
            self._entries.append(entry)
            return entry
    
    def get(self, index: int) -> Optional[LogEntry]:
        """
        Get a log entry by index.
        
        Args:
            index: The log index (1-based)
            
        Returns:
            The LogEntry or None if index is out of bounds
        """
        with self._lock:
            if index < 1 or index > len(self._entries):
                return None
            return self._entries[index - 1]  # Convert to 0-based array index
    
    def get_entries_from(self, start_index: int) -> List[LogEntry]:
        """
        Get all entries starting from an index.
        Used for replicating log to followers.
        
        Args:
            start_index: Starting index (inclusive, 1-based)
            
        Returns:
            List of LogEntry objects
        """
        with self._lock:
            if start_index < 1:
                start_index = 1
            
            # Convert to 0-based and slice
            array_index = start_index - 1
            return self._entries[array_index:].copy()
    
    def last_index(self) -> int:
        """Get the index of the last log entry"""
        with self._lock:
            return len(self._entries)
    
    def last_term(self) -> int:
        """Get the term of the last log entry"""
        with self._lock:
            if not self._entries:
                return 0
            return self._entries[-1].term
    
    def set_commit_index(self, index: int):
        """Update the commit index (entries up to this point are committed)"""
        with self._lock:
            self._commit_index = min(index, len(self._entries))
    
    def get_commit_index(self) -> int:
        """Get the current commit index"""
        with self._lock:
            return self._commit_index
    
    def get_last_applied(self) -> int:
        """Get the index of the last applied entry"""
        with self._lock:
            return self._last_applied
    
    def set_last_applied(self, index: int):
        """Update the last applied index"""
        with self._lock:
            self._last_applied = index
    
    def get_uncommitted_entries(self) -> List[LogEntry]:
        """Get entries that are committed but not yet applied"""
        with self._lock:
            start = self._last_applied
            end = self._commit_index
            
            if start >= end:
                return []
            
            # Return entries from (last_applied + 1) to commit_index (inclusive)
            return self._entries[start:end].copy()
    
    def __len__(self):
        """Return the number of entries in the log"""
        with self._lock:
            return len(self._entries)
    
    def __repr__(self):
        with self._lock:
            return f"Log(entries={len(self._entries)}, committed={self._commit_index}, applied={self._last_applied})"
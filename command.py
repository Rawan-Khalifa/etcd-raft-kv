from enum import Enum
from typing import Any, Optional
import json

class CommandType(Enum):
    """Types of commands our system supports"""
    PUT = "PUT"
    DELETE = "DELETE"
    # GET is not a command because it doesn't modify state
    # (reads don't need to go through Raft)

class Command:
    """
    Represents a state machine command.
    This is what gets replicated through Raft.
    """
    
    def __init__(self, command_type: CommandType, key: str, value: Optional[str] = None):
        """
        Create a new command.
        
        Args:
            command_type: The type of operation (PUT or DELETE)
            key: The key to operate on
            value: The value (only for PUT operations)
        """
        self.type = command_type
        self.key = key
        self.value = value
        
        # Validate
        if command_type == CommandType.PUT and value is None:
            raise ValueError("PUT command requires a value")
    
    def to_dict(self):
        """Convert command to dictionary (for serialization)"""
        return {
            'type': self.type.value,
            'key': self.key,
            'value': self.value
        }
    
    def to_json(self):
        """Serialize command to JSON string"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create command from dictionary"""
        command_type = CommandType(data['type'])
        return cls(
            command_type=command_type,
            key=data['key'],
            value=data.get('value')
        )
    
    @classmethod
    def from_json(cls, json_str: str):
        """Deserialize command from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    def __repr__(self):
        if self.type == CommandType.PUT:
            return f"Command(PUT {self.key}={self.value})"
        else:
            return f"Command(DELETE {self.key})"
    
    def __eq__(self, other):
        """Compare commands for equality"""
        if not isinstance(other, Command):
            return False
        return (self.type == other.type and 
                self.key == other.key and 
                self.value == other.value)
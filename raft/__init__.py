"""
Raft Consensus Library

A production-ready implementation of the Raft consensus algorithm with:
- Leader election and log replication
- Persistent storage with WAL and snapshots
- gRPC and HTTP transport layers
- Lease-based reads for performance
- Dynamic cluster membership
- Prometheus metrics
"""

from .core.node import RaftNode, NodeState
from .core.command import Command, CommandType
from .storage.kvstore import KVStore

__version__ = "1.0.0"
__all__ = ["RaftNode", "NodeState", "Command", "CommandType", "KVStore"]

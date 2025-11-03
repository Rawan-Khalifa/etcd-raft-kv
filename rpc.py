from dataclasses import dataclass
from typing import List, Optional
from command import Command

@dataclass
class RequestVoteRequest:
    """
    RequestVote RPC request.
    
    Sent by candidates during elections to gather votes.
    """
    term: int                    # Candidate's term
    candidate_id: str           # Candidate requesting vote
    last_log_index: int         # Index of candidate's last log entry
    last_log_term: int          # Term of candidate's last log entry
    
    def to_dict(self):
        return {
            'term': self.term,
            'candidate_id': self.candidate_id,
            'last_log_index': self.last_log_index,
            'last_log_term': self.last_log_term
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class RequestVoteResponse:
    """
    RequestVote RPC response.
    
    Sent by nodes in response to vote requests.
    """
    term: int           # Current term, for candidate to update itself
    vote_granted: bool  # True if candidate received vote
    
    def to_dict(self):
        return {
            'term': self.term,
            'vote_granted': self.vote_granted
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class LogEntryData:
    """Serializable version of LogEntry for RPC"""
    index: int
    term: int
    command: dict  # Command serialized as dict
    
    def to_dict(self):
        return {
            'index': self.index,
            'term': self.term,
            'command': self.command
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class AppendEntriesRequest:
    """
    AppendEntries RPC request.
    
    Sent by leader to replicate log entries and/or as heartbeat.
    """
    term: int                           # Leader's term
    leader_id: str                      # So follower can redirect clients
    prev_log_index: int                 # Index of log entry immediately preceding new ones
    prev_log_term: int                  # Term of prev_log_index entry
    entries: List[LogEntryData]         # Log entries to store (empty for heartbeat)
    leader_commit: int                  # Leader's commit_index
    
    def to_dict(self):
        return {
            'term': self.term,
            'leader_id': self.leader_id,
            'prev_log_index': self.prev_log_index,
            'prev_log_term': self.prev_log_term,
            'entries': [e.to_dict() for e in self.entries],
            'leader_commit': self.leader_commit
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            term=data['term'],
            leader_id=data['leader_id'],
            prev_log_index=data['prev_log_index'],
            prev_log_term=data['prev_log_term'],
            entries=[LogEntryData.from_dict(e) for e in data['entries']],
            leader_commit=data['leader_commit']
        )


@dataclass
class AppendEntriesResponse:
    """
    AppendEntries RPC response.
    """
    term: int       # Current term, for leader to update itself
    success: bool   # True if follower contained entry matching prev_log_index and prev_log_term
    match_index: int  # For leader to update match_index (optimization)
    
    def to_dict(self):
        return {
            'term': self.term,
            'success': self.success,
            'match_index': self.match_index
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)
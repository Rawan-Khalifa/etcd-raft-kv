import requests
import json
from typing import Optional
from rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
)

class RaftRPCClient:
    """
    Client for making RPC calls to other Raft nodes.
    """
    
    def __init__(self, timeout: float = 0.5):
        """
        Initialize RPC client.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
    
    def request_vote(self, address: str, request: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send RequestVote RPC to another node"""
        try:
            url = f"{address}/raft/request_vote"
            response = requests.post(
                url,
                json=request.to_dict(),
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return RequestVoteResponse.from_dict(response.json())
            else:
                print(f"[RPC] RequestVote to {address} failed with status {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            # Timeout - expected if node is slow or down
            return None
        except requests.exceptions.ConnectionError as e:
            # Can't connect - node might be down or address wrong
            print(f"[RPC] Cannot connect to {address}: {e}")
            return None
        except Exception as e:
            print(f"[RPC] RequestVote error: {e}")
            return None

    def append_entries(self, address: str, request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries RPC to another node"""
        try:
            url = f"{address}/raft/append_entries"
            response = requests.post(
                url,
                json=request.to_dict(),
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return AppendEntriesResponse.from_dict(response.json())
            else:
                print(f"[RPC] AppendEntries to {address} failed with status {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            # Timeout - log occasionally
            return None
        except requests.exceptions.ConnectionError as e:
            # Can't connect - THIS IS THE LIKELY CULPRIT
            print(f"[RPC] Cannot connect to {address} for AppendEntries: {e}")
            return None
        except Exception as e:
            print(f"[RPC] AppendEntries error to {address}: {e}")
            return None
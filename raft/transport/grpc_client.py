"""
gRPC client for Raft RPC communication.

Replaces HTTP/JSON requests with binary gRPC for better performance.
"""
import logging
import grpc
from typing import Optional
from raft.proto.generated import raft_pb2, raft_pb2_grpc
from raft.transport.rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    LogEntryData
)

logger = logging.getLogger(__name__)

class RaftGRPCClient:
    """Client for making gRPC RPC calls to other Raft nodes"""
    
    def __init__(self, timeout: float = 0.5):
        """
        Initialize gRPC client.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self._channels = {}  # Cache channels per address
        self._stubs = {}     # Cache stubs per address
    
    def _get_channel(self, address: str) -> Optional[grpc.Channel]:
        """Get or create gRPC channel to address"""
        try:
            if address not in self._channels:
                # Remove http:// or https:// prefix if present
                target = address.replace('http://', '').replace('https://', '')
                self._channels[address] = grpc.aio.secure_channel(
                    target, grpc.ssl_channel_credentials()
                ) if address.startswith('https://') else grpc.aio.insecure_channel(target)
            return self._channels[address]
        except Exception as e:
            logger.error(f"Failed to create channel to {address}: {e}")
            return None
    
    def request_vote(self, address: str, request: RequestVoteRequest) -> Optional[RequestVoteResponse]:
        """Send RequestVote RPC to another node via gRPC"""
        try:
            # Convert to proto
            proto_request = raft_pb2.RequestVoteRequest(
                term=request.term,
                candidate_id=request.candidate_id,
                last_log_index=request.last_log_index,
                last_log_term=request.last_log_term
            )
            
            # Create stub if needed
            target = address.replace('http://', '').replace('https://', '')
            try:
                channel = grpc.insecure_channel(
                    target,
                    options=[
                        ('grpc.keepalive_time_ms', 10000),
                        ('grpc.keepalive_timeout_ms', 5000),
                    ]
                )
                stub = raft_pb2_grpc.RaftRPCStub(channel)
                
                # Make call with timeout
                proto_response = stub.RequestVote(
                    proto_request,
                    timeout=self.timeout
                )
                
                # Convert back to internal format
                return RequestVoteResponse(
                    term=proto_response.term,
                    vote_granted=proto_response.vote_granted
                )
            finally:
                channel.close()
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.DEADLINE_EXCEEDED:
                logger.debug(f"RequestVote RPC error to {address}: {e}")
            return None
        except Exception as e:
            logger.debug(f"RequestVote error to {address}: {e}")
            return None
    
    def append_entries(self, address: str, request: AppendEntriesRequest) -> Optional[AppendEntriesResponse]:
        """Send AppendEntries RPC to another node via gRPC"""
        try:
            # Convert entries to proto
            proto_entries = []
            for entry in request.entries:
                cmd = entry.command  # Already a dict
                cmd_type = cmd['type']
                proto_type_name = cmd_type.upper()
                try:
                    proto_cmd_type = raft_pb2.CommandType.Value(proto_type_name)
                except ValueError as exc:
                    logger.error(f"Unknown command type '{cmd_type}' when sending AppendEntries to {address}")
                    raise
                proto_entries.append(raft_pb2.LogEntry(
                    index=entry.index,
                    term=entry.term,
                    command=raft_pb2.Command(
                        type=proto_cmd_type,
                        key=cmd['key'],
                        value=cmd.get('value', '')
                    )
                ))
            
            # Convert to proto
            proto_request = raft_pb2.AppendEntriesRequest(
                term=request.term,
                leader_id=request.leader_id,
                prev_log_index=request.prev_log_index,
                prev_log_term=request.prev_log_term,
                entries=proto_entries,
                leader_commit=request.leader_commit
            )
            
            # DEBUG: Log outgoing request
            import sys
            print(f"[gRPC-Client] >>> AppendEntries to {address}: term={request.term}, prev_index={request.prev_log_index}, entries={len(request.entries)}", flush=True)
            sys.stdout.flush()
            
            # Create stub if needed
            target = address.replace('http://', '').replace('https://', '')
            try:
                channel = grpc.insecure_channel(
                    target,
                    options=[
                        ('grpc.keepalive_time_ms', 10000),
                        ('grpc.keepalive_timeout_ms', 5000),
                    ]
                )
                stub = raft_pb2_grpc.RaftRPCStub(channel)
                
                # Make call with timeout
                proto_response = stub.AppendEntries(
                    proto_request,
                    timeout=self.timeout
                )
                
                # DEBUG: Log response
                import sys
                print(f"[gRPC-Client] <<< Response from {address}: success={proto_response.success}, match_index={proto_response.match_index}, term={proto_response.term}", flush=True)
                sys.stdout.flush()
                
                # Convert back to internal format
                return AppendEntriesResponse(
                    term=proto_response.term,
                    success=proto_response.success,
                    match_index=proto_response.match_index
                )
            finally:
                channel.close()
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.DEADLINE_EXCEEDED:
                logger.debug(f"AppendEntries RPC error to {address}: {e}")
            return None
        except Exception as e:
            logger.debug(f"AppendEntries error to {address}: {e}")
            return None
    
    def close(self):
        """Close all channels"""
        for channel in self._channels.values():
            try:
                channel.close()
            except:
                pass
        self._channels.clear()
        self._stubs.clear()
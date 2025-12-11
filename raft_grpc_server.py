"""
gRPC server for Raft consensus and KV store operations.

Replaces HTTP/JSON with gRPC for 5-10x faster binary serialization,
HTTP/2 multiplexing, and production-grade RPC communication.
"""
import logging
import grpc
from concurrent import futures
from typing import Optional
import raft_pb2
import raft_pb2_grpc
from command import Command, CommandType
from rpc import (
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse,
    LogEntryData
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RaftRPCServicer(raft_pb2_grpc.RaftRPCServicer):
    """Implementation of Raft RPC service"""
    
    def __init__(self, raft_node):
        self.raft_node = raft_node
    
    def RequestVote(self, request: raft_pb2.RequestVoteRequest, context: grpc.ServicerContext) -> raft_pb2.RequestVoteResponse:
        """Handle RequestVote RPC"""
        try:
            internal_request = RequestVoteRequest(
                term=request.term,
                candidate_id=request.candidate_id,
                last_log_index=request.last_log_index,
                last_log_term=request.last_log_term
            )
            
            internal_response = self.raft_node.handle_request_vote(internal_request)
            
            return raft_pb2.RequestVoteResponse(
                term=internal_response.term,
                vote_granted=internal_response.vote_granted
            )
        except Exception as e:
            logger.error(f"RequestVote error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.RequestVoteResponse()
    
    def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context: grpc.ServicerContext) -> raft_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC"""
        try:
            # DEBUG: Log incoming request
            print(f"[gRPC-Server] >>> AppendEntries from leader {request.leader_id} (term={request.term}, prev_index={request.prev_log_index}, entries={len(request.entries)})", flush=True)
            
            entries = []
            for entry in request.entries:
                proto_type_name = raft_pb2.CommandType.Name(entry.command.type)
                cmd = Command.from_dict({
                    'type': proto_type_name.lower(),
                    'key': entry.command.key,
                    'value': entry.command.value if entry.command.value else None
                })
                entries.append(LogEntryData(
                    index=entry.index,
                    term=entry.term,
                    command=cmd.to_dict()
                ))
            
            internal_request = AppendEntriesRequest(
                term=request.term,
                leader_id=request.leader_id,
                prev_log_index=request.prev_log_index,
                prev_log_term=request.prev_log_term,
                entries=entries,
                leader_commit=request.leader_commit
            )
            
            internal_response = self.raft_node.handle_append_entries(internal_request)
            
            # DEBUG: Log response
            print(f"[gRPC-Server] <<< Returning: success={internal_response.success}, match_index={internal_response.match_index}, term={internal_response.term}", flush=True)
            
            return raft_pb2.AppendEntriesResponse(
                term=internal_response.term,
                success=internal_response.success,
                match_index=internal_response.match_index
            )
        except Exception as e:
            logger.error(f"AppendEntries error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.AppendEntriesResponse()


class KVStoreServicer(raft_pb2_grpc.KVStoreServicer):
    """Implementation of KV Store service"""
    
    def __init__(self, raft_node):
        self.raft_node = raft_node
    
    def Get(self, request: raft_pb2.GetRequest, context: grpc.ServicerContext) -> raft_pb2.GetResponse:
        """Handle Get request"""
        try:
            value = self.raft_node.state_machine.get(request.key)
            if value is None:
                return raft_pb2.GetResponse(found=False)
            return raft_pb2.GetResponse(found=True, value=str(value))
        except Exception as e:
            logger.error(f"Get error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.GetResponse(found=False)
    
    def Put(self, request: raft_pb2.PutRequest, context: grpc.ServicerContext) -> raft_pb2.PutResponse:
        """Handle Put request"""
        try:
            if not self.raft_node.is_leader():
                leader = self.raft_node.leader_id or "unknown"
                return raft_pb2.PutResponse(success=False, leader_hint=leader)
            
            cmd = Command(CommandType.PUT, request.key, request.value)
            success = self.raft_node.replicate_command(cmd)
            
            return raft_pb2.PutResponse(success=success, leader_hint="")
        except Exception as e:
            logger.error(f"Put error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.PutResponse(success=False)
    
    def Delete(self, request: raft_pb2.DeleteRequest, context: grpc.ServicerContext) -> raft_pb2.DeleteResponse:
        """Handle Delete request"""
        try:
            if not self.raft_node.is_leader():
                leader = self.raft_node.leader_id or "unknown"
                return raft_pb2.DeleteResponse(success=False, leader_hint=leader)
            
            cmd = Command(CommandType.DELETE, request.key)
            success = self.raft_node.replicate_command(cmd)
            
            return raft_pb2.DeleteResponse(success=success, leader_hint="")
        except Exception as e:
            logger.error(f"Delete error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.DeleteResponse(success=False)
    
    def Range(self, request: raft_pb2.RangeRequest, context: grpc.ServicerContext) -> raft_pb2.RangeResponse:
        """Handle Range query"""
        try:
            items = []
            for key, value in self.raft_node.state_machine.range_query(request.start, request.end):
                items.append(raft_pb2.RangeItem(key=key, value=str(value)))
            
            return raft_pb2.RangeResponse(items=items)
        except Exception as e:
            logger.error(f"Range error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.RangeResponse()
    
    def Status(self, request: raft_pb2.StatusRequest, context: grpc.ServicerContext) -> raft_pb2.StatusResponse:
        """Handle Status request"""
        try:
            status = self.raft_node.get_status()
            return raft_pb2.StatusResponse(
                node_id=status['node_id'],
                state=status['state'],
                current_term=status['current_term'],
                commit_index=status['commit_index'],
                last_applied=status['last_applied'],
                log_length=status['log_length'],
                voted_for=status.get('voted_for', ''),
                last_snapshot_index=status.get('last_snapshot_index', 0),
                last_snapshot_term=status.get('last_snapshot_term', 0)
            )
        except Exception as e:
            logger.error(f"Status error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return raft_pb2.StatusResponse()


def create_grpc_server(raft_node, host: str, port: int):
    """
    Create and start gRPC server.
    
    Args:
        raft_node: The RaftNode instance
        host: Host to bind to (e.g., 'localhost')
        port: Port to listen on
    
    Returns:
        grpc.Server instance
    """
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', -1),
            ('grpc.max_receive_message_length', -1),
        ]
    )
    
    raft_pb2_grpc.add_RaftRPCServicer_to_server(
        RaftRPCServicer(raft_node),
        server
    )
    raft_pb2_grpc.add_KVStoreServicer_to_server(
        KVStoreServicer(raft_node),
        server
    )
    
    server.add_insecure_port(f'{host}:{port}')
    logger.info(f"Starting gRPC server on {host}:{port}")
    server.start()
    
    return server

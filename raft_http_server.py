from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from rpc import RequestVoteRequest, AppendEntriesRequest

class RaftRPCHandler(BaseHTTPRequestHandler):
    """HTTP handler for Raft RPC endpoints"""
    
    # Class variable - the RaftNode instance
    raft_node = None
    
    def do_POST(self):
        """Handle POST requests for RPC"""
        if self.path == '/raft/request_vote':
            self._handle_request_vote()
        elif self.path == '/raft/append_entries':
            self._handle_append_entries()
        else:
            self.send_error(404)
    
    def _handle_request_vote(self):
        """Handle RequestVote RPC"""
        try:
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body)
            
            # Parse request
            request = RequestVoteRequest.from_dict(data)
            
            # Handle it
            response = self.raft_node.handle_request_vote(request)
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response.to_dict()).encode('utf-8'))
            
        except Exception as e:
            self.send_error(500, str(e))
    
    def _handle_append_entries(self):
        """Handle AppendEntries RPC"""
        try:
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body)
            
            # Parse request
            request = AppendEntriesRequest.from_dict(data)
            
            # Handle it
            response = self.raft_node.handle_append_entries(request)
            
            # Send response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response.to_dict()).encode('utf-8'))
            
        except Exception as e:
            self.send_error(500, str(e))
    
    def log_message(self, format, *args):
        """Suppress default logging"""
        pass  # Or use safe_print if you want logs

def create_raft_rpc_server(raft_node, host, port):
    """
    Create an HTTP server for Raft RPC.
    
    Args:
        raft_node: The RaftNode instance
        host: Host to bind to
        port: Port to listen on
        
    Returns:
        HTTPServer instance
    """
    RaftRPCHandler.raft_node = raft_node
    server = HTTPServer((host, port), RaftRPCHandler)
    return server
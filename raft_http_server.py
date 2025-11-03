from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from rpc import RequestVoteRequest, AppendEntriesRequest

class RaftRPCHandler(BaseHTTPRequestHandler):
    """HTTP handler for Raft RPC endpoints"""
    
    # Class variable - the RaftNode instance
    raft_node = None
    
    def do_POST(self):
        """Handle POST requests for RPC"""
        try:
            if self.path == '/raft/request_vote':
                self._handle_request_vote()
            elif self.path == '/raft/append_entries':
                self._handle_append_entries()
            else:
                self.send_error(404)
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected, nothing we can do
            pass
    
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
            
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected before we could respond
            pass
        except Exception as e:
            # Try to send error, but ignore if client already disconnected
            try:
                self.send_error(500, str(e))
            except (BrokenPipeError, ConnectionResetError):
                pass
    
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
            
        except (BrokenPipeError, ConnectionResetError):
            # Client disconnected before we could respond
            pass
        except Exception as e:
            # Try to send error, but ignore if client already disconnected
            try:
                self.send_error(500, str(e))
            except (BrokenPipeError, ConnectionResetError):
                pass
    
    def log_message(self, format, *args):
        """Suppress default HTTP logging"""
        pass

    def handle_error(self, request, client_address):
        """Override to suppress error printing for broken pipes"""
        # Only log non-network errors
        import sys
        exc_type = sys.exc_info()[0]
        if exc_type not in (BrokenPipeError, ConnectionResetError):
            super().handle_error(request, client_address)

def create_raft_rpc_server(raft_node, host, port):
    """
    Create an HTTP server for Raft RPC.
    """
    # Create a NEW handler class for each server with its own node
    class NodeSpecificHandler(RaftRPCHandler):
        # Override with this specific node
        node_instance = raft_node
    
    # Use the node_instance instead of raft_node in handlers
    class RaftRPCHandlerForNode(BaseHTTPRequestHandler):
        """HTTP handler for Raft RPC endpoints"""
        
        def do_POST(self):
            """Handle POST requests for RPC"""
            try:
                if self.path == '/raft/request_vote':
                    self._handle_request_vote()
                elif self.path == '/raft/append_entries':
                    self._handle_append_entries()
                else:
                    self.send_error(404)
            except (BrokenPipeError, ConnectionResetError):
                pass
        
        def _handle_request_vote(self):
            """Handle RequestVote RPC"""
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8')
                data = json.loads(body)
                
                request = RequestVoteRequest.from_dict(data)
                response = raft_node.handle_request_vote(request)  # Use the passed-in raft_node
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response.to_dict()).encode('utf-8'))
                
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                try:
                    self.send_error(500, str(e))
                except (BrokenPipeError, ConnectionResetError):
                    pass
        
        def _handle_append_entries(self):
            """Handle AppendEntries RPC"""
            print(f"[{raft_node.node_id}] <<< RECEIVED AppendEntries (handler)", flush=True)
            
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8')
                data = json.loads(body)
                
                request = AppendEntriesRequest.from_dict(data)
                response = raft_node.handle_append_entries(request)  # Use the passed-in raft_node
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response.to_dict()).encode('utf-8'))
                
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                try:
                    self.send_error(500, str(e))
                except (BrokenPipeError, ConnectionResetError):
                    pass
        
        def log_message(self, format, *args):
            """Suppress default HTTP logging"""
            pass
    
    # Create server with this specific handler
    class QuietHTTPServer(HTTPServer):
        def handle_error(self, request, client_address):
            import sys
            exc_type = sys.exc_info()[0]
            if exc_type not in (BrokenPipeError, ConnectionResetError):
                super().handle_error(request, client_address)
    
    server = QuietHTTPServer((host, port), RaftRPCHandlerForNode)
    return server
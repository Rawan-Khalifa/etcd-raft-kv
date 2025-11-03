from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from rpc import RequestVoteRequest, AppendEntriesRequest
from urllib.parse import urlparse
from command import Command, CommandType

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

# ADD THESE METHODS TO HANDLE HTTP API ENDPOINTS
    def do_GET(self):
        """Handle GET requests for HTTP API"""
        path = urlparse(self.path).path
        
        if path == '/status':
            self._handle_status()
        elif path.startswith('/kv/'):
            self._handle_get()
        else:
            self.send_error(404)
    
    def do_PUT(self):
        """Handle PUT requests for HTTP API"""
        path = urlparse(self.path).path
        
        if path.startswith('/kv/'):
            self._handle_put()
        else:
            self.send_error(404)
    
    def do_DELETE(self):
        """Handle DELETE requests for HTTP API"""
        path = urlparse(self.path).path
        
        if path.startswith('/kv/'):
            self._handle_delete()
        else:
            self.send_error(404)
    
    def _set_headers(self, status_code=200, content_type='application/json'):
        """Helper to set response headers"""
        self.send_response(status_code)
        self.send_header('Content-Type', content_type)
        self.end_headers()
    
    def _send_json(self, data, status_code=200):
        """Helper to send JSON response"""
        self._set_headers(status_code)
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def _handle_status(self):
        """Get node status"""
        try:
            status = self.raft_node.get_status()
            self._send_json(status)
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_get(self):
        """Get value for key"""
        path = urlparse(self.path).path
        key = path[4:]  # Remove '/kv/' prefix
        
        try:
            value = self.raft_node.get(key)
            
            if value is None:
                self._send_json({'error': 'Key not found'}, 404)
            else:
                self._send_json({'key': key, 'value': value})
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_put(self):
        """Put key-value pair"""
        path = urlparse(self.path).path
        key = path[4:]  # Remove '/kv/' prefix
        
        try:
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body)
            value = data.get('value')
            
            if value is None:
                self._send_json({'error': 'Missing "value" in request body'}, 400)
                return
            
            # Propose command
            command = Command(CommandType.PUT, key, value)
            result = self.raft_node.propose_command(command)
            
            if result['success']:
                self._send_json({
                    'key': key,
                    'value': value,
                    'message': 'Stored successfully',
                    'log_index': result.get('index')
                })
            else:
                self._send_json({
                    'error': result.get('error', 'Failed to store'),
                    'leader': result.get('leader')
                }, 503)
            
        except json.JSONDecodeError:
            self._send_json({'error': 'Invalid JSON in request body'}, 400)
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_delete(self):
        """Delete key"""
        path = urlparse(self.path).path
        key = path[4:]  # Remove '/kv/' prefix
        
        try:
            # Propose command
            command = Command(CommandType.DELETE, key)
            result = self.raft_node.propose_command(command)
            
            if result['success']:
                self._send_json({
                    'key': key,
                    'message': 'Deleted successfully',
                    'log_index': result.get('index')
                })
            else:
                self._send_json({
                    'error': result.get('error', 'Failed to delete'),
                    'leader': result.get('leader')
                }, 503)
                
        except Exception as e:
            self._send_json({'error': str(e)}, 500)

def create_raft_rpc_server(raft_node, host, port):
    """
    Create an HTTP server for Raft RPC AND HTTP API.
    Uses the combined RaftRPCHandler that handles both Raft communication and client API requests.
    """
    # Create a handler class bound to this specific raft_node
    class BoundHandler(RaftRPCHandler):
        pass
    
    # Bind the raft_node to the handler class
    BoundHandler.raft_node = raft_node
    
    # Create server with the bound handler
    class QuietHTTPServer(HTTPServer):
        def handle_error(self, request, client_address):
            import sys
            exc_type = sys.exc_info()[0]
            if exc_type not in (BrokenPipeError, ConnectionResetError):
                super().handle_error(request, client_address)
    
    server = QuietHTTPServer((host, port), BoundHandler)
    return server


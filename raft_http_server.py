from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from rpc import RequestVoteRequest, AppendEntriesRequest
from urllib.parse import urlparse, parse_qs
from command import Command, CommandType
from lease import LeaseManager, ReadCache

class RaftRPCHandler(BaseHTTPRequestHandler):
    """HTTP handler for Raft RPC endpoints"""
    
    # Class variables - the RaftNode instance and optional features
    raft_node = None
    lease_manager = None
    read_cache = None
    
    def do_POST(self):
        """Handle POST requests for RPC and write operations"""
        try:
            path = urlparse(self.path).path
            
            if path == '/raft/request_vote':
                self._handle_request_vote()
            elif path == '/raft/append_entries':
                self._handle_append_entries()
            elif path == '/membership/add':
                self._handle_add_member()
            elif path == '/membership/remove':
                self._handle_remove_member()
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
            pass
        except Exception as e:
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
            
            # LEASE REFRESH: On valid AppendEntries from leader, refresh lease
            if self.lease_manager and response.success:
                self.lease_manager.refresh_lease(request.leader_id)
            
            # Send response
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

    def handle_error(self, request, client_address):
        """Override to suppress error printing for broken pipes"""
        import sys
        exc_type = sys.exc_info()[0]
        if exc_type not in (BrokenPipeError, ConnectionResetError):
            super().handle_error(request, client_address)

    def do_GET(self):
        """Handle GET requests for HTTP API"""
        path = urlparse(self.path).path
        
        if path == '/status':
            self._handle_status()
        elif path == '/metrics':
            self._handle_metrics()
        elif path == '/membership/list':
            self._handle_list_members()
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
            
            # Add lease info if available
            if self.lease_manager:
                status['lease'] = {
                    'valid': self.lease_manager.is_lease_valid(),
                    'leader_id': self.lease_manager.leader_id
                }
            
            self._send_json(status)
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_metrics(self):
        """Get Prometheus metrics"""
        try:
            metrics_text = self.raft_node.metrics.prometheus_format(self.raft_node.node_id)
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
            self.end_headers()
            self.wfile.write(metrics_text.encode('utf-8'))
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_get(self):
        """
        Get value for key with lease-based read optimization.
        
        Fast path: If on a follower with valid leader lease, serve from cache/memory
        without consensus (10x faster but eventually consistent)
        
        Slow path: If on leader or no valid lease, serve from committed state (consistent)
        """
        path = urlparse(self.path).path
        key = path[4:]  # Remove '/kv/' prefix
        
        try:
            # Determine if we should serve from lease (fast but eventual consistency)
            use_lease = False
            if self.lease_manager and self.read_cache:
                node_state = self.raft_node.state.value
                if self.lease_manager.can_serve_read(node_state):
                    use_lease = True
                    # Try cache first
                    cached = self.read_cache.get(key)
                    if cached is not None:
                        return self._send_json({
                            'key': key,
                            'value': cached,
                            'from_cache': True,
                            'consistency': 'eventual'
                        })
            
            # Get value from state machine (always consistent)
            value = self.raft_node.get(key)
            
            if value is None:
                self._send_json({'error': 'Key not found'}, 404)
            else:
                # Cache the read if using lease
                if use_lease and self.read_cache:
                    self.read_cache.put(key, value)
                
                response = {
                    'key': key,
                    'value': value,
                    'from_cache': False,
                    'consistency': 'eventual' if use_lease else 'strong'
                }
                self._send_json(response)
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_put(self):
        """Put key-value pair (always requires consensus)"""
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
            
            # Propose command (requires consensus)
            command = Command(CommandType.PUT, key, value)
            result = self.raft_node.propose_command(command)
            
            if result['success']:
                # Invalidate cache on write
                if self.read_cache:
                    self.read_cache.invalidate(key)
                
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
        """Delete key (always requires consensus)"""
        path = urlparse(self.path).path
        key = path[4:]  # Remove '/kv/' prefix
        
        try:
            # Propose command (requires consensus)
            command = Command(CommandType.DELETE, key)
            result = self.raft_node.propose_command(command)
            
            if result['success']:
                # Invalidate cache on write
                if self.read_cache:
                    self.read_cache.invalidate(key)
                
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
    
    def _handle_list_members(self):
        """Get current cluster membership"""
        try:
            if hasattr(self.raft_node, 'dynamic_membership'):
                members = list(self.raft_node.dynamic_membership.current_peers)
            else:
                members = list(self.raft_node.peers)
            
            self._send_json({
                'members': members,
                'node_id': self.raft_node.node_id,
                'state': self.raft_node.state.value,
                'leader': self.raft_node.leader_id
            })
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_add_member(self):
        """Add a new member to the cluster via Raft consensus"""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body)
            
            peer_address = data.get('peer')
            if not peer_address:
                self._send_json({'error': 'Missing "peer" in request'}, 400)
                return
            
            # Check if we're the leader
            if self.raft_node.state.value != 'LEADER':
                self._send_json({
                    'error': 'Only leader can add members',
                    'leader': self.raft_node.leader_id
                }, 503)
                return
            
            # Check if already in cluster
            if peer_address in self.raft_node.peers:
                self._send_json({'error': 'Server already in cluster'}, 400)
                return
            
            # Create a membership change command and add to log
            # This will be replicated to all followers through Raft consensus
            command = Command(CommandType.MEMBERSHIP_ADD, peer_address)
            result = self.raft_node.propose_command(command)
            
            if result['success']:
                self._send_json({
                    'message': f'Added {peer_address} to cluster',
                    'members': list(self.raft_node.peers),
                    'log_index': result.get('index')
                }, 200)
            else:
                self._send_json({
                    'error': result.get('error', 'Failed to add member'),
                    'leader': result.get('leader')
                }, 503)
    
        except json.JSONDecodeError:
            self._send_json({'error': 'Invalid JSON in request'}, 400)
        except Exception as e:
            self._send_json({'error': str(e)}, 500)
    
    def _handle_remove_member(self):
        """Remove a member from the cluster via Raft consensus"""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body)
            
            peer_address = data.get('peer')
            if not peer_address:
                self._send_json({'error': 'Missing "peer" in request'}, 400)
                return
            
            # Check if we're the leader
            if self.raft_node.state.value != 'LEADER':
                self._send_json({
                    'error': 'Only leader can remove members',
                    'leader': self.raft_node.leader_id
                }, 503)
                return
            
            # Check if in cluster
            if peer_address not in self.raft_node.peers:
                self._send_json({'error': 'Server not in cluster'}, 400)
                return
            
            # Prevent removing the last node
            if len(self.raft_node.peers) <= 1:
                self._send_json({'error': 'Cannot remove last server'}, 400)
                return
            
            # Create a membership change command and add to log
            # This will be replicated to all followers through Raft consensus
            command = Command(CommandType.MEMBERSHIP_REMOVE, peer_address)
            result = self.raft_node.propose_command(command)
            
            if result['success']:
                self._send_json({
                    'message': f'Removed {peer_address} from cluster',
                    'members': list(self.raft_node.peers),
                    'log_index': result.get('index')
                }, 200)
            else:
                self._send_json({
                    'error': result.get('error', 'Failed to remove member'),
                    'leader': result.get('leader')
                }, 503)
    
        except json.JSONDecodeError:
            self._send_json({'error': 'Invalid JSON in request'}, 400)
        except Exception as e:
            self._send_json({'error': str(e)}, 500)


def create_raft_rpc_server(raft_node, host, port, enable_lease=True):
    """
    Create an HTTP server for Raft RPC AND HTTP API.
    
    Args:
        raft_node: The RaftNode instance
        host: Host to bind to
        port: Port to bind to
        enable_lease: Enable lease-based reads for 10x read speedup
    """
    # Create a handler class bound to this specific raft_node
    class BoundHandler(RaftRPCHandler):
        pass
    
    # Bind the raft_node to the handler class
    BoundHandler.raft_node = raft_node
    
    # Initialize lease manager and read cache if enabled
    if enable_lease:
        BoundHandler.lease_manager = LeaseManager(lease_duration_ms=500)
        BoundHandler.read_cache = ReadCache(ttl_seconds=1.0)
    
    # Create server with the bound handler
    class QuietHTTPServer(HTTPServer):
        def handle_error(self, request, client_address):
            import sys
            exc_type = sys.exc_info()[0]
            if exc_type not in (BrokenPipeError, ConnectionResetError):
                super().handle_error(request, client_address)
    
    server = QuietHTTPServer((host, port), BoundHandler)
    return server


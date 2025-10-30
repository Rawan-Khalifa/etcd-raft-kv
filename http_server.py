from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
import json
from coordinator import Coordinator
from command import Command, CommandType

class KVStoreHandler(BaseHTTPRequestHandler):
    """HTTP request handler but now using coordinator"""
    
    # Class variable - shared coordinator
    coordinator = None
    
    def _set_headers(self, status_code=200, content_type='application/json'):
        """Helper to set response headers"""
        self.send_response(status_code)
        self.send_header('Content-Type', content_type)
        self.end_headers()
    
    def _send_json(self, data, status_code=200):
        """Helper to send JSON response"""
        self._set_headers(status_code)
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def _parse_key_from_path(self):
        """Extract key from URL path like /kv/mykey"""
        path = urlparse(self.path).path
        parts = path.strip('/').split('/')
        
        if len(parts) >= 2 and parts[0] == 'kv':
            return '/'.join(parts[1:])
        return None
    
    def do_GET(self):
        """Handle GET requests"""
        path = urlparse(self.path).path
        
        # Status endpoint
        if path == '/status':
            status = self.coordinator.get_status()
            self._send_json(status)
            return
        
        # Key-value get
        key = self._parse_key_from_path()
        
        if key is None:
            self._send_json({'error': 'Invalid path. Use /kv/{key}'}, 400)
            return
        
        value = self.coordinator.get(key)
        
        if value is None:
            self._send_json({'error': 'Key not found'}, 404)
        else:
            self._send_json({'key': key, 'value': value})
            
    def do_PUT(self):
        """Handle PUT requests - store a key-value pair"""
        key = self._parse_key_from_path()
        
        if key is None:
            self._send_json({'error': 'Invalid path. Use /kv/{key}'}, 400)
            return
        
        # Read the request body to get the value
        content_length = int(self.headers.get('Content-Length', 0)) # how many bytes to read
        body = self.rfile.read(content_length).decode('utf-8')
        
        try:
            data = json.loads(body)
            value = data.get('value')
            
            if value is None:
                self._send_json({'error': 'Missing "value" in request body'}, 400)
                return
            
            self.store.put(key, value)
            self._send_json({'key': key, 'value': value, 'message': 'Stored successfully'})
            
        except json.JSONDecodeError:
            self._send_json({'error': 'Invalid JSON in request body'}, 400)
    
    def do_DELETE(self):
        """Handle DELETE requests - delete a key"""
        key = self._parse_key_from_path()
        
        if key is None:
            self._send_json({'error': 'Invalid path. Use /kv/{key}'}, 400)
            return
        
        deleted = self.store.delete(key)
        
        if deleted:
            self._send_json({'key': key, 'message': 'Deleted successfully'})
        else:
            self._send_json({'error': 'Key not found'}, 404)
    
    def log_message(self, format, *args):
        """Override to customize logging"""
        print(f"[HTTP] {self.command} {self.path} - {args[1]}")


def run_server(host='localhost', port=8080):
    """Start the HTTP server"""
    # Create the store and attach it to the handler class
    KVStoreHandler.store = KVStore()
    
    server_address = (host, port)
    httpd = HTTPServer(server_address, KVStoreHandler)
    
    print(f"🚀 KV Store server running on http://{host}:{port}")
    print(f"   GET    http://{host}:{port}/kv/{{key}}")
    print(f"   PUT    http://{host}:{port}/kv/{{key}}")
    print(f"   DELETE http://{host}:{port}/kv/{{key}}")
    print("\nPress Ctrl+C to stop\n")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n Shutting down server.. womp wooomp...")
        httpd.shutdown()


if __name__ == "__main__":
    run_server()
#!/usr/bin/env python3
"""
Real-time cluster state visualizer
Shows leader, followers, log state, and health
"""
import time
import sys
import os
import requests

class ClusterVisualizer:
    def __init__(self, nodes):
        self.nodes = nodes
        
    def clear_screen(self):
        os.system('clear' if os.name != 'nt' else 'cls')
    
    def get_node_status(self, raft_addr):
        """Get status from a single node"""
        try:
            # Convert Raft port to API port (now they're the same)
            api_addr = raft_addr  # No conversion needed anymore
            
            response = requests.get(f"{api_addr}/status", timeout=1)
            return {
                'alive': True,
                'status': response.json()
            }
        except Exception as e:
            return {
                'alive': False,
                'error': str(e)
            }
    
    def get_cluster_state(self):
        """Get state from all nodes"""
        states = []
        for addr in self.nodes:
            node_status = self.get_node_status(addr)
            
            if node_status['alive']:
                # Node is alive, extract the actual status
                states.append({
                    'address': addr,
                    'status': node_status['status'],  # The actual status dict from the node
                    'alive': True
                })
            else:
                # Node is dead
                states.append({
                    'address': addr,
                    'status': None,
                    'alive': False
                })
        return states
    
    def render_node(self, node_state):
        """Render a single node's state"""
        addr = node_state['address']
        port = addr.split(':')[-1]
        node_id = f"node{int(port) - 9000}"
        
        if not node_state['alive']:
            return f"  ❌ {addr} - DEAD"
        
        status = node_state['status']
        if not status:
            return f"  ❌ {addr} - NO STATUS"
        
        # Handle missing keys gracefully
        state_str = status.get('state', 'UNKNOWN')
        term = status.get('term', 0)
        log_size = status.get('log_size', 0)
        commit_index = status.get('commit_index', 0)
        last_applied = status.get('last_applied', 0)
        
        # Format based on state
        if state_str == 'LEADER':
            icon = "👑"
            color = "🟢"
        elif state_str == 'CANDIDATE':
            icon = "🗳️"
            color = "🟡"
        elif state_str == 'FOLLOWER':
            icon = "👥"
            color = "🔵"
        else:
            icon = "❓"
            color = "🔴"
        
        return f"  {icon} {addr} - {color} {state_str} (Term {term}, Log: {log_size}, Commit: {commit_index}, Applied: {last_applied})"
    
    def render_cluster(self):
        """Render entire cluster state"""
        self.clear_screen()
        
        print("=" * 80)
        print("🌐  RAFT CLUSTER STATE".center(80))
        print("=" * 80)
        print()
        
        states = self.get_cluster_state()
        
        # Count nodes by state - only for alive nodes with valid status
        alive_count = sum(1 for s in states if s['alive'])
        leader_count = sum(1 for s in states if s['alive'] and s['status'] and s['status'].get('state') == 'LEADER')
        candidate_count = sum(1 for s in states if s['alive'] and s['status'] and s['status'].get('state') == 'CANDIDATE')
        follower_count = sum(1 for s in states if s['alive'] and s['status'] and s['status'].get('state') == 'FOLLOWER')
        
        # Find leader
        leader = None
        leader_term = 0
        for state in states:
            if state['alive'] and state['status'] and state['status'].get('state') == 'LEADER':
                port = state['address'].split(':')[-1]
                leader = f"node{int(port) - 9000}"
                leader_term = state['status'].get('term', 0)
                break
        
        # Cluster health summary
        print(f"  📊 Cluster Health: {alive_count}/3 nodes alive")
        print(f"  👥 States: {leader_count} Leader, {candidate_count} Candidates, {follower_count} Followers")
        
        if leader:
            print(f"  👑 Current Leader: {leader} (Term {leader_term})")
        else:
            print(f"  ⚠️  Current Leader: No leader elected")
        
        print()
        print("  " + "-" * 76)
        print()
        
        # Node states
        for state in states:
            print(self.render_node(state))
        
        print()
        print("=" * 80)
        print()
        print("  Press Ctrl+C to exit | Updates every 0.5s")
    
    def watch(self, interval=0.5):
        """Continuously watch cluster state"""
        try:
            while True:
                self.render_cluster()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\n✋ Stopping visualizer...")
            sys.exit(0)


if __name__ == "__main__":
    nodes = [
        "http://localhost:9001",
        "http://localhost:9002",
        "http://localhost:9003"
    ]
    
    print("Starting cluster visualizer...")
    print("Make sure your cluster is running with:")
    print("  ./start_cluster.sh")
    print()
    
    visualizer = ClusterVisualizer(nodes)
    visualizer.watch(interval=0.5)
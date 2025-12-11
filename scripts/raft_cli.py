#!/usr/bin/env python3
"""
Simple CLI for interacting with a running Raft cluster
Usage:
    ./raft_cli.py put mykey myvalue
    ./raft_cli.py get mykey
    ./raft_cli.py delete mykey
    ./raft_cli.py status
    ./raft_cli.py members
"""

import sys
import requests
import json

# Default cluster nodes
NODES = [
    "http://localhost:9010",
    "http://localhost:9011",
    "http://localhost:9012"
]

def find_leader():
    """Find the current leader node"""
    for node in NODES:
        try:
            response = requests.get(f"{node}/status", timeout=2)
            status = response.json()
            if status['state'] == 'LEADER':
                return node
        except:
            continue
    return NODES[0]  # Fallback to first node

def put(key, value):
    """Put a key-value pair"""
    leader = find_leader()
    try:
        response = requests.put(
            f"{leader}/kv/{key}",
            json={"value": value},
            timeout=5
        )
        if response.status_code == 200:
            print(f"✓ Set {key} = {value}")
        else:
            print(f"✗ Error: {response.json().get('error', 'Unknown error')}")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")

def get(key):
    """Get a value by key"""
    node = NODES[0]  # Can read from any node
    try:
        response = requests.get(f"{node}/kv/{key}", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(data['value'])
            if data.get('from_cache'):
                print("  (from cache)")
        elif response.status_code == 404:
            print(f"✗ Key '{key}' not found")
        else:
            print(f"✗ Error: {response.json().get('error', 'Unknown error')}")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")

def delete(key):
    """Delete a key"""
    leader = find_leader()
    try:
        response = requests.delete(f"{leader}/kv/{key}", timeout=5)
        if response.status_code == 200:
            print(f"✓ Deleted {key}")
        else:
            print(f"✗ Error: {response.json().get('error', 'Unknown error')}")
    except Exception as e:
        print(f"✗ Failed to connect: {e}")

def status():
    """Show cluster status"""
    print("Cluster Status:")
    print("-" * 50)
    for node in NODES:
        try:
            response = requests.get(f"{node}/status", timeout=2)
            data = response.json()
            state = data['state']
            marker = "👑" if state == "LEADER" else "  "
            print(f"{marker} {node}: {state} (term {data['term']}, {data['log_size']} entries)")
        except:
            print(f"✗ {node}: UNREACHABLE")

def members():
    """Show cluster members"""
    node = NODES[0]
    try:
        response = requests.get(f"{node}/membership/list", timeout=5)
        data = response.json()
        print("Cluster Members:")
        print("-" * 50)
        for i, member in enumerate(data.get('members', []), 1):
            print(f"{i}. {member}")
    except Exception as e:
        print(f"✗ Failed to get members: {e}")

def usage():
    """Print usage information"""
    print("""
Raft CLI - Simple command-line interface for Raft cluster

USAGE:
    raft_cli.py <command> [arguments]

COMMANDS:
    put <key> <value>    Write a key-value pair
    get <key>            Read a value by key
    delete <key>         Delete a key
    status               Show cluster status
    members              List cluster members

EXAMPLES:
    ./raft_cli.py put user:alice "Alice Johnson"
    ./raft_cli.py get user:alice
    ./raft_cli.py delete user:alice
    ./raft_cli.py status
    ./raft_cli.py members
""")

def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "put":
        if len(sys.argv) != 4:
            print("Usage: raft_cli.py put <key> <value>")
            sys.exit(1)
        put(sys.argv[2], sys.argv[3])
    
    elif command == "get":
        if len(sys.argv) != 3:
            print("Usage: raft_cli.py get <key>")
            sys.exit(1)
        get(sys.argv[2])
    
    elif command == "delete":
        if len(sys.argv) != 3:
            print("Usage: raft_cli.py delete <key>")
            sys.exit(1)
        delete(sys.argv[2])
    
    elif command == "status":
        status()
    
    elif command == "members":
        members()
    
    else:
        print(f"Unknown command: {command}")
        usage()
        sys.exit(1)

if __name__ == "__main__":
    main()

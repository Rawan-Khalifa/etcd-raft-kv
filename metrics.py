"""
Prometheus metrics for Raft consensus monitoring
"""
import time
from typing import Dict
from enum import Enum

class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"

class Metrics:
    """Prometheus metrics collector"""
    
    def __init__(self):
        # Counter metrics
        self.elections_total = 0
        self.leader_elections_total = 0
        self.lost_elections_total = 0
        
        # Gauge metrics (instantaneous values)
        self.current_term = 0
        self.commit_index = 0
        self.last_applied = 0
        self.log_size = 0
        self.follower_count = 0
        self.peer_count = 0
        
        # Histogram metrics
        self.election_latency_ms = []
        self.replication_latency_ms = []
        
        # State tracking
        self.last_election_time = None
        self.started_at = time.time()
        self.leader_changes = 0
        self.last_leader_id = None
    
    def record_election_start(self):
        """Record start of election"""
        self.last_election_time = time.time()
        self.elections_total += 1
    
    def record_election_won(self):
        """Record successful election"""
        self.leader_elections_total += 1
        if self.last_election_time:
            latency = (time.time() - self.last_election_time) * 1000
            self.election_latency_ms.append(latency)
            # Cap histogram samples at 1000 entries
            if len(self.election_latency_ms) > 1000:
                self.election_latency_ms.pop(0)
    
    def record_election_lost(self):
        """Record failed election"""
        self.lost_elections_total += 1
    
    def record_leader_change(self, new_leader_id: str):
        """Record leader change"""
        if self.last_leader_id != new_leader_id:
            self.leader_changes += 1
            self.last_leader_id = new_leader_id
    
    def record_replication_latency(self, latency_ms: float):
        """Record replication latency (keep last 1000 samples)"""
        self.replication_latency_ms.append(latency_ms)
        if len(self.replication_latency_ms) > 1000:
            self.replication_latency_ms.pop(0)  # Remove oldest
    
    def update_state(self, current_term: int, commit_index: int, 
                     last_applied: int, log_size: int, peer_count: int):
        """Update gauge metrics"""
        self.current_term = current_term
        self.commit_index = commit_index
        self.last_applied = last_applied
        self.log_size = log_size
        self.peer_count = peer_count
    
    def get_metrics(self) -> Dict:
        """Get all metrics as dictionary"""
        uptime_seconds = time.time() - self.started_at
        
        # Calculate averages
        avg_election_latency = (
            sum(self.election_latency_ms) / len(self.election_latency_ms)
            if self.election_latency_ms else 0
        )
        
        avg_replication_latency = (
            sum(self.replication_latency_ms) / len(self.replication_latency_ms)
            if self.replication_latency_ms else 0
        )
        
        return {
            'uptime_seconds': uptime_seconds,
            'elections_total': self.elections_total,
            'leader_elections_total': self.leader_elections_total,
            'lost_elections_total': self.lost_elections_total,
            'leader_changes_total': self.leader_changes,
            'current_term': self.current_term,
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'log_size': self.log_size,
            'peer_count': self.peer_count,
            'avg_election_latency_ms': avg_election_latency,
            'avg_replication_latency_ms': avg_replication_latency,
            'election_latency_samples': len(self.election_latency_ms),
            'replication_latency_samples': len(self.replication_latency_ms),
        }
    
    def _calculate_histogram_buckets(self, samples: list, buckets: list) -> dict:
        """Calculate histogram bucket counts"""
        bucket_counts = {b: 0 for b in buckets}
        bucket_counts['+Inf'] = len(samples)
        
        for sample in samples:
            for bucket in buckets:
                if sample <= bucket:
                    bucket_counts[bucket] += 1
        
        return bucket_counts
    
    def prometheus_format(self, node_id: str) -> str:
        """Export metrics in Prometheus text format"""
        metrics = self.get_metrics()
        lines = [
            f"# HELP raft_elections_total Total number of elections",
            f"# TYPE raft_elections_total counter",
            f'raft_elections_total{{node="{node_id}"}} {metrics["elections_total"]}',
            f"",
            f"# HELP raft_leader_elections_total Total successful leader elections",
            f"# TYPE raft_leader_elections_total counter",
            f'raft_leader_elections_total{{node="{node_id}"}} {metrics["leader_elections_total"]}',
            f"",
            f"# HELP raft_lost_elections_total Total failed elections",
            f"# TYPE raft_lost_elections_total counter",
            f'raft_lost_elections_total{{node="{node_id}"}} {metrics["lost_elections_total"]}',
            f"",
            f"# HELP raft_current_term Current Raft term",
            f"# TYPE raft_current_term gauge",
            f'raft_current_term{{node="{node_id}"}} {metrics["current_term"]}',
            f"",
            f"# HELP raft_commit_index Highest committed log index",
            f"# TYPE raft_commit_index gauge",
            f'raft_commit_index{{node="{node_id}"}} {metrics["commit_index"]}',
            f"",
            f"# HELP raft_log_size Number of entries in log",
            f"# TYPE raft_log_size gauge",
            f'raft_log_size{{node="{node_id}"}} {metrics["log_size"]}',
            f"",
            f"# HELP raft_uptime_seconds Node uptime",
            f"# TYPE raft_uptime_seconds gauge",
            f'raft_uptime_seconds{{node="{node_id}"}} {metrics["uptime_seconds"]:.2f}',
            f"",
            # Add histogram for election latency
            f"# HELP raft_election_latency_ms Election completion time in milliseconds",
            f"# TYPE raft_election_latency_ms histogram",
        ]
        
        # Define buckets (in milliseconds)
        election_buckets = [10, 50, 100, 500, 1000, 5000]
        buckets = self._calculate_histogram_buckets(self.election_latency_ms, election_buckets)
        
        # Export bucket counts
        for bucket in election_buckets:
            lines.append(f'raft_election_latency_ms_bucket{{node="{node_id}",le="{bucket}"}} {buckets[bucket]}')
        lines.append(f'raft_election_latency_ms_bucket{{node="{node_id}",le="+Inf"}} {buckets["+Inf"]}')
        
        # Export sum and count
        total_ms = sum(self.election_latency_ms)
        count = len(self.election_latency_ms)
        lines.append(f'raft_election_latency_ms_sum{{node="{node_id}"}} {total_ms}')
        lines.append(f'raft_election_latency_ms_count{{node="{node_id}"}} {count}')
        lines.append("")
        
        # Same for replication latency
        lines.append(f"# HELP raft_replication_latency_ms Replication time in milliseconds")
        lines.append(f"# TYPE raft_replication_latency_ms histogram")
        
        replication_buckets = [1, 5, 10, 50, 100, 500]
        buckets = self._calculate_histogram_buckets(self.replication_latency_ms, replication_buckets)
        
        for bucket in replication_buckets:
            lines.append(f'raft_replication_latency_ms_bucket{{node="{node_id}",le="{bucket}"}} {buckets[bucket]}')
        lines.append(f'raft_replication_latency_ms_bucket{{node="{node_id}",le="+Inf"}} {buckets["+Inf"]}')
        
        total_ms = sum(self.replication_latency_ms)
        count = len(self.replication_latency_ms)
        lines.append(f'raft_replication_latency_ms_sum{{node="{node_id}"}} {total_ms}')
        lines.append(f'raft_replication_latency_ms_count{{node="{node_id}"}} {count}')
        
        return "\n".join(lines)
"""
Worker heartbeat and status management.
Tracks worker health and detects dead workers.
"""
import time
from typing import Optional, Dict, List
from datetime import datetime
from enum import Enum
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class WorkerStatus(str, Enum):
    """Worker status enumeration."""
    ACTIVE = "active"  # Worker is processing tasks
    IDLE = "idle"      # Worker is running but not processing
    DEAD = "dead"      # Worker has not sent heartbeat (stale)


class WorkerHeartbeat:
    """
    Manages worker heartbeats and status tracking.
    """
    
    def __init__(
        self,
        heartbeat_interval: int = 10,
        heartbeat_ttl: int = 30,
        stale_threshold: int = 30
    ):
        """
        Initialize worker heartbeat manager.
        
        Args:
            heartbeat_interval: Seconds between heartbeats (default: 10)
            heartbeat_ttl: TTL for heartbeat key in Redis (default: 30)
            stale_threshold: Seconds before worker considered dead (default: 30)
        """
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.stale_threshold = stale_threshold
        
        log.info(
            "Initialized worker heartbeat manager",
            extra={
                "heartbeat_interval": heartbeat_interval,
                "heartbeat_ttl": heartbeat_ttl,
                "stale_threshold": stale_threshold
            }
        )
    
    def _get_heartbeat_key(self, worker_id: str) -> str:
        """Get Redis key for worker heartbeat."""
        return f"worker:{worker_id}:heartbeat"
    
    def _get_status_key(self, worker_id: str) -> str:
        """Get Redis key for worker status."""
        return f"worker:{worker_id}:status"
    
    def _get_metadata_key(self, worker_id: str) -> str:
        """Get Redis key for worker metadata."""
        return f"worker:{worker_id}:metadata"
    
    def send_heartbeat(
        self,
        worker_id: str,
        status: WorkerStatus = WorkerStatus.IDLE,
        metadata: Optional[Dict] = None
    ) -> bool:
        """
        Send heartbeat from worker.
        
        Args:
            worker_id: Worker identifier
            status: Current worker status
            metadata: Additional worker metadata
            
        Returns:
            True if heartbeat sent successfully
        """
        heartbeat_key = self._get_heartbeat_key(worker_id)
        status_key = self._get_status_key(worker_id)
        metadata_key = self._get_metadata_key(worker_id)
        
        current_time = time.time()
        timestamp = datetime.utcnow().isoformat()
        
        # Store heartbeat with TTL
        redis_broker.client.setex(
            heartbeat_key,
            self.heartbeat_ttl,
            str(current_time)
        )
        
        # Store status
        redis_broker.client.set(status_key, status.value)
        redis_broker.client.expire(status_key, self.heartbeat_ttl * 2)
        
        # Store metadata
        if metadata:
            import json
            metadata_json = json.dumps(metadata)
            redis_broker.client.setex(
                metadata_key,
                self.heartbeat_ttl * 2,
                metadata_json
            )
        
        log.debug(
            f"Worker {worker_id} sent heartbeat",
            extra={
                "worker_id": worker_id,
                "status": status.value,
                "timestamp": timestamp
            }
        )
        
        return True
    
    def get_heartbeat(self, worker_id: str) -> Optional[float]:
        """
        Get last heartbeat timestamp for worker.
        
        Args:
            worker_id: Worker identifier
            
        Returns:
            Timestamp of last heartbeat, or None if not found
        """
        heartbeat_key = self._get_heartbeat_key(worker_id)
        heartbeat_str = redis_broker.client.get(heartbeat_key)
        
        if heartbeat_str:
            if isinstance(heartbeat_str, bytes):
                heartbeat_str = heartbeat_str.decode()
            return float(heartbeat_str)
        
        return None
    
    def get_worker_status(self, worker_id: str) -> WorkerStatus:
        """
        Get current status of worker.
        
        Args:
            worker_id: Worker identifier
            
        Returns:
            Worker status
        """
        status_key = self._get_status_key(worker_id)
        status_str = redis_broker.client.get(status_key)
        
        if status_str:
            if isinstance(status_str, bytes):
                status_str = status_str.decode()
            try:
                return WorkerStatus(status_str)
            except ValueError:
                pass
        
        # Check if heartbeat exists
        heartbeat = self.get_heartbeat(worker_id)
        if heartbeat:
            return WorkerStatus.IDLE
        
        return WorkerStatus.DEAD
    
    def is_worker_alive(self, worker_id: str) -> bool:
        """
        Check if worker is alive (has recent heartbeat).
        
        Args:
            worker_id: Worker identifier
            
        Returns:
            True if worker is alive
        """
        heartbeat = self.get_heartbeat(worker_id)
        
        if heartbeat is None:
            return False
        
        # Check if heartbeat is stale
        elapsed = time.time() - heartbeat
        
        return elapsed < self.stale_threshold
    
    def get_stale_workers(self) -> List[str]:
        """
        Get list of workers with stale heartbeats (dead workers).
        
        Returns:
            List of worker IDs with stale heartbeats
        """
        # Find all worker heartbeat keys
        pattern = "worker:*:heartbeat"
        keys = list(redis_broker.client.scan_iter(match=pattern))
        
        stale_workers = []
        current_time = time.time()
        
        for key in keys:
            # Extract worker_id from key
            worker_id = key.decode().split(":")[1] if isinstance(key, bytes) else key.split(":")[1]
            
            heartbeat = self.get_heartbeat(worker_id)
            
            if heartbeat is None:
                stale_workers.append(worker_id)
            else:
                elapsed = current_time - heartbeat
                if elapsed >= self.stale_threshold:
                    stale_workers.append(worker_id)
                    # Mark as dead
                    self.mark_worker_dead(worker_id)
        
        return stale_workers
    
    def mark_worker_dead(self, worker_id: str) -> None:
        """
        Mark worker as dead.
        
        Args:
            worker_id: Worker identifier
        """
        status_key = self._get_status_key(worker_id)
        redis_broker.client.set(status_key, WorkerStatus.DEAD.value)
        redis_broker.client.expire(status_key, 3600)  # Keep for 1 hour
        
        log.warning(
            f"Marked worker {worker_id} as DEAD",
            extra={"worker_id": worker_id}
        )
    
    def get_all_workers(self) -> List[str]:
        """
        Get list of all registered workers.
        
        Returns:
            List of worker IDs
        """
        pattern = "worker:*:heartbeat"
        keys = list(redis_broker.client.scan_iter(match=pattern))
        
        workers = []
        for key in keys:
            worker_id = key.decode().split(":")[1] if isinstance(key, bytes) else key.split(":")[1]
            workers.append(worker_id)
        
        return workers
    
    def get_worker_info(self, worker_id: str) -> Dict:
        """
        Get complete information about a worker.
        
        Args:
            worker_id: Worker identifier
            
        Returns:
            Dictionary with worker information
        """
        heartbeat = self.get_heartbeat(worker_id)
        status = self.get_worker_status(worker_id)
        
        # Get metadata
        metadata_key = self._get_metadata_key(worker_id)
        metadata_json = redis_broker.client.get(metadata_key)
        metadata = {}
        
        if metadata_json:
            import json
            if isinstance(metadata_json, bytes):
                metadata_json = metadata_json.decode()
            try:
                metadata = json.loads(metadata_json)
            except:
                pass
        
        info = {
            "worker_id": worker_id,
            "status": status.value,
            "is_alive": self.is_worker_alive(worker_id),
            "last_heartbeat": heartbeat,
            "last_heartbeat_ago": time.time() - heartbeat if heartbeat else None,
            "metadata": metadata
        }
        
        return info
    
    def get_all_workers_info(self) -> List[Dict]:
        """
        Get information about all workers.
        
        Returns:
            List of worker info dictionaries
        """
        workers = self.get_all_workers()
        
        return [self.get_worker_info(worker_id) for worker_id in workers]
    
    def remove_worker(self, worker_id: str) -> None:
        """
        Remove worker from tracking.
        
        Args:
            worker_id: Worker identifier
        """
        heartbeat_key = self._get_heartbeat_key(worker_id)
        status_key = self._get_status_key(worker_id)
        metadata_key = self._get_metadata_key(worker_id)
        
        redis_broker.client.delete(heartbeat_key)
        redis_broker.client.delete(status_key)
        redis_broker.client.delete(metadata_key)
        
        log.info(f"Removed worker {worker_id} from tracking")


# Global worker heartbeat instance
worker_heartbeat = WorkerHeartbeat(
    heartbeat_interval=10,
    heartbeat_ttl=30,
    stale_threshold=30
)

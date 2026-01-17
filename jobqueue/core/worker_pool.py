"""
Worker pool management for running multiple workers safely.
Handles worker scaling, coordination, and ensures no duplicate task processing.
"""
import os
import signal
import multiprocessing
import time
from typing import List, Dict, Optional, Any
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.utils.logger import log


class WorkerPool:
    """
    Manages a pool of workers for distributed task processing.
    Ensures safe concurrent execution with no duplicate processing.
    """
    
    def __init__(
        self,
        pool_name: str = "default",
        worker_class=None,
        queue_name: str = "default",
        initial_workers: int = 1
    ):
        """
        Initialize worker pool.
        
        Args:
            pool_name: Name of the worker pool
            worker_class: Worker class to instantiate
            queue_name: Queue name for workers
            initial_workers: Initial number of workers to start
        """
        self.pool_name = pool_name
        self.worker_class = worker_class
        self.queue_name = queue_name
        self.initial_workers = initial_workers
        
        self.workers: Dict[str, multiprocessing.Process] = {}
        self.worker_configs: Dict[str, Dict] = {}
        self.is_running = False
        
        log.info(
            f"WorkerPool initialized",
            extra={
                "pool_name": pool_name,
                "queue_name": queue_name,
                "initial_workers": initial_workers
            }
        )
    
    def _get_worker_id(self, index: int) -> str:
        """Generate unique worker ID for pool worker."""
        import socket
        hostname = socket.gethostname()
        pid = os.getpid()
        return f"{self.pool_name}-worker-{hostname}-{pid}-{index}"
    
    def _worker_process(self, worker_id: str, queue_name: str, config: Dict):
        """
        Worker process entry point.
        Runs a worker instance in a separate process.
        
        Args:
            worker_id: Unique worker ID
            queue_name: Queue to process
            config: Worker configuration
        """
        try:
            # Import here to avoid circular imports
            from jobqueue.worker.simple_worker import SimpleWorker
            
            # Create worker instance
            worker = SimpleWorker(worker_id=worker_id, queue_name=queue_name)
            
            # Start worker
            worker.start()
            
        except Exception as e:
            log.error(
                f"Worker process {worker_id} failed: {e}",
                extra={"worker_id": worker_id, "error": str(e)}
            )
            raise
    
    def start(self) -> None:
        """Start the worker pool."""
        if self.is_running:
            log.warning(f"Worker pool {self.pool_name} already running")
            return
        
        if not self.worker_class:
            raise ValueError("worker_class must be provided")
        
        log.info(
            f"Starting worker pool {self.pool_name} with {self.initial_workers} workers"
        )
        
        self.is_running = True
        
        # Start initial workers
        for i in range(self.initial_workers):
            self.add_worker()
        
        log.info(
            f"Worker pool {self.pool_name} started",
            extra={"worker_count": len(self.workers)}
        )
    
    def stop(self, graceful: bool = True) -> None:
        """
        Stop the worker pool.
        
        Args:
            graceful: If True, send SIGTERM for graceful shutdown
        """
        if not self.is_running:
            return
        
        log.info(
            f"Stopping worker pool {self.pool_name}",
            extra={"worker_count": len(self.workers), "graceful": graceful}
        )
        
        self.is_running = False
        
        # Stop all workers
        for worker_id, process in list(self.workers.items()):
            self.remove_worker(worker_id, graceful=graceful)
        
        log.info(f"Worker pool {self.pool_name} stopped")
    
    def add_worker(self) -> str:
        """
        Add a worker to the pool.
        
        Returns:
            Worker ID of the new worker
        """
        if not self.is_running:
            raise RuntimeError("Worker pool is not running")
        
        # Generate unique worker ID
        index = len(self.workers)
        worker_id = self._get_worker_id(index)
        
        # Create worker process
        process = multiprocessing.Process(
            target=self._worker_process,
            args=(worker_id, self.queue_name, {}),
            name=worker_id,
            daemon=False
        )
        
        process.start()
        
        # Store worker
        self.workers[worker_id] = process
        self.worker_configs[worker_id] = {
            "started_at": datetime.utcnow().isoformat(),
            "queue_name": self.queue_name
        }
        
        log.info(
            f"Added worker {worker_id} to pool {self.pool_name}",
            extra={
                "worker_id": worker_id,
                "pool_name": self.pool_name,
                "total_workers": len(self.workers)
            }
        )
        
        return worker_id
    
    def remove_worker(self, worker_id: str, graceful: bool = True) -> bool:
        """
        Remove a worker from the pool.
        
        Args:
            worker_id: Worker ID to remove
            graceful: If True, send SIGTERM for graceful shutdown
            
        Returns:
            True if worker removed successfully
        """
        if worker_id not in self.workers:
            log.warning(f"Worker {worker_id} not found in pool")
            return False
        
        process = self.workers[worker_id]
        
        if graceful:
            # Send SIGTERM for graceful shutdown
            process.terminate()
            
            # Wait for graceful shutdown (max 10 seconds)
            process.join(timeout=10)
            
            if process.is_alive():
                log.warning(
                    f"Worker {worker_id} did not shutdown gracefully, forcing kill"
                )
                process.kill()
                process.join()
        else:
            # Force kill
            process.kill()
            process.join()
        
        # Remove from pool
        del self.workers[worker_id]
        if worker_id in self.worker_configs:
            del self.worker_configs[worker_id]
        
        log.info(
            f"Removed worker {worker_id} from pool {self.pool_name}",
            extra={
                "worker_id": worker_id,
                "pool_name": self.pool_name,
                "remaining_workers": len(self.workers)
            }
        )
        
        return True
    
    def scale_up(self, count: int = 1) -> List[str]:
        """
        Scale up the worker pool by adding workers.
        
        Args:
            count: Number of workers to add
            
        Returns:
            List of new worker IDs
        """
        new_workers = []
        
        for i in range(count):
            worker_id = self.add_worker()
            new_workers.append(worker_id)
        
        log.info(
            f"Scaled up pool {self.pool_name} by {count} workers",
            extra={
                "pool_name": self.pool_name,
                "added": count,
                "total_workers": len(self.workers)
            }
        )
        
        return new_workers
    
    def scale_down(self, count: int = 1) -> List[str]:
        """
        Scale down the worker pool by removing workers.
        
        Args:
            count: Number of workers to remove
            
        Returns:
            List of removed worker IDs
        """
        if count > len(self.workers):
            count = len(self.workers)
        
        removed_workers = []
        
        # Remove workers (FIFO - remove oldest first)
        worker_ids = list(self.workers.keys())[:count]
        
        for worker_id in worker_ids:
            if self.remove_worker(worker_id, graceful=True):
                removed_workers.append(worker_id)
        
        log.info(
            f"Scaled down pool {self.pool_name} by {len(removed_workers)} workers",
            extra={
                "pool_name": self.pool_name,
                "removed": len(removed_workers),
                "total_workers": len(self.workers)
            }
        )
        
        return removed_workers
    
    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get worker pool status.
        
        Returns:
            Dictionary with pool status
        """
        worker_statuses = []
        
        for worker_id, process in self.workers.items():
            config = self.worker_configs.get(worker_id, {})
            
            # Check if process is alive
            is_alive = process.is_alive()
            
            # Get worker info from heartbeat if available
            worker_info = None
            try:
                all_workers = worker_heartbeat.get_all_workers_info()
                for w in all_workers:
                    if w.get("worker_id") == worker_id:
                        worker_info = w
                        break
            except Exception:
                pass
            
            worker_statuses.append({
                "worker_id": worker_id,
                "process_alive": is_alive,
                "started_at": config.get("started_at"),
                "queue_name": config.get("queue_name"),
                "status": worker_info.get("status") if worker_info else None,
                "is_alive": worker_info.get("is_alive", False) if worker_info else is_alive
            })
        
        return {
            "pool_name": self.pool_name,
            "queue_name": self.queue_name,
            "is_running": self.is_running,
            "total_workers": len(self.workers),
            "alive_workers": sum(1 for s in worker_statuses if s["process_alive"]),
            "workers": worker_statuses
        }
    
    def restart_worker(self, worker_id: str) -> bool:
        """
        Restart a worker.
        
        Args:
            worker_id: Worker ID to restart
            
        Returns:
            True if worker restarted successfully
        """
        if worker_id not in self.workers:
            return False
        
        # Remove old worker
        self.remove_worker(worker_id, graceful=True)
        
        # Add new worker
        new_worker_id = self.add_worker()
        
        log.info(
            f"Restarted worker {worker_id} -> {new_worker_id}",
            extra={
                "old_worker_id": worker_id,
                "new_worker_id": new_worker_id
            }
        )
        
        return True


class DistributedWorkerManager:
    """
    Manages multiple worker pools across different machines.
    Ensures safe distributed execution with Redis atomic operations.
    """
    
    def __init__(self):
        """Initialize distributed worker manager."""
        self.pools: Dict[str, WorkerPool] = {}
        log.info("DistributedWorkerManager initialized")
    
    def create_pool(
        self,
        pool_name: str,
        worker_class=None,
        queue_name: str = "default",
        initial_workers: int = 1
    ) -> WorkerPool:
        """
        Create a new worker pool.
        
        Args:
            pool_name: Name of the pool
            worker_class: Worker class to use
            queue_name: Queue name
            initial_workers: Initial number of workers
            
        Returns:
            Created WorkerPool instance
        """
        if pool_name in self.pools:
            raise ValueError(f"Pool {pool_name} already exists")
        
        pool = WorkerPool(
            pool_name=pool_name,
            worker_class=worker_class,
            queue_name=queue_name,
            initial_workers=initial_workers
        )
        
        self.pools[pool_name] = pool
        
        log.info(
            f"Created worker pool {pool_name}",
            extra={
                "pool_name": pool_name,
                "queue_name": queue_name,
                "initial_workers": initial_workers
            }
        )
        
        return pool
    
    def get_pool(self, pool_name: str) -> Optional[WorkerPool]:
        """Get a worker pool by name."""
        return self.pools.get(pool_name)
    
    def remove_pool(self, pool_name: str, graceful: bool = True) -> bool:
        """
        Remove a worker pool.
        
        Args:
            pool_name: Pool name
            graceful: Graceful shutdown
            
        Returns:
            True if removed successfully
        """
        if pool_name not in self.pools:
            return False
        
        pool = self.pools[pool_name]
        pool.stop(graceful=graceful)
        
        del self.pools[pool_name]
        
        log.info(f"Removed worker pool {pool_name}")
        
        return True
    
    def get_all_pools_status(self) -> Dict[str, Any]:
        """Get status of all worker pools."""
        pools_status = {}
        
        for pool_name, pool in self.pools.items():
            pools_status[pool_name] = pool.get_pool_status()
        
        return {
            "total_pools": len(self.pools),
            "pools": pools_status
        }


# Global distributed worker manager
distributed_worker_manager = DistributedWorkerManager()

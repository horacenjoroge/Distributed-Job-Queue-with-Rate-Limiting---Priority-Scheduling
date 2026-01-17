"""
Worker autoscaling system for dynamically adjusting worker count based on queue size.
"""
import time
import threading
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.worker_pool import WorkerPool
from jobqueue.utils.logger import log


class WorkerAutoscaler:
    """
    Automatically scales workers up/down based on queue size.
    Monitors queue size and adjusts worker count accordingly.
    """
    
    def __init__(
        self,
        pool: WorkerPool,
        scale_up_threshold: int = 100,
        scale_down_threshold: int = 10,
        min_workers: int = 1,
        max_workers: int = 50,
        check_interval: int = 30,
        cooldown_seconds: int = 60,
        scale_up_count: int = 1,
        scale_down_count: int = 1
    ):
        """
        Initialize worker autoscaler.
        
        Args:
            pool: WorkerPool to manage
            scale_up_threshold: Queue size threshold to scale up (default: 100)
            scale_down_threshold: Queue size threshold to scale down (default: 10)
            min_workers: Minimum number of workers (default: 1)
            max_workers: Maximum number of workers (default: 50)
            check_interval: How often to check queue size in seconds (default: 30)
            cooldown_seconds: Cooldown period between scaling actions (default: 60)
            scale_up_count: Number of workers to add when scaling up (default: 1)
            scale_down_count: Number of workers to remove when scaling down (default: 1)
        """
        self.pool = pool
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.check_interval = check_interval
        self.cooldown_seconds = cooldown_seconds
        self.scale_up_count = scale_up_count
        self.scale_down_count = scale_down_count
        
        self.is_running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.last_scale_action: Optional[datetime] = None
        self.scaling_history: List[Dict[str, Any]] = []
        
        log.info(
            f"WorkerAutoscaler initialized",
            extra={
                "pool_name": pool.pool_name,
                "scale_up_threshold": scale_up_threshold,
                "scale_down_threshold": scale_down_threshold,
                "min_workers": min_workers,
                "max_workers": max_workers,
                "check_interval": check_interval,
                "cooldown_seconds": cooldown_seconds
            }
        )
    
    def _get_queue_size(self) -> int:
        """
        Get current queue size.
        
        Returns:
            Queue size
        """
        try:
            # Try FIFO queue first
            queue = Queue(self.pool.queue_name)
            size = queue.size()
            return size
        except Exception:
            try:
                # Try priority queue
                queue = PriorityQueue(self.pool.queue_name)
                size = queue.size()
                return size
            except Exception as e:
                log.error(f"Error getting queue size: {e}")
                return 0
    
    def _health_check(self) -> bool:
        """
        Perform health check before scaling.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Check Redis connection
            if not redis_broker.is_connected():
                redis_broker.connect()
            
            # Check if pool is running
            if not self.pool.is_running:
                return False
            
            # Check if we can get queue size
            queue_size = self._get_queue_size()
            
            return True
            
        except Exception as e:
            log.error(f"Health check failed: {e}")
            return False
    
    def _can_scale(self) -> bool:
        """
        Check if we can perform a scaling action (cooldown check).
        
        Returns:
            True if cooldown period has passed
        """
        if self.last_scale_action is None:
            return True
        
        elapsed = (datetime.utcnow() - self.last_scale_action).total_seconds()
        return elapsed >= self.cooldown_seconds
    
    def _should_scale_up(self, queue_size: int) -> bool:
        """
        Check if we should scale up.
        
        Args:
            queue_size: Current queue size
            
        Returns:
            True if should scale up
        """
        if not self._can_scale():
            return False
        
        if queue_size <= self.scale_up_threshold:
            return False
        
        # Check max workers limit
        current_workers = len(self.pool.workers)
        if current_workers >= self.max_workers:
            log.debug(
                f"Max workers ({self.max_workers}) reached, cannot scale up",
                extra={
                    "current_workers": current_workers,
                    "max_workers": self.max_workers,
                    "queue_size": queue_size
                }
            )
            return False
        
        return True
    
    def _should_scale_down(self, queue_size: int) -> bool:
        """
        Check if we should scale down.
        
        Args:
            queue_size: Current queue size
            
        Returns:
            True if should scale down
        """
        if not self._can_scale():
            return False
        
        if queue_size >= self.scale_down_threshold:
            return False
        
        # Check min workers limit
        current_workers = len(self.pool.workers)
        if current_workers <= self.min_workers:
            log.debug(
                f"Min workers ({self.min_workers}) reached, cannot scale down",
                extra={
                    "current_workers": current_workers,
                    "min_workers": self.min_workers,
                    "queue_size": queue_size
                }
            )
            return False
        
        return True
    
    def _scale_up(self) -> bool:
        """
        Scale up workers.
        
        Returns:
            True if scaled up successfully
        """
        try:
            # Health check
            if not self._health_check():
                log.warning("Health check failed, skipping scale up")
                return False
            
            # Calculate how many workers to add
            current_workers = len(self.pool.workers)
            queue_size = self._get_queue_size()
            
            # Calculate desired workers based on queue size
            # Add 1 worker per 100 tasks (with max limit)
            desired_workers = min(
                queue_size // self.scale_up_threshold + 1,
                self.max_workers
            )
            
            workers_to_add = min(
                desired_workers - current_workers,
                self.scale_up_count,
                self.max_workers - current_workers
            )
            
            if workers_to_add <= 0:
                return False
            
            # Scale up
            new_workers = self.pool.scale_up(workers_to_add)
            
            self.last_scale_action = datetime.utcnow()
            
            self.scaling_history.append({
                "action": "scale_up",
                "timestamp": self.last_scale_action.isoformat(),
                "workers_added": len(new_workers),
                "queue_size": queue_size,
                "total_workers": len(self.pool.workers)
            })
            
            log.info(
                f"Autoscaled up: added {len(new_workers)} workers",
                extra={
                    "pool_name": self.pool.pool_name,
                    "workers_added": len(new_workers),
                    "queue_size": queue_size,
                    "total_workers": len(self.pool.workers)
                }
            )
            
            return True
            
        except Exception as e:
            log.error(f"Error scaling up: {e}")
            return False
    
    def _scale_down(self) -> bool:
        """
        Scale down workers.
        
        Returns:
            True if scaled down successfully
        """
        try:
            # Health check
            if not self._health_check():
                log.warning("Health check failed, skipping scale down")
                return False
            
            # Only scale down if queue is small and workers are idle
            current_workers = len(self.pool.workers)
            queue_size = self._get_queue_size()
            
            # Calculate how many workers to remove
            # Keep at least enough workers for current queue size
            if queue_size > 0:
                # Keep at least 1 worker per 10 tasks
                min_needed = max(1, (queue_size // 10) + 1)
            else:
                min_needed = self.min_workers
            
            workers_to_remove = min(
                current_workers - min_needed,
                self.scale_down_count,
                current_workers - self.min_workers
            )
            
            if workers_to_remove <= 0:
                return False
            
            # Scale down
            removed_workers = self.pool.scale_down(workers_to_remove)
            
            self.last_scale_action = datetime.utcnow()
            
            self.scaling_history.append({
                "action": "scale_down",
                "timestamp": self.last_scale_action.isoformat(),
                "workers_removed": len(removed_workers),
                "queue_size": queue_size,
                "total_workers": len(self.pool.workers)
            })
            
            log.info(
                f"Autoscaled down: removed {len(removed_workers)} workers",
                extra={
                    "pool_name": self.pool.pool_name,
                    "workers_removed": len(removed_workers),
                    "queue_size": queue_size,
                    "total_workers": len(self.pool.workers)
                }
            )
            
            return True
            
        except Exception as e:
            log.error(f"Error scaling down: {e}")
            return False
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        log.info(f"Autoscaler monitoring started for pool {self.pool.pool_name}")
        
        while self.is_running:
            try:
                # Get queue size
                queue_size = self._get_queue_size()
                
                # Check if should scale up
                if self._should_scale_up(queue_size):
                    self._scale_up()
                
                # Check if should scale down
                elif self._should_scale_down(queue_size):
                    self._scale_down()
                
                # Wait before next check
                time.sleep(self.check_interval)
                
            except Exception as e:
                log.error(f"Error in autoscaler monitor loop: {e}")
                time.sleep(self.check_interval)
        
        log.info(f"Autoscaler monitoring stopped for pool {self.pool.pool_name}")
    
    def start(self) -> None:
        """Start the autoscaler."""
        if self.is_running:
            log.warning(f"Autoscaler for pool {self.pool.pool_name} already running")
            return
        
        self.is_running = True
        
        # Start monitor thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name=f"autoscaler-{self.pool.pool_name}"
        )
        self.monitor_thread.start()
        
        log.info(
            f"Autoscaler started for pool {self.pool.pool_name}",
            extra={
                "pool_name": self.pool.pool_name,
                "check_interval": self.check_interval,
                "scale_up_threshold": self.scale_up_threshold,
                "scale_down_threshold": self.scale_down_threshold
            }
        )
    
    def stop(self) -> None:
        """Stop the autoscaler."""
        if not self.is_running:
            return
        
        log.info(f"Stopping autoscaler for pool {self.pool.pool_name}")
        
        self.is_running = False
        
        # Wait for monitor thread to finish
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        log.info(f"Autoscaler stopped for pool {self.pool.pool_name}")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get autoscaler status.
        
        Returns:
            Dictionary with autoscaler status
        """
        queue_size = self._get_queue_size()
        current_workers = len(self.pool.workers)
        
        return {
            "pool_name": self.pool.pool_name,
            "is_running": self.is_running,
            "queue_size": queue_size,
            "current_workers": current_workers,
            "scale_up_threshold": self.scale_up_threshold,
            "scale_down_threshold": self.scale_down_threshold,
            "min_workers": self.min_workers,
            "max_workers": self.max_workers,
            "check_interval": self.check_interval,
            "cooldown_seconds": self.cooldown_seconds,
            "last_scale_action": self.last_scale_action.isoformat() if self.last_scale_action else None,
            "can_scale": self._can_scale(),
            "should_scale_up": self._should_scale_up(queue_size),
            "should_scale_down": self._should_scale_down(queue_size),
            "scaling_history_count": len(self.scaling_history)
        }
    
    def get_scaling_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get scaling history.
        
        Args:
            limit: Maximum number of history entries to return
            
        Returns:
            List of scaling history entries
        """
        return self.scaling_history[-limit:]


# Global autoscaler registry
_autoscalers: Dict[str, WorkerAutoscaler] = {}


def create_autoscaler(
    pool: WorkerPool,
    scale_up_threshold: int = 100,
    scale_down_threshold: int = 10,
    min_workers: int = 1,
    max_workers: int = 50,
    check_interval: int = 30,
    cooldown_seconds: int = 60
) -> WorkerAutoscaler:
    """
    Create and register an autoscaler for a worker pool.
    
    Args:
        pool: WorkerPool to manage
        scale_up_threshold: Queue size threshold to scale up
        scale_down_threshold: Queue size threshold to scale down
        min_workers: Minimum number of workers
        max_workers: Maximum number of workers
        check_interval: How often to check queue size
        cooldown_seconds: Cooldown period between scaling actions
        
    Returns:
        Created WorkerAutoscaler instance
    """
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=scale_up_threshold,
        scale_down_threshold=scale_down_threshold,
        min_workers=min_workers,
        max_workers=max_workers,
        check_interval=check_interval,
        cooldown_seconds=cooldown_seconds
    )
    
    _autoscalers[pool.pool_name] = autoscaler
    
    log.info(f"Created autoscaler for pool {pool.pool_name}")
    
    return autoscaler


def get_autoscaler(pool_name: str) -> Optional[WorkerAutoscaler]:
    """Get autoscaler by pool name."""
    return _autoscalers.get(pool_name)


def remove_autoscaler(pool_name: str) -> bool:
    """Remove autoscaler by pool name."""
    if pool_name in _autoscalers:
        autoscaler = _autoscalers[pool_name]
        autoscaler.stop()
        del _autoscalers[pool_name]
        return True
    return False

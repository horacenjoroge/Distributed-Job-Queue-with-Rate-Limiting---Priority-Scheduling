"""
Task recovery system for crashed workers.
Detects orphaned tasks and re-queues them safely.
"""
import time
from typing import List, Optional, Dict
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.worker_heartbeat import worker_heartbeat
from jobqueue.utils.logger import log


class TaskRecovery:
    """
    Manages task recovery from crashed workers.
    Detects orphaned tasks and re-queues them safely.
    """
    
    def __init__(self):
        """Initialize task recovery system."""
        log.info("Initialized task recovery system")
    
    def _get_active_tasks_key(self, worker_id: str) -> str:
        """Get Redis key for worker's active tasks."""
        return f"worker:{worker_id}:active"
    
    def _get_task_lock_key(self, task_id: str) -> str:
        """Get Redis key for task execution lock."""
        return f"task:lock:{task_id}"
    
    def add_active_task(self, worker_id: str, task: Task) -> bool:
        """
        Add task to worker's active set.
        
        Args:
            worker_id: Worker identifier
            task: Task being processed
            
        Returns:
            True if added successfully
        """
        try:
            key = self._get_active_tasks_key(worker_id)
            task_json = task.to_json()
            
            # Add to set
            redis_broker.client.sadd(key, task_json)
            redis_broker.client.expire(key, 3600)  # 1 hour TTL
            
            # Also store task lock
            lock_key = self._get_task_lock_key(task.id)
            redis_broker.client.setex(
                lock_key,
                3600,  # 1 hour TTL
                worker_id
            )
            
            log.debug(
                f"Added task {task.id} to active set for worker {worker_id}",
                extra={
                    "worker_id": worker_id,
                    "task_id": task.id
                }
            )
            
            return True
            
        except Exception as e:
            log.error(f"Failed to add active task: {e}")
            return False
    
    def remove_active_task(self, worker_id: str, task: Task) -> bool:
        """
        Remove task from worker's active set.
        
        Args:
            worker_id: Worker identifier
            task: Completed task
            
        Returns:
            True if removed successfully
        """
        try:
            key = self._get_active_tasks_key(worker_id)
            task_json = task.to_json()
            
            # Remove from set
            redis_broker.client.srem(key, task_json)
            
            # Remove task lock
            lock_key = self._get_task_lock_key(task.id)
            redis_broker.client.delete(lock_key)
            
            log.debug(
                f"Removed task {task.id} from active set for worker {worker_id}",
                extra={
                    "worker_id": worker_id,
                    "task_id": task.id
                }
            )
            
            return True
            
        except Exception as e:
            log.error(f"Failed to remove active task: {e}")
            return False
    
    def get_active_tasks(self, worker_id: str) -> List[Task]:
        """
        Get all active tasks for a worker.
        
        Args:
            worker_id: Worker identifier
            
        Returns:
            List of active tasks
        """
        key = self._get_active_tasks_key(worker_id)
        task_jsons = redis_broker.client.smembers(key)
        
        tasks = []
        for task_json in task_jsons:
            try:
                if isinstance(task_json, bytes):
                    task_json = task_json.decode()
                task = Task.from_json(task_json)
                tasks.append(task)
            except Exception as e:
                log.error(f"Failed to deserialize active task: {e}")
        
        return tasks
    
    def get_orphaned_tasks(self) -> List[tuple[str, Task]]:
        """
        Get orphaned tasks (tasks assigned to dead workers).
        
        Returns:
            List of (worker_id, task) tuples
        """
        orphaned = []
        
        try:
            # Get all dead workers
            dead_workers = worker_heartbeat.get_stale_workers()
            
            if not dead_workers:
                return orphaned
            
            log.info(
                f"Checking for orphaned tasks from {len(dead_workers)} dead workers",
                extra={"dead_workers": dead_workers}
            )
            
            # Check each dead worker's active tasks
            for worker_id in dead_workers:
                active_tasks = self.get_active_tasks(worker_id)
                
                for task in active_tasks:
                    # Verify task is still locked to this worker
                    lock_key = self._get_task_lock_key(task.id)
                    lock_worker = redis_broker.client.get(lock_key)
                    
                    if lock_worker:
                        if isinstance(lock_worker, bytes):
                            lock_worker = lock_worker.decode()
                        
                        # Task is orphaned if locked to dead worker
                        if lock_worker == worker_id:
                            orphaned.append((worker_id, task))
            
        except Exception as e:
            log.error(f"Error getting orphaned tasks: {e}")
        
        return orphaned
    
    def recover_orphaned_tasks(self) -> int:
        """
        Recover all orphaned tasks and re-queue them.
        
        Returns:
            Number of tasks recovered
        """
        orphaned = self.get_orphaned_tasks()
        
        if not orphaned:
            return 0
        
        log.info(
            f"Recovering {len(orphaned)} orphaned tasks",
            extra={"orphaned_count": len(orphaned)}
        )
        
        recovered_count = 0
        
        for worker_id, task in orphaned:
            try:
                # Acquire lock to prevent duplicate execution
                if not self._acquire_recovery_lock(task.id):
                    log.warning(
                        f"Task {task.id} already being recovered by another process",
                        extra={"task_id": task.id}
                    )
                    continue
                
                # Reset task status
                task.status = TaskStatus.PENDING
                task.worker_id = None
                task.started_at = None
                
                # Remove from dead worker's active set
                self.remove_active_task(worker_id, task)
                
                # Re-enqueue task
                from jobqueue.core.redis_queue import Queue
                from jobqueue.core.priority_queue import PriorityQueue
                
                if task.priority:
                    queue = PriorityQueue(task.queue_name)
                else:
                    queue = Queue(task.queue_name)
                
                queue.enqueue(task)
                
                # Release lock after short delay (task is queued)
                time.sleep(0.1)
                self._release_recovery_lock(task.id)
                
                recovered_count += 1
                
                log.info(
                    f"Recovered task {task.id} from dead worker {worker_id}",
                    extra={
                        "task_id": task.id,
                        "worker_id": worker_id,
                        "queue": task.queue_name
                    }
                )
                
            except Exception as e:
                log.error(
                    f"Failed to recover task {task.id}: {e}",
                    extra={"task_id": task.id, "error": str(e)}
                )
                # Release lock on error
                self._release_recovery_lock(task.id)
        
        return recovered_count
    
    def _acquire_recovery_lock(self, task_id: str, timeout: int = 5) -> bool:
        """
        Acquire lock for task recovery (prevents duplicate recovery).
        
        Args:
            task_id: Task ID
            timeout: Lock timeout in seconds
            
        Returns:
            True if lock acquired
        """
        lock_key = f"task:recovery:lock:{task_id}"
        
        # Try to set lock (only if not exists)
        result = redis_broker.client.set(
            lock_key,
            "recovering",
            nx=True,  # Only set if not exists
            ex=timeout  # Expire after timeout
        )
        
        return bool(result)
    
    def _release_recovery_lock(self, task_id: str) -> None:
        """
        Release recovery lock.
        
        Args:
            task_id: Task ID
        """
        lock_key = f"task:recovery:lock:{task_id}"
        redis_broker.client.delete(lock_key)
    
    def is_task_locked(self, task_id: str) -> bool:
        """
        Check if task is currently locked (being executed).
        
        Args:
            task_id: Task ID
            
        Returns:
            True if task is locked
        """
        lock_key = self._get_task_lock_key(task_id)
        lock = redis_broker.client.get(lock_key)
        return lock is not None
    
    def get_task_lock_owner(self, task_id: str) -> Optional[str]:
        """
        Get the worker ID that has locked this task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Worker ID or None
        """
        lock_key = self._get_task_lock_key(task_id)
        lock_owner = redis_broker.client.get(lock_key)
        
        if lock_owner:
            if isinstance(lock_owner, bytes):
                lock_owner = lock_owner.decode()
            return lock_owner
        
        return None
    
    def get_recovery_stats(self) -> Dict:
        """
        Get task recovery statistics.
        
        Returns:
            Dictionary with recovery stats
        """
        orphaned = self.get_orphaned_tasks()
        
        # Count by worker
        by_worker = {}
        for worker_id, task in orphaned:
            by_worker[worker_id] = by_worker.get(worker_id, 0) + 1
        
        # Count by queue
        by_queue = {}
        for worker_id, task in orphaned:
            queue = task.queue_name
            by_queue[queue] = by_queue.get(queue, 0) + 1
        
        return {
            "orphaned_tasks": len(orphaned),
            "by_worker": by_worker,
            "by_queue": by_queue
        }


# Global task recovery instance
task_recovery = TaskRecovery()

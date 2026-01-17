"""
Job queue implementation with priority and rate limiting.
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from jobqueue.core.task import Task, TaskStatus, TaskPriority
from jobqueue.core.task_cancellation import task_cancellation, CancellationReason
from jobqueue.core.event_publisher import event_publisher
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.backend.postgres_backend import postgres_backend
from jobqueue.utils.logger import log
from config import settings


class JobQueue:
    """
    Main job queue class that manages task submission and retrieval.
    """
    
    def __init__(self, name: str = "default"):
        """
        Initialize job queue.
        
        Args:
            name: Queue name
        """
        self.name = name
        self.rate_limits = {
            TaskPriority.HIGH: settings.rate_limit_high,
            TaskPriority.MEDIUM: settings.rate_limit_medium,
            TaskPriority.LOW: settings.rate_limit_low,
        }
        
        log.info(f"Initialized job queue: {name}")
    
    def submit_task(
        self,
        task_name: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.MEDIUM,
        max_retries: Optional[int] = None,
        timeout: Optional[int] = None,
        depends_on: Optional[List[str]] = None,
    ) -> Task:
        """
        Submit a new task to the queue.
        
        Args:
            task_name: Name of the task to execute
            args: Positional arguments for the task
            kwargs: Keyword arguments for the task
            priority: Task priority level
            max_retries: Maximum number of retries
            timeout: Task timeout in seconds
            depends_on: List of task IDs this task depends on
            
        Returns:
            Created Task object
        """
        task = Task(
            name=task_name,
            priority=priority,
            queue_name=self.name,
            args=args or [],
            kwargs=kwargs or {},
            max_retries=max_retries or settings.max_retries,
            timeout=timeout or settings.task_timeout,
            depends_on=depends_on or [],
        )
        
        # Save task to database
        self._save_task_to_db(task)
        
        # Push to Redis queue if ready (no dependencies)
        if task.is_ready():
            task.status = TaskStatus.QUEUED
            self._push_to_redis(task)
            
            # Publish event
            event_publisher.publish_task_enqueued(
                task_id=task.id,
                task_name=task.name,
                queue_name=task.queue_name,
                priority=task.priority.value if task.priority else "medium"
            )
            
            log.info(f"Task {task.id} queued", extra={"task_id": task.id, "task_name": task_name})
        else:
            log.info(
                f"Task {task.id} pending dependencies",
                extra={"task_id": task.id, "depends_on": task.depends_on}
            )
        
        return task
    
    def _save_task_to_db(self, task: Task) -> None:
        """Save task to PostgreSQL database."""
        query = """
        INSERT INTO tasks (
            id, name, priority, status, queue_name, args, kwargs,
            retry_count, max_retries, timeout, created_at,
            parent_task_id, depends_on
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        task_dict = task.to_dict()
        params = (
            task_dict["id"],
            task_dict["name"],
            task_dict["priority"],
            task_dict["status"],
            task_dict["queue_name"],
            task_dict["args"],
            task_dict["kwargs"],
            task_dict["retry_count"],
            task_dict["max_retries"],
            task_dict["timeout"],
            task_dict["created_at"],
            task_dict["parent_task_id"],
            task_dict["depends_on"],
        )
        
        postgres_backend.execute_query(query, params)
    
    def _push_to_redis(self, task: Task) -> None:
        """Push task to Redis queue."""
        queue_key = task.get_queue_key()
        task_json = task.to_json()
        redis_broker.push_task(queue_key, task_json)
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """
        Retrieve a task by ID from the database.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task object or None if not found
        """
        query = "SELECT * FROM tasks WHERE id = %s"
        result = postgres_backend.execute_query(query, (task_id,), fetch_one=True)
        
        if result:
            return Task.from_dict(dict(result))
        return None
    
    def update_task(self, task: Task) -> None:
        """
        Update task in the database.
        
        Args:
            task: Task object to update
        """
        query = """
        UPDATE tasks SET
            status = %s,
            result = %s,
            error = %s,
            retry_count = %s,
            started_at = %s,
            completed_at = %s,
            worker_id = %s
        WHERE id = %s
        """
        
        task_dict = task.to_dict()
        params = (
            task_dict["status"],
            task_dict["result"],
            task_dict["error"],
            task_dict["retry_count"],
            task_dict["started_at"],
            task_dict["completed_at"],
            task_dict["worker_id"],
            task_dict["id"],
        )
        
        postgres_backend.execute_query(query, params)
    
    def get_queue_size(self, priority: Optional[TaskPriority] = None) -> int:
        """
        Get the current size of the queue.
        
        Args:
            priority: Optional priority level to check specific queue
            
        Returns:
            Number of tasks in queue
        """
        if priority:
            queue_key = f"queue:{self.name}:{priority}"
            return redis_broker.get_queue_length(queue_key)
        
        # Get total across all priorities
        total = 0
        for p in TaskPriority:
            queue_key = f"queue:{self.name}:{p}"
            total += redis_broker.get_queue_length(queue_key)
        
        return total
    
    def cancel_task(
        self,
        task_id: str,
        reason: CancellationReason = CancellationReason.USER_REQUESTED,
        force: bool = False
    ) -> bool:
        """
        Cancel a pending or running task.
        
        Args:
            task_id: Task ID to cancel
            reason: Cancellation reason
            force: If True, force kill running tasks
            
        Returns:
            True if successfully cancelled
        """
        task = self.get_task(task_id)
        
        if not task:
            log.warning(f"Task {task_id} not found")
            return False
        
        if task.status in [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            log.warning(
                f"Task {task_id} already completed with status {task.status.value}",
                extra={"task_id": task_id, "status": task.status.value}
            )
            return False
        
        # Use cancellation system
        success = task_cancellation.cancel_task(task, reason=reason, force=force)
        
        if success:
            # Update task in database
            self.update_task(task)
            
            log.info(
                f"Task {task_id} cancelled",
                extra={
                    "task_id": task_id,
                    "status": task.status.value,
                    "reason": reason.value,
                    "force": force
                }
            )
        
        return success
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the queue.
        
        Returns:
            Dictionary with queue statistics
        """
        stats = {
            "queue_name": self.name,
            "queued_by_priority": {},
            "total_queued": 0,
            "status_counts": {},
        }
        
        # Get queued counts by priority
        for priority in TaskPriority:
            count = self.get_queue_size(priority)
            stats["queued_by_priority"][priority] = count
            stats["total_queued"] += count
        
        # Get task counts by status from database
        query = """
        SELECT status, COUNT(*) as count
        FROM tasks
        WHERE queue_name = %s
        GROUP BY status
        """
        results = postgres_backend.execute_query(query, (self.name,), fetch_all=True)
        
        if results:
            for row in results:
                stats["status_counts"][row["status"]] = row["count"]
        
        return stats

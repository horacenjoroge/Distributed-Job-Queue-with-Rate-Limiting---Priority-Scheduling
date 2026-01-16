"""
Dead Letter Queue (DLQ) for failed tasks.
Stores tasks that have exceeded maximum retry attempts.
"""
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.utils.logger import log


class DeadLetterQueue:
    """
    Dead Letter Queue for storing permanently failed tasks.
    """
    
    def __init__(self, queue_name: str = "dead_letter_queue"):
        """
        Initialize Dead Letter Queue.
        
        Args:
            queue_name: Name of the DLQ (default: "dead_letter_queue")
        """
        self.queue_name = queue_name
        self.key = f"dlq:{queue_name}"
        
        log.info(f"Initialized Dead Letter Queue: {queue_name}")
    
    def add_task(
        self,
        task: Task,
        failure_reason: str,
        stack_trace: Optional[str] = None
    ) -> bool:
        """
        Add a failed task to the Dead Letter Queue.
        
        Args:
            task: Failed task
            failure_reason: Reason for failure
            stack_trace: Stack trace (optional)
            
        Returns:
            True if added successfully
        """
        # Create DLQ entry with metadata
        dlq_entry = {
            "task": task.to_json(),
            "failure_reason": failure_reason,
            "stack_trace": stack_trace,
            "added_at": datetime.utcnow().isoformat(),
            "retry_count": task.retry_count,
            "max_retries": task.max_retries,
            "task_id": task.id,
            "task_name": task.name,
            "queue_name": task.queue_name,
            "error": task.error,
            "retry_history": task.retry_history
        }
        
        # Store in Redis list
        import json
        entry_json = json.dumps(dlq_entry)
        redis_broker.client.lpush(self.key, entry_json)
        
        log.error(
            f"Task {task.id} added to Dead Letter Queue",
            extra={
                "task_id": task.id,
                "task_name": task.name,
                "failure_reason": failure_reason,
                "retry_count": task.retry_count
            }
        )
        
        return True
    
    def get_tasks(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get tasks from Dead Letter Queue.
        
        Args:
            limit: Maximum number of tasks to retrieve
            offset: Offset for pagination
            
        Returns:
            List of DLQ entries
        """
        # Get range of entries
        entries = redis_broker.client.lrange(self.key, offset, offset + limit - 1)
        
        tasks = []
        for entry_json in entries:
            try:
                import json
                entry = json.loads(entry_json)
                tasks.append(entry)
            except Exception as e:
                log.error(f"Failed to parse DLQ entry: {e}")
        
        return tasks
    
    def get_task_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific task from DLQ by ID.
        
        Args:
            task_id: Task ID
            
        Returns:
            DLQ entry or None
        """
        all_tasks = self.get_tasks(limit=10000)  # Get all tasks
        
        for entry in all_tasks:
            if entry.get("task_id") == task_id:
                return entry
        
        return None
    
    def size(self) -> int:
        """
        Get number of tasks in Dead Letter Queue.
        
        Returns:
            Number of tasks
        """
        return redis_broker.client.llen(self.key)
    
    def purge(self) -> int:
        """
        Remove all tasks from Dead Letter Queue.
        
        Returns:
            Number of tasks purged
        """
        count = self.size()
        
        if count > 0:
            redis_broker.client.delete(self.key)
            log.info(f"Purged {count} tasks from Dead Letter Queue")
        
        return count
    
    def remove_task(self, task_id: str) -> bool:
        """
        Remove a specific task from DLQ.
        
        Args:
            task_id: Task ID to remove
            
        Returns:
            True if removed
        """
        all_entries = redis_broker.client.lrange(self.key, 0, -1)
        
        for entry_json in all_entries:
            try:
                import json
                entry = json.loads(entry_json)
                
                if entry.get("task_id") == task_id:
                    # Remove this entry
                    redis_broker.client.lrem(self.key, 1, entry_json)
                    log.info(f"Removed task {task_id} from Dead Letter Queue")
                    return True
            except Exception as e:
                log.error(f"Error processing DLQ entry: {e}")
        
        return False
    
    def retry_task(
        self,
        task_id: str,
        reset_retry_count: bool = True
    ) -> Optional[Task]:
        """
        Retry a task from Dead Letter Queue.
        Removes from DLQ and re-enqueues for execution.
        
        Args:
            task_id: Task ID to retry
            reset_retry_count: Reset retry_count to 0 (default: True)
            
        Returns:
            Task if retried, None if not found
        """
        entry = self.get_task_by_id(task_id)
        
        if not entry:
            log.warning(f"Task {task_id} not found in Dead Letter Queue")
            return None
        
        # Deserialize task
        task = Task.from_json(entry["task"])
        
        # Reset retry count if requested
        if reset_retry_count:
            task.retry_count = 0
            task.retry_history = []
        
        # Reset status
        task.status = TaskStatus.PENDING
        task.error = None
        task.started_at = None
        task.completed_at = None
        
        # Remove from DLQ
        self.remove_task(task_id)
        
        # Re-enqueue task
        from jobqueue.core.redis_queue import Queue
        from jobqueue.core.priority_queue import PriorityQueue
        
        if task.priority:
            queue = PriorityQueue(task.queue_name)
        else:
            queue = Queue(task.queue_name)
        
        queue.enqueue(task)
        
        log.info(
            f"Retried task {task_id} from Dead Letter Queue",
            extra={
                "task_id": task_id,
                "reset_retry_count": reset_retry_count
            }
        )
        
        return task
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get Dead Letter Queue statistics.
        
        Returns:
            Dictionary with DLQ stats
        """
        size = self.size()
        
        # Get sample of tasks for analysis
        sample = self.get_tasks(limit=100)
        
        # Count by task name
        by_task_name = {}
        for entry in sample:
            task_name = entry.get("task_name", "unknown")
            by_task_name[task_name] = by_task_name.get(task_name, 0) + 1
        
        # Count by queue
        by_queue = {}
        for entry in sample:
            queue_name = entry.get("queue_name", "unknown")
            by_queue[queue_name] = by_queue.get(queue_name, 0) + 1
        
        # Get oldest and newest entries
        oldest_entry = None
        newest_entry = None
        
        if sample:
            oldest_entry = sample[-1]  # Last in list (oldest)
            newest_entry = sample[0]    # First in list (newest)
        
        return {
            "queue_name": self.queue_name,
            "size": size,
            "by_task_name": by_task_name,
            "by_queue": by_queue,
            "oldest_entry": oldest_entry.get("added_at") if oldest_entry else None,
            "newest_entry": newest_entry.get("added_at") if newest_entry else None
        }
    
    def check_alert_threshold(self, threshold: int = 100) -> tuple[bool, int]:
        """
        Check if DLQ size exceeds threshold.
        
        Args:
            threshold: Alert threshold (default: 100)
            
        Returns:
            Tuple of (exceeds_threshold, current_size)
        """
        size = self.size()
        exceeds = size >= threshold
        
        if exceeds:
            log.warning(
                f"Dead Letter Queue size ({size}) exceeds threshold ({threshold})",
                extra={
                    "dlq_size": size,
                    "threshold": threshold
                }
            )
        
        return exceeds, size


# Global Dead Letter Queue instance
dead_letter_queue = DeadLetterQueue()


def add_to_dlq(
    task: Task,
    failure_reason: str,
    exception: Optional[Exception] = None
) -> bool:
    """
    Helper function to add task to Dead Letter Queue.
    
    Args:
        task: Failed task
        failure_reason: Reason for failure
        exception: Exception object (for stack trace)
        
    Returns:
        True if added
    """
    stack_trace = None
    if exception:
        stack_trace = "".join(traceback.format_exception(
            type(exception),
            exception,
            exception.__traceback__
        ))
    
    return dead_letter_queue.add_task(task, failure_reason, stack_trace)

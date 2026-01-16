"""
Scheduled task storage and management using Redis sorted sets.
"""
import time
from typing import List, Optional
from datetime import datetime, timezone
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task
from jobqueue.utils.logger import log


class ScheduledTaskStore:
    """
    Manages scheduled tasks using Redis sorted sets.
    Tasks are stored with their execution timestamp as the score.
    """
    
    def __init__(self, key_prefix: str = "scheduled"):
        """
        Initialize scheduled task store.
        
        Args:
            key_prefix: Redis key prefix for scheduled tasks
        """
        self.key_prefix = key_prefix
        
        log.info(f"Initialized scheduled task store with prefix: {key_prefix}")
    
    def _get_key(self, queue_name: str) -> str:
        """
        Get Redis key for scheduled tasks in a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Redis key
        """
        return f"{self.key_prefix}:{queue_name}"
    
    def schedule_task(self, task: Task) -> bool:
        """
        Schedule a task for future execution.
        
        Args:
            task: Task to schedule
            
        Returns:
            True if scheduled successfully
        """
        if not task.schedule_time:
            # Calculate schedule time if not set
            task.set_schedule_time()
        
        if not task.schedule_time:
            log.warning(f"Task {task.id} has no schedule time, cannot schedule")
            return False
        
        key = self._get_key(task.queue_name)
        
        # Use timestamp as score for sorted set
        score = task.schedule_time.timestamp()
        
        # Store task as JSON
        task_json = task.to_json()
        
        # Add to sorted set
        redis_broker.client.zadd(key, {task_json: score})
        
        log.info(
            f"Scheduled task {task.id} for execution",
            extra={
                "task_id": task.id,
                "queue": task.queue_name,
                "schedule_time": task.schedule_time.isoformat(),
                "score": score
            }
        )
        
        return True
    
    def get_ready_tasks(
        self,
        queue_name: str,
        limit: int = 100
    ) -> List[Task]:
        """
        Get tasks that are ready to execute (schedule_time <= now).
        
        Args:
            queue_name: Queue name
            limit: Maximum number of tasks to retrieve
            
        Returns:
            List of ready tasks
        """
        key = self._get_key(queue_name)
        current_time = datetime.utcnow().timestamp()
        
        # Get tasks with score <= current_time
        task_jsons = redis_broker.client.zrangebyscore(
            key,
            0,
            current_time,
            start=0,
            num=limit
        )
        
        tasks = []
        for task_json in task_jsons:
            try:
                task = Task.from_json(task_json)
                tasks.append(task)
            except Exception as e:
                log.error(f"Failed to deserialize scheduled task: {e}")
        
        if tasks:
            log.debug(
                f"Found {len(tasks)} ready tasks in {queue_name}",
                extra={"queue": queue_name, "count": len(tasks)}
            )
        
        return tasks
    
    def remove_task(self, task: Task) -> bool:
        """
        Remove a task from the scheduled set.
        
        Args:
            task: Task to remove
            
        Returns:
            True if removed
        """
        key = self._get_key(task.queue_name)
        task_json = task.to_json()
        
        removed = redis_broker.client.zrem(key, task_json)
        
        if removed:
            log.debug(f"Removed scheduled task {task.id} from {task.queue_name}")
        
        return bool(removed)
    
    def get_scheduled_count(self, queue_name: str) -> int:
        """
        Get count of scheduled tasks in a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Number of scheduled tasks
        """
        key = self._get_key(queue_name)
        return redis_broker.client.zcard(key)
    
    def get_next_scheduled_time(self, queue_name: str) -> Optional[datetime]:
        """
        Get the next scheduled execution time for a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Next execution time, or None if no scheduled tasks
        """
        key = self._get_key(queue_name)
        
        # Get task with lowest score (earliest time)
        results = redis_broker.client.zrange(key, 0, 0, withscores=True)
        
        if not results:
            return None
        
        _, score = results[0]
        return datetime.fromtimestamp(score, tz=timezone.utc).replace(tzinfo=None)
    
    def peek_scheduled_tasks(
        self,
        queue_name: str,
        limit: int = 10
    ) -> List[tuple[Task, datetime]]:
        """
        Peek at upcoming scheduled tasks without removing them.
        
        Args:
            queue_name: Queue name
            limit: Maximum number to peek
            
        Returns:
            List of (task, scheduled_time) tuples
        """
        key = self._get_key(queue_name)
        
        # Get tasks with scores
        results = redis_broker.client.zrange(key, 0, limit - 1, withscores=True)
        
        tasks = []
        for task_json, score in results:
            try:
                task = Task.from_json(task_json)
                scheduled_time = datetime.fromtimestamp(score, tz=timezone.utc).replace(tzinfo=None)
                tasks.append((task, scheduled_time))
            except Exception as e:
                log.error(f"Failed to deserialize scheduled task: {e}")
        
        return tasks
    
    def cancel_scheduled_task(self, task_id: str, queue_name: str) -> bool:
        """
        Cancel a scheduled task by ID.
        
        Args:
            task_id: Task ID
            queue_name: Queue name
            
        Returns:
            True if cancelled
        """
        key = self._get_key(queue_name)
        
        # Get all tasks and find matching ID
        all_tasks = redis_broker.client.zrange(key, 0, -1)
        
        for task_json in all_tasks:
            try:
                task = Task.from_json(task_json)
                if task.id == task_id:
                    removed = redis_broker.client.zrem(key, task_json)
                    if removed:
                        log.info(f"Cancelled scheduled task {task_id}")
                        return True
            except Exception as e:
                log.error(f"Error checking task: {e}")
        
        return False
    
    def purge_scheduled_tasks(self, queue_name: str) -> int:
        """
        Remove all scheduled tasks from a queue.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Number of tasks removed
        """
        key = self._get_key(queue_name)
        count = redis_broker.client.zcard(key)
        
        if count > 0:
            redis_broker.client.delete(key)
            log.info(f"Purged {count} scheduled tasks from {queue_name}")
        
        return count
    
    def get_stats(self, queue_name: str) -> dict:
        """
        Get statistics about scheduled tasks.
        
        Args:
            queue_name: Queue name
            
        Returns:
            Dictionary with stats
        """
        total_scheduled = self.get_scheduled_count(queue_name)
        next_time = self.get_next_scheduled_time(queue_name)
        
        # Count ready tasks
        current_time = datetime.utcnow().timestamp()
        key = self._get_key(queue_name)
        ready_count = redis_broker.client.zcount(key, 0, current_time)
        
        return {
            "queue": queue_name,
            "total_scheduled": total_scheduled,
            "ready_to_execute": ready_count,
            "waiting": total_scheduled - ready_count,
            "next_execution_time": next_time.isoformat() if next_time else None
        }


# Global scheduled task store instance
scheduled_task_store = ScheduledTaskStore()

"""
Priority queue implementation using Redis sorted sets.
"""
import time
from typing import Optional, List
from jobqueue.core.task import Task, TaskPriority
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class PriorityQueue:
    """
    Priority queue using Redis sorted sets.
    Tasks are ordered by priority and timestamp for FIFO within same priority.
    """
    
    # Priority weights (higher = more important)
    PRIORITY_WEIGHTS = {
        TaskPriority.HIGH: 1000,
        TaskPriority.MEDIUM: 500,
        TaskPriority.LOW: 100
    }
    
    def __init__(self, name: str = "default"):
        """
        Initialize priority queue.
        
        Args:
            name: Queue name
            
        Example:
            queue = PriorityQueue("default")
        """
        self.name = name
        self._queue_key = f"priority_queue:{name}"
        
        log.info(f"Initialized priority queue: {name}")
    
    @property
    def queue_key(self) -> str:
        """Get the Redis key for this queue."""
        return self._queue_key
    
    def _calculate_score(
        self,
        priority: TaskPriority,
        timestamp: Optional[float] = None,
        age_boost: float = 0.0
    ) -> float:
        """
        Calculate priority score for sorted set.
        Score combines priority weight and timestamp for FIFO ordering.
        
        Formula: score = priority_weight - (timestamp / 1000000) + age_boost
        
        The division by 1000000 ensures timestamp doesn't overwhelm priority.
        Earlier tasks get slightly higher scores within same priority (FIFO).
        
        Args:
            priority: Task priority level
            timestamp: Task creation timestamp (defaults to current time)
            age_boost: Additional boost for aged tasks (starvation prevention)
            
        Returns:
            Score for sorted set (higher score = higher priority)
            
        Example:
            HIGH task at t=1000: score = 1000 - (1000/1000000) = 999.999
            HIGH task at t=2000: score = 1000 - (2000/1000000) = 999.998
            (First task has higher score, maintaining FIFO)
        """
        if timestamp is None:
            timestamp = time.time()
        
        priority_weight = self.PRIORITY_WEIGHTS.get(
            priority,
            self.PRIORITY_WEIGHTS[TaskPriority.MEDIUM]
        )
        
        # Normalize timestamp to small value to maintain priority ordering
        # while preserving FIFO within same priority
        normalized_timestamp = timestamp / 1000000.0
        
        # Calculate score: priority - timestamp (earlier = higher) + age boost
        score = priority_weight - normalized_timestamp + age_boost
        
        log.debug(
            f"Calculated score: {score}",
            extra={
                "priority": priority,
                "priority_weight": priority_weight,
                "timestamp": timestamp,
                "age_boost": age_boost
            }
        )
        
        return score
    
    def recalculate_score(self, task_id: str) -> bool:
        """
        Recalculate and update score for a task (for priority adjustment).
        
        Args:
            task_id: Task ID to update
            
        Returns:
            True if task was found and updated
        """
        # Get all tasks to find the one to update
        all_tasks_data = redis_broker.client.zrange(
            self._queue_key,
            0, -1,
            withscores=True
        )
        
        for task_json, old_score in all_tasks_data:
            task = Task.from_json(task_json)
            if task.id == task_id:
                # Calculate new score
                new_score = self._calculate_score(task.priority, task.created_at.timestamp())
                
                # Update in sorted set
                redis_broker.client.zadd(self._queue_key, {task_json: new_score})
                
                log.info(
                    f"Recalculated score for task {task_id}",
                    extra={
                        "task_id": task_id,
                        "old_score": old_score,
                        "new_score": new_score
                    }
                )
                
                return True
        
        return False
    
    def enqueue(self, task: Task) -> str:
        """
        Add a task to the priority queue.
        
        Args:
            task: Task to enqueue
            
        Returns:
            Task ID
            
        Example:
            task = Task(name="process", priority=TaskPriority.HIGH)
            queue.enqueue(task)
        """
        task_json = task.to_json()
        score = self._calculate_score(task.priority, task.created_at.timestamp())
        
        # Add to sorted set with score
        redis_broker.client.zadd(self._queue_key, {task_json: score})
        
        log.debug(
            f"Enqueued task to priority queue {self.name}",
            extra={
                "task_id": task.id,
                "priority": task.priority,
                "score": score
            }
        )
        
        return task.id
    
    def dequeue(self, timeout: int = 0) -> Optional[Task]:
        """
        Remove and return highest priority task.
        Uses BZPOPMAX for blocking operation.
        
        Args:
            timeout: Blocking timeout in seconds (0 = block indefinitely)
            
        Returns:
            Highest priority task or None if timeout
            
        Example:
            task = queue.dequeue(timeout=5)
        """
        if timeout == 0:
            # Blocking pop with highest score (highest priority)
            result = redis_broker.client.bzpopmax(self._queue_key, timeout=0)
        else:
            result = redis_broker.client.bzpopmax(self._queue_key, timeout=timeout)
        
        if result:
            _, task_json, score = result
            task = Task.from_json(task_json)
            
            log.debug(
                f"Dequeued task from priority queue {self.name}",
                extra={
                    "task_id": task.id,
                    "priority": task.priority,
                    "score": score
                }
            )
            
            return task
        
        return None
    
    def dequeue_nowait(self) -> Optional[Task]:
        """
        Remove and return highest priority task without blocking.
        
        Returns:
            Highest priority task or None if queue empty
        """
        # Pop max (highest score = highest priority)
        result = redis_broker.client.zpopmax(self._queue_key, count=1)
        
        if result:
            task_json, score = result[0]
            task = Task.from_json(task_json)
            
            log.debug(
                f"Dequeued task from priority queue {self.name} (non-blocking)",
                extra={
                    "task_id": task.id,
                    "priority": task.priority,
                    "score": score
                }
            )
            
            return task
        
        return None
    
    def size(self) -> int:
        """
        Get the current size of the queue.
        
        Returns:
            Number of tasks in the queue
        """
        size = redis_broker.client.zcard(self._queue_key)
        
        log.debug(
            f"Priority queue {self.name} size: {size}",
            extra={"queue": self.name, "size": size}
        )
        
        return size
    
    def purge(self) -> int:
        """
        Remove all tasks from the queue.
        
        Returns:
            Number of tasks removed
        """
        size = self.size()
        
        if size > 0:
            redis_broker.client.delete(self._queue_key)
            
            log.info(
                f"Purged priority queue {self.name}",
                extra={"queue": self.name, "tasks_removed": size}
            )
        
        return size
    
    def peek(self) -> Optional[Task]:
        """
        View highest priority task without removing it.
        
        Returns:
            Highest priority task or None if empty
        """
        # Get max without removing
        result = redis_broker.client.zrange(
            self._queue_key,
            -1, -1,  # Last element (highest score)
            withscores=True
        )
        
        if result:
            task_json, score = result[0]
            task = Task.from_json(task_json)
            
            log.debug(
                f"Peeked at task in priority queue {self.name}",
                extra={"task_id": task.id, "priority": task.priority}
            )
            
            return task
        
        return None
    
    def get_by_priority(self, priority: TaskPriority) -> List[Task]:
        """
        Get all tasks of a specific priority without removing them.
        
        Args:
            priority: Priority level to filter
            
        Returns:
            List of tasks with specified priority
        """
        # Get all tasks
        all_tasks_data = redis_broker.client.zrange(
            self._queue_key,
            0, -1,
            withscores=True
        )
        
        tasks = []
        for task_json, score in all_tasks_data:
            task = Task.from_json(task_json)
            if task.priority == priority:
                tasks.append(task)
        
        return tasks
    
    def size_by_priority(self) -> dict:
        """
        Get queue size broken down by priority.
        
        Returns:
            Dictionary with counts per priority level
        """
        all_tasks_data = redis_broker.client.zrange(self._queue_key, 0, -1)
        
        counts = {
            TaskPriority.HIGH: 0,
            TaskPriority.MEDIUM: 0,
            TaskPriority.LOW: 0
        }
        
        for task_json in all_tasks_data:
            task = Task.from_json(task_json)
            counts[task.priority] = counts.get(task.priority, 0) + 1
        
        return counts
    
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return self.size() == 0
    
    def __repr__(self) -> str:
        """String representation."""
        return f"PriorityQueue(name='{self.name}', size={self.size()})"
    
    def __len__(self) -> int:
        """Allow len() to be called on queue."""
        return self.size()

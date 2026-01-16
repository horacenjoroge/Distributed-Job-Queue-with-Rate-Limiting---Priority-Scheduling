"""
Redis-based FIFO queue implementation using LIST data structure.
"""
from typing import Optional
from jobqueue.core.task import Task
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class Queue:
    """
    FIFO queue implementation using Redis LIST.
    Uses LPUSH for enqueue and RPOP/BRPOP for dequeue.
    """
    
    def __init__(self, name: str = "default"):
        """
        Initialize queue.
        
        Args:
            name: Queue name (e.g., 'default', 'high_priority')
        """
        self.name = name
        self._queue_key = f"queue:{name}"
        
        log.info(f"Initialized Redis queue: {self.name}")
    
    @property
    def queue_key(self) -> str:
        """Get the Redis key for this queue."""
        return self._queue_key
    
    def enqueue(self, task: Task) -> str:
        """
        Add a task to the queue (FIFO).
        Uses LPUSH to add to the left side of the list.
        
        Args:
            task: Task to enqueue
            
        Returns:
            Task ID
            
        Example:
            queue = Queue("default")
            task = Task(name="process_data", args=[1, 2, 3])
            task_id = queue.enqueue(task)
        """
        task_json = task.to_json()
        
        # LPUSH adds to the left (head) of the list
        redis_broker.client.lpush(self._queue_key, task_json)
        
        log.debug(
            f"Enqueued task to {self.name}",
            extra={"task_id": task.id, "queue": self.name}
        )
        
        return task.id
    
    def dequeue(self, timeout: int = 0) -> Optional[Task]:
        """
        Remove and return a task from the queue (FIFO).
        Uses BRPOP (blocking) to pop from the right side of the list.
        
        Args:
            timeout: Blocking timeout in seconds (0 = block indefinitely)
            
        Returns:
            Task object or None if timeout
            
        Example:
            queue = Queue("default")
            task = queue.dequeue(timeout=5)  # Wait up to 5 seconds
            if task:
                print(f"Got task: {task.id}")
        """
        if timeout == 0:
            # Blocking pop from right (tail) of the list
            result = redis_broker.client.brpop(self._queue_key, timeout=0)
        else:
            # Blocking with timeout
            result = redis_broker.client.brpop(self._queue_key, timeout=timeout)
        
        if result:
            _, task_json = result
            task = Task.from_json(task_json)
            
            log.debug(
                f"Dequeued task from {self.name}",
                extra={"task_id": task.id, "queue": self.name}
            )
            
            return task
        
        return None
    
    def dequeue_nowait(self) -> Optional[Task]:
        """
        Remove and return a task without blocking.
        Uses RPOP (non-blocking).
        
        Returns:
            Task object or None if queue is empty
            
        Example:
            queue = Queue("default")
            task = queue.dequeue_nowait()
            if task:
                print(f"Got task: {task.id}")
            else:
                print("Queue is empty")
        """
        task_json = redis_broker.client.rpop(self._queue_key)
        
        if task_json:
            task = Task.from_json(task_json)
            
            log.debug(
                f"Dequeued task from {self.name} (non-blocking)",
                extra={"task_id": task.id, "queue": self.name}
            )
            
            return task
        
        return None
    
    def size(self) -> int:
        """
        Get the current size of the queue.
        Uses LLEN to get list length.
        
        Returns:
            Number of tasks in the queue
            
        Example:
            queue = Queue("default")
            print(f"Queue size: {queue.size()}")
        """
        size = redis_broker.client.llen(self._queue_key)
        
        log.debug(
            f"Queue {self.name} size: {size}",
            extra={"queue": self.name, "size": size}
        )
        
        return size
    
    def purge(self) -> int:
        """
        Remove all tasks from the queue.
        Deletes the Redis key.
        
        Returns:
            Number of tasks that were in the queue
            
        Example:
            queue = Queue("default")
            removed = queue.purge()
            print(f"Removed {removed} tasks")
        """
        size = self.size()
        
        if size > 0:
            redis_broker.client.delete(self._queue_key)
            
            log.info(
                f"Purged queue {self.name}",
                extra={"queue": self.name, "tasks_removed": size}
            )
        
        return size
    
    def peek(self, index: int = -1) -> Optional[Task]:
        """
        View a task without removing it from the queue.
        Uses LINDEX to access by index.
        
        Args:
            index: Index to peek at (-1 = next task to dequeue, 0 = last enqueued)
            
        Returns:
            Task object or None if index out of range
            
        Example:
            queue = Queue("default")
            next_task = queue.peek()  # View next task without removing
        """
        task_json = redis_broker.client.lindex(self._queue_key, index)
        
        if task_json:
            task = Task.from_json(task_json)
            
            log.debug(
                f"Peeked at task in {self.name}",
                extra={"task_id": task.id, "queue": self.name, "index": index}
            )
            
            return task
        
        return None
    
    def is_empty(self) -> bool:
        """
        Check if the queue is empty.
        
        Returns:
            True if empty, False otherwise
            
        Example:
            queue = Queue("default")
            if queue.is_empty():
                print("No tasks to process")
        """
        return self.size() == 0
    
    def get_all_tasks(self) -> list[Task]:
        """
        Get all tasks in the queue without removing them.
        Uses LRANGE to get all elements.
        
        Returns:
            List of all tasks in the queue (in order)
            
        Example:
            queue = Queue("default")
            all_tasks = queue.get_all_tasks()
            for task in all_tasks:
                print(task.name)
        """
        task_jsons = redis_broker.client.lrange(self._queue_key, 0, -1)
        
        tasks = []
        for task_json in task_jsons:
            try:
                task = Task.from_json(task_json)
                tasks.append(task)
            except Exception as e:
                log.error(f"Failed to parse task from queue: {e}")
        
        # Reverse to show in dequeue order (FIFO)
        tasks.reverse()
        
        return tasks
    
    def remove_task(self, task_id: str) -> bool:
        """
        Remove a specific task from the queue by ID.
        Uses LREM to remove by value.
        
        Args:
            task_id: Task ID to remove
            
        Returns:
            True if task was removed, False if not found
            
        Example:
            queue = Queue("default")
            success = queue.remove_task("task-123")
        """
        # Get all tasks to find the matching one
        all_tasks = self.get_all_tasks()
        
        for task in all_tasks:
            if task.id == task_id:
                task_json = task.to_json()
                # LREM removes elements matching the value
                removed = redis_broker.client.lrem(self._queue_key, 0, task_json)
                
                if removed > 0:
                    log.info(
                        f"Removed task {task_id} from queue {self.name}",
                        extra={"task_id": task_id, "queue": self.name}
                    )
                    return True
        
        log.warning(
            f"Task {task_id} not found in queue {self.name}",
            extra={"task_id": task_id, "queue": self.name}
        )
        return False
    
    def clear_failed_tasks(self) -> int:
        """
        Remove all failed or invalid tasks from the queue.
        
        Returns:
            Number of tasks removed
        """
        removed_count = 0
        task_jsons = redis_broker.client.lrange(self._queue_key, 0, -1)
        
        for task_json in task_jsons:
            try:
                Task.from_json(task_json)
            except Exception as e:
                # Invalid task, remove it
                redis_broker.client.lrem(self._queue_key, 0, task_json)
                removed_count += 1
                log.warning(f"Removed invalid task from queue: {e}")
        
        if removed_count > 0:
            log.info(
                f"Cleared {removed_count} invalid tasks from {self.name}",
                extra={"queue": self.name, "removed": removed_count}
            )
        
        return removed_count
    
    def __repr__(self) -> str:
        """String representation of the queue."""
        return f"Queue(name='{self.name}', size={self.size()})"
    
    def __len__(self) -> int:
        """Allow len() to be called on queue."""
        return self.size()

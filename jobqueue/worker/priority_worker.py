"""
Priority-aware worker that processes tasks by priority.
"""
import time
import signal
import sys
from typing import Optional
from datetime import datetime
from jobqueue.worker.base_worker import Worker
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task import Task, TaskStatus
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task_registry import task_registry
from jobqueue.core.async_support import execute_task_sync_or_async
from jobqueue.utils.logger import log
from config import settings


class PriorityWorker(Worker):
    """
    Worker that processes tasks from a priority queue.
    Always processes highest priority tasks first.
    """
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        queue_name: str = "default",
        poll_timeout: int = 5
    ):
        """
        Initialize priority worker.
        
        Args:
            worker_id: Unique worker identifier
            queue_name: Queue to process tasks from
            poll_timeout: Timeout for blocking queue operations
            
        Example:
            worker = PriorityWorker(queue_name="default")
            worker.start()
        """
        super().__init__(worker_id, queue_name)
        self.poll_timeout = poll_timeout
        self.priority_queue = None
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.current_task = None
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        log.info(
            f"PriorityWorker initialized",
            extra={
                "worker_id": self.worker_id,
                "queue": queue_name,
                "poll_timeout": poll_timeout
            }
        )
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        log.debug("Signal handlers registered")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        
        log.info(
            f"Worker {self.worker_id} received {signal_name}, shutting down...",
            extra={"signal": signal_name}
        )
        
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the worker and connect to Redis."""
        if not redis_broker.is_connected():
            redis_broker.connect()
            log.info(f"Worker {self.worker_id} connected to Redis")
        
        # Initialize priority queue
        self.priority_queue = PriorityQueue(self.queue_name)
        
        # Start processing
        super().start()
    
    def stop(self):
        """Stop the worker gracefully."""
        log.info(f"Gracefully stopping worker {self.worker_id}")
        
        if self.current_task:
            log.info(
                f"Waiting for current task {self.current_task.id} to complete",
                extra={"task_id": self.current_task.id}
            )
        
        super().stop()
        
        if redis_broker.is_connected():
            redis_broker.disconnect()
            log.info(f"Worker {self.worker_id} disconnected")
        
        log.info(
            f"Worker {self.worker_id} stopped",
            extra={
                "tasks_processed": self.tasks_processed,
                "tasks_failed": self.tasks_failed
            }
        )
    
    def run_loop(self):
        """Main worker loop - processes tasks by priority."""
        log.info(f"Worker {self.worker_id} entering priority-aware loop")
        
        while self.is_running:
            try:
                # Pull highest priority task from queue
                task = self.priority_queue.dequeue(timeout=self.poll_timeout)
                
                if task is None:
                    log.debug(
                        f"Worker {self.worker_id} waiting for tasks...",
                        extra={"queue": self.queue_name}
                    )
                    continue
                
                # Log priority information
                log.info(
                    f"Worker {self.worker_id} received task",
                    extra={
                        "task_id": task.id,
                        "task_name": task.name,
                        "priority": task.priority
                    }
                )
                
                # Process the task
                self.current_task = task
                self.process_task(task)
                self.current_task = None
                self.tasks_processed += 1
                
            except Exception as e:
                log.error(f"Error in worker loop: {e}")
                time.sleep(1)
        
        log.info(
            f"Worker {self.worker_id} exited loop",
            extra={"tasks_processed": self.tasks_processed}
        )
    
    def process_task(self, task: Task):
        """
        Process a single task by executing its function.
        
        Args:
            task: Task to process
        """
        log.info(
            f"Processing task {task.id}",
            extra={
                "task_id": task.id,
                "task_name": task.name,
                "priority": task.priority
            }
        )
        
        task.mark_running(self.worker_id)
        task.started_at = datetime.utcnow()
        
        try:
            # Get task function from registry
            task_func = task_registry.get_task(task.name)
            
            if task_func is None:
                raise ValueError(f"Task '{task.name}' not found in registry")
            
            # Execute the task
            log.debug(
                f"Executing task: {task.name}",
                extra={"task_id": task.id, "priority": task.priority}
            )
            
            result = execute_task_sync_or_async(task_func, *task.args, **task.kwargs)
            
            # Mark success
            task.mark_success(result)
            
            # Store result in Redis
            self._store_result(task)
            
            log.info(
                f"Task {task.id} completed successfully",
                extra={
                    "task_id": task.id,
                    "priority": task.priority,
                    "execution_time": task.execution_time()
                }
            )
            
        except Exception as e:
            error_msg = str(e)
            task.mark_failed(error_msg)
            self.tasks_failed += 1
            
            log.error(
                f"Task {task.id} failed: {error_msg}",
                extra={
                    "task_id": task.id,
                    "priority": task.priority,
                    "error": error_msg
                }
            )
            
            # Handle failure
            self._handle_task_failure(task)
    
    def _store_result(self, task: Task):
        """Store task result in Redis with TTL."""
        try:
            result_key = task.get_result_key()
            task_json = task.to_json()
            
            redis_broker.set_with_ttl(result_key, task_json, settings.result_ttl)
            
            log.debug(f"Stored result for task {task.id}")
            
        except Exception as e:
            log.error(f"Failed to store result for task {task.id}: {e}")
    
    def _handle_task_failure(self, task: Task):
        """Handle task failure - retry or move to DLQ."""
        try:
            if task.can_retry():
                task.increment_retry()
                
                # Re-enqueue with same priority
                self.priority_queue.enqueue(task)
                
                log.info(
                    f"Task {task.id} re-queued for retry {task.retry_count}/{task.max_retries}",
                    extra={
                        "task_id": task.id,
                        "priority": task.priority,
                        "retry_count": task.retry_count
                    }
                )
            else:
                # Move to dead letter queue
                self._move_to_dead_letter_queue(task)
                
                log.error(
                    f"Task {task.id} moved to DLQ after {task.max_retries} retries",
                    extra={"task_id": task.id, "priority": task.priority}
                )
                
        except Exception as e:
            log.error(f"Error handling task failure: {e}")
    
    def _move_to_dead_letter_queue(self, task: Task):
        """Move failed task to dead letter queue."""
        try:
            dlq_key = "dead_letter_queue"
            task_json = task.to_json()
            
            redis_broker.client.lpush(dlq_key, task_json)
            
            log.info(f"Task {task.id} added to DLQ")
            
        except Exception as e:
            log.error(f"Failed to move task {task.id} to DLQ: {e}")
    
    def get_result(self, task_id: str) -> Optional[Task]:
        """Retrieve task result from Redis."""
        try:
            result_key = f"result:{task_id}"
            task_json = redis_broker.get(result_key)
            
            if task_json:
                return Task.from_json(task_json)
            
            return None
            
        except Exception as e:
            log.error(f"Failed to retrieve result for task {task_id}: {e}")
            return None
    
    def get_stats(self) -> dict:
        """Get worker statistics."""
        stats = {
            "worker_id": self.worker_id,
            "queue_name": self.queue_name,
            "is_running": self.is_running,
            "tasks_processed": self.tasks_processed,
            "tasks_failed": self.tasks_failed
        }
        
        # Add priority queue stats
        if self.priority_queue:
            stats["queue_size"] = self.priority_queue.size()
            stats["queue_by_priority"] = self.priority_queue.size_by_priority()
        
        return stats

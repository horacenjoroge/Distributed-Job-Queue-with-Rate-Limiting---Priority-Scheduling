"""
Simple worker implementation with queue integration.
"""
import time
from typing import Optional
from jobqueue.worker.base_worker import Worker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.utils.logger import log


class SimpleWorker(Worker):
    """
    Simple worker that pulls tasks from Redis queue and processes them.
    """
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        queue_name: str = "default",
        poll_timeout: int = 5
    ):
        """
        Initialize simple worker.
        
        Args:
            worker_id: Unique worker identifier
            queue_name: Queue to process tasks from
            poll_timeout: Timeout for blocking queue operations (seconds)
            
        Example:
            worker = SimpleWorker(queue_name="default")
            worker.start()
        """
        super().__init__(worker_id, queue_name)
        self.poll_timeout = poll_timeout
        self.queue = None
        self.tasks_processed = 0
        
        log.info(
            f"SimpleWorker initialized",
            extra={
                "worker_id": self.worker_id,
                "queue": queue_name,
                "poll_timeout": poll_timeout
            }
        )
    
    def start(self):
        """Start the worker and connect to Redis."""
        # Connect to Redis
        if not redis_broker.is_connected():
            redis_broker.connect()
            log.info(f"Worker {self.worker_id} connected to Redis")
        
        # Initialize queue
        self.queue = Queue(self.queue_name)
        
        # Start processing
        super().start()
    
    def stop(self):
        """Stop the worker and disconnect."""
        super().stop()
        
        # Disconnect from Redis
        if redis_broker.is_connected():
            redis_broker.disconnect()
            log.info(f"Worker {self.worker_id} disconnected from Redis")
    
    def run_loop(self):
        """
        Main worker loop - continuously pull and process tasks.
        """
        log.info(f"Worker {self.worker_id} entering main loop")
        
        while self.is_running:
            try:
                # Pull task from queue with timeout
                task = self.queue.dequeue(timeout=self.poll_timeout)
                
                if task is None:
                    # Timeout, no task available
                    log.debug(
                        f"Worker {self.worker_id} waiting for tasks...",
                        extra={"queue": self.queue_name}
                    )
                    continue
                
                # Process the task
                log.info(
                    f"Worker {self.worker_id} received task",
                    extra={"task_id": task.id, "task_name": task.name}
                )
                
                self.process_task(task)
                self.tasks_processed += 1
                
            except Exception as e:
                log.error(f"Error in worker loop: {e}")
                # Continue processing even if one task fails
                time.sleep(1)
        
        log.info(
            f"Worker {self.worker_id} exited main loop",
            extra={"tasks_processed": self.tasks_processed}
        )
    
    def process_task(self, task: Task):
        """
        Process a single task.
        This is a placeholder - will be enhanced in next commit.
        
        Args:
            task: Task to process
        """
        log.info(
            f"Processing task {task.id}",
            extra={"task_id": task.id, "task_name": task.name}
        )
        
        # TODO: Actual task execution will be implemented
        # For now, just log that we received it
        log.debug(
            f"Task details",
            extra={
                "task_id": task.id,
                "name": task.name,
                "args": task.args,
                "kwargs": task.kwargs
            }
        )
    
    def get_stats(self) -> dict:
        """
        Get worker statistics.
        
        Returns:
            Dictionary with worker stats
        """
        return {
            "worker_id": self.worker_id,
            "queue_name": self.queue_name,
            "is_running": self.is_running,
            "tasks_processed": self.tasks_processed
        }

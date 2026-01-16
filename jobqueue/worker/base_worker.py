"""
Basic worker implementation for processing tasks.
"""
import os
import socket
from typing import Optional
from jobqueue.utils.logger import log


class Worker:
    """
    Basic worker class for processing tasks from the queue.
    """
    
    def __init__(self, worker_id: Optional[str] = None, queue_name: str = "default"):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique worker identifier (auto-generated if not provided)
            queue_name: Name of the queue to process
            
        Example:
            worker = Worker()
            worker.start()
        """
        self.worker_id = worker_id or self._generate_worker_id()
        self.queue_name = queue_name
        self.is_running = False
        
        log.info(
            f"Worker initialized",
            extra={"worker_id": self.worker_id, "queue": queue_name}
        )
    
    def _generate_worker_id(self) -> str:
        """
        Generate a unique worker ID.
        
        Returns:
            Worker ID in format: worker-{hostname}-{pid}
        """
        hostname = socket.gethostname()
        pid = os.getpid()
        return f"worker-{hostname}-{pid}"
    
    def start(self):
        """
        Start the worker to process tasks.
        Worker will run until stop() is called.
        
        Example:
            worker = Worker()
            worker.start()
        """
        log.info(f"Starting worker {self.worker_id}")
        self.is_running = True
        
        try:
            self.run_loop()
        except KeyboardInterrupt:
            log.info("Worker interrupted by user")
            self.stop()
        except Exception as e:
            log.error(f"Worker crashed: {e}")
            self.stop()
            raise
    
    def stop(self):
        """
        Stop the worker gracefully.
        
        Example:
            worker.stop()
        """
        log.info(f"Stopping worker {self.worker_id}")
        self.is_running = False
    
    def run_loop(self):
        """
        Main worker loop that processes tasks continuously.
        Override this method in subclasses for custom behavior.
        """
        raise NotImplementedError("Subclasses must implement run_loop()")
    
    def process_task(self, task):
        """
        Process a single task.
        Override this method in subclasses for custom task processing.
        
        Args:
            task: Task object to process
        """
        raise NotImplementedError("Subclasses must implement process_task()")
    
    def __repr__(self) -> str:
        """String representation of worker."""
        return f"Worker(id='{self.worker_id}', queue='{self.queue_name}', running={self.is_running})"

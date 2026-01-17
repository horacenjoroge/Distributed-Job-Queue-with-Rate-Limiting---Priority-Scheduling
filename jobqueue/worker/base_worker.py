"""
Basic worker implementation for processing tasks.
"""
import os
import socket
import threading
import time
from typing import Optional
from datetime import datetime
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.core.task import WorkerType
from jobqueue.utils.logger import log


class Worker:
    """
    Basic worker class for processing tasks from the queue.
    """
    
    def __init__(
        self,
        worker_id: Optional[str] = None,
        queue_name: str = "default",
        worker_type: Optional[WorkerType] = None
    ):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique worker identifier (auto-generated if not provided)
            queue_name: Name of the queue to process (can be overridden by worker_type)
            worker_type: Worker type (cpu, io, gpu, default) - determines which queues to subscribe to
            
        Example:
            worker = Worker()
            worker.start()
        """
        self.worker_id = worker_id or self._generate_worker_id()
        self.worker_type = worker_type or WorkerType.DEFAULT
        
        # If worker_type is specified, determine queues to subscribe to
        if worker_type and worker_type != WorkerType.DEFAULT:
            from jobqueue.core.task_routing import task_router
            from jobqueue.core.task import TaskPriority
            
            # Get queues for this worker type
            self.subscribed_queues = task_router.get_worker_queues(
                worker_type,
                priorities=[TaskPriority.HIGH, TaskPriority.MEDIUM, TaskPriority.LOW]
            )
            # Use first queue as primary queue_name for backward compatibility
            self.queue_name = self.subscribed_queues[0] if self.subscribed_queues else queue_name
        else:
            self.subscribed_queues = [queue_name]
            self.queue_name = queue_name
        
        self.is_running = False
        
        # Worker identification
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.started_at = None
        self.stopped_at = None
        
        # Heartbeat thread
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.heartbeat_running = False
        self.current_status = WorkerStatus.IDLE
        
        log.info(
            f"Worker initialized",
            extra={
                "worker_id": self.worker_id,
                "hostname": self.hostname,
                "pid": self.pid,
                "queue": queue_name
            }
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
        self.started_at = datetime.utcnow()
        
        log.info(
            f"Starting worker {self.worker_id}",
            extra={
                "worker_id": self.worker_id,
                "hostname": self.hostname,
                "pid": self.pid,
                "started_at": self.started_at.isoformat()
            }
        )
        self.is_running = True
        
        # Start heartbeat thread
        self._start_heartbeat()
        
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
        self.stopped_at = datetime.utcnow()
        
        uptime = None
        if self.started_at:
            uptime = (self.stopped_at - self.started_at).total_seconds()
        
        log.info(
            f"Stopping worker {self.worker_id}",
            extra={
                "worker_id": self.worker_id,
                "hostname": self.hostname,
                "pid": self.pid,
                "uptime_seconds": uptime
            }
        )
        self.is_running = False
        
        # Stop heartbeat thread
        self._stop_heartbeat()
    
    def get_info(self) -> dict:
        """
        Get worker information.
        
        Returns:
            Dictionary with worker details
        """
        info = {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "pid": self.pid,
            "queue_name": self.queue_name,
            "is_running": self.is_running,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "stopped_at": self.stopped_at.isoformat() if self.stopped_at else None
        }
        
        if self.started_at and self.is_running:
            uptime = (datetime.utcnow() - self.started_at).total_seconds()
            info["uptime_seconds"] = uptime
        
        return info
    
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
    
    def _start_heartbeat(self) -> None:
        """Start heartbeat thread."""
        self.heartbeat_running = True
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self.worker_id}"
        )
        self.heartbeat_thread.start()
        
        log.debug(f"Started heartbeat thread for worker {self.worker_id}")
    
    def _stop_heartbeat(self) -> None:
        """Stop heartbeat thread."""
        self.heartbeat_running = False
        
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=2)
        
        log.debug(f"Stopped heartbeat thread for worker {self.worker_id}")
    
    def _heartbeat_loop(self) -> None:
        """Heartbeat loop that runs in background thread."""
        while self.heartbeat_running and self.is_running:
            try:
                # Send heartbeat with current status
                metadata = {
                    "hostname": self.hostname,
                    "pid": self.pid,
                    "queue_name": self.queue_name,
                    "started_at": self.started_at.isoformat() if self.started_at else None
                }
                
                worker_heartbeat.send_heartbeat(
                    worker_id=self.worker_id,
                    status=self.current_status,
                    metadata=metadata
                )
                
                # Sleep for heartbeat interval
                time.sleep(worker_heartbeat.heartbeat_interval)
                
            except Exception as e:
                log.error(f"Error sending heartbeat: {e}")
                time.sleep(worker_heartbeat.heartbeat_interval)
    
    def set_status(self, status: WorkerStatus) -> None:
        """
        Update worker status.
        
        Args:
            status: New worker status
        """
        self.current_status = status
    
    def __repr__(self) -> str:
        """String representation of worker."""
        return f"Worker(id='{self.worker_id}', queue='{self.queue_name}', running={self.is_running})"

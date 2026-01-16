"""
Worker implementation for processing tasks.
"""
import os
import socket
import signal
import sys
from typing import Optional
from datetime import datetime

from jobqueue.broker.redis_broker import redis_broker
from jobqueue.backend.postgres_backend import postgres_backend
from jobqueue.core.task import Task, TaskStatus, TaskPriority
from jobqueue.utils.logger import log
from config import settings


class Worker:
    """
    Worker class that processes tasks from the queue.
    """
    
    def __init__(self, worker_id: Optional[str] = None):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique worker ID (generated if not provided)
        """
        self.worker_id = worker_id or self._generate_worker_id()
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.is_running = False
        self.current_task: Optional[Task] = None
        
        # Statistics
        self.processed_tasks = 0
        self.failed_tasks = 0
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        log.info(
            f"Worker initialized",
            extra={
                "worker_id": self.worker_id,
                "hostname": self.hostname,
                "pid": self.pid,
            }
        )
    
    def _generate_worker_id(self) -> str:
        """Generate a unique worker ID."""
        hostname = socket.gethostname()
        pid = os.getpid()
        return f"worker-{hostname}-{pid}"
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        log.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)
    
    def start(self):
        """Start the worker to process tasks."""
        try:
            log.info(f"Starting worker {self.worker_id}")
            
            # Connect to Redis and PostgreSQL
            redis_broker.connect()
            postgres_backend.connect()
            
            # Register worker in database
            self._register_worker()
            
            self.is_running = True
            
            log.info(f"Worker {self.worker_id} started successfully")
            
            # Main processing loop
            self._process_loop()
            
        except Exception as e:
            log.error(f"Error starting worker: {e}")
            raise
    
    def stop(self):
        """Stop the worker gracefully."""
        log.info(f"Stopping worker {self.worker_id}")
        
        self.is_running = False
        
        # Update worker status in database
        self._update_worker_status("stopped")
        
        # Disconnect from services
        redis_broker.disconnect()
        postgres_backend.disconnect()
        
        log.info(f"Worker {self.worker_id} stopped")
    
    def _register_worker(self):
        """Register worker in the database."""
        query = """
        INSERT INTO workers (id, hostname, pid, status, processed_tasks, failed_tasks)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            started_at = CURRENT_TIMESTAMP,
            last_heartbeat = CURRENT_TIMESTAMP,
            status = EXCLUDED.status
        """
        
        params = (
            self.worker_id,
            self.hostname,
            self.pid,
            "running",
            0,
            0,
        )
        
        postgres_backend.execute_query(query, params)
        log.info(f"Worker {self.worker_id} registered in database")
    
    def _update_worker_status(self, status: str):
        """Update worker status in database."""
        query = """
        UPDATE workers SET
            status = %s,
            last_heartbeat = CURRENT_TIMESTAMP,
            current_task_id = %s,
            processed_tasks = %s,
            failed_tasks = %s
        WHERE id = %s
        """
        
        current_task_id = self.current_task.id if self.current_task else None
        
        params = (
            status,
            current_task_id,
            self.processed_tasks,
            self.failed_tasks,
            self.worker_id,
        )
        
        postgres_backend.execute_query(query, params)
    
    def _process_loop(self):
        """Main task processing loop."""
        log.info(f"Worker {self.worker_id} entering processing loop")
        
        # Priority order for queue checking
        priorities = [TaskPriority.HIGH, TaskPriority.MEDIUM, TaskPriority.LOW]
        
        while self.is_running:
            try:
                # Check max tasks per child limit
                if self.processed_tasks >= settings.worker_max_tasks_per_child:
                    log.info(f"Worker reached max tasks limit, shutting down for restart")
                    break
                
                # Try to get task from each priority queue
                task = None
                for priority in priorities:
                    queue_key = f"queue:default:{priority}"
                    task_json = redis_broker.pop_task(queue_key, timeout=1)
                    
                    if task_json:
                        task = Task.from_json(task_json)
                        break
                
                if task:
                    self._process_task(task)
                else:
                    # Update heartbeat even when idle
                    self._update_worker_status("idle")
                
            except Exception as e:
                log.error(f"Error in processing loop: {e}")
                continue
    
    def _process_task(self, task: Task):
        """
        Process a single task.
        
        Args:
            task: Task to process
        """
        self.current_task = task
        
        log.info(
            f"Processing task {task.id}",
            extra={
                "task_id": task.id,
                "task_name": task.name,
                "priority": task.priority,
            }
        )
        
        try:
            # Mark task as running
            task.mark_running(self.worker_id)
            self._update_task_in_db(task)
            self._update_worker_status("running")
            
            # Execute the task
            # TODO: Implement actual task execution with registered task handlers
            # For now, we'll just simulate success
            result = self._execute_task(task)
            
            # Mark as successful
            task.mark_success(result)
            self._update_task_in_db(task)
            
            # Store result in Redis with TTL
            result_key = task.get_result_key()
            redis_broker.set_with_ttl(
                result_key,
                task.to_json(),
                settings.result_ttl
            )
            
            self.processed_tasks += 1
            
            log.info(
                f"Task {task.id} completed successfully",
                extra={"task_id": task.id, "execution_time": task.execution_time()}
            )
            
        except Exception as e:
            log.error(f"Task {task.id} failed: {e}")
            
            task.mark_failed(str(e))
            self._update_task_in_db(task)
            
            self.failed_tasks += 1
            
            # Handle retry logic
            if task.can_retry():
                task.increment_retry()
                self._requeue_task(task)
                log.info(f"Task {task.id} requeued for retry {task.retry_count}/{task.max_retries}")
            else:
                # Move to dead letter queue
                self._move_to_dead_letter_queue(task)
                log.error(f"Task {task.id} moved to dead letter queue after max retries")
        
        finally:
            self.current_task = None
    
    def _execute_task(self, task: Task):
        """
        Execute the actual task logic.
        
        Args:
            task: Task to execute
            
        Returns:
            Task result
            
        Note:
            This is a placeholder. Actual implementation will involve
            a task registry and dynamic task loading.
        """
        # TODO: Implement task registry and execution
        # For now, just return a success message
        log.info(f"Executing task: {task.name} with args={task.args}, kwargs={task.kwargs}")
        return {"status": "success", "message": f"Task {task.name} executed"}
    
    def _update_task_in_db(self, task: Task):
        """Update task in database."""
        query = """
        UPDATE tasks SET
            status = %s,
            worker_id = %s,
            started_at = %s,
            completed_at = %s,
            result = %s,
            error = %s,
            retry_count = %s
        WHERE id = %s
        """
        
        task_dict = task.to_dict()
        params = (
            task_dict["status"],
            task_dict["worker_id"],
            task_dict["started_at"],
            task_dict["completed_at"],
            task_dict["result"],
            task_dict["error"],
            task_dict["retry_count"],
            task_dict["id"],
        )
        
        postgres_backend.execute_query(query, params)
    
    def _requeue_task(self, task: Task):
        """Requeue a task for retry."""
        task.status = TaskStatus.QUEUED
        self._update_task_in_db(task)
        
        queue_key = task.get_queue_key()
        redis_broker.push_task(queue_key, task.to_json())
    
    def _move_to_dead_letter_queue(self, task: Task):
        """Move failed task to dead letter queue."""
        query = """
        INSERT INTO dead_letter_queue (task_id, task_data, error, retry_count, original_queue)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        params = (
            task.id,
            task.to_json(),
            task.error,
            task.retry_count,
            task.queue_name,
        )
        
        postgres_backend.execute_query(query, params)


def main():
    """Main entry point for the worker."""
    log.info("Starting job queue worker")
    
    worker = Worker()
    
    try:
        worker.start()
    except KeyboardInterrupt:
        log.info("Worker interrupted by user")
        worker.stop()
    except Exception as e:
        log.error(f"Worker crashed: {e}")
        worker.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

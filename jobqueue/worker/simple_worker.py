"""
Simple worker implementation with queue integration.
"""
import time
import signal
import sys
from typing import Optional
from datetime import datetime
from jobqueue.worker.base_worker import Worker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task, TaskStatus
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task_registry import task_registry
from jobqueue.core.async_support import execute_task_sync_or_async
from jobqueue.core.distributed_rate_limiter import distributed_rate_limiter
from jobqueue.core.queue_config import queue_config_manager
from jobqueue.core.task_dependencies import task_dependency_graph
from jobqueue.core.retry_backoff import default_backoff
from jobqueue.core.scheduled_tasks import scheduled_task_store
from jobqueue.core.dead_letter_queue import dead_letter_queue, add_to_dlq
from jobqueue.core.task_timeout import TimeoutManager, ProcessTimeoutKiller
from jobqueue.core.worker_heartbeat import WorkerStatus
from jobqueue.utils.logger import log
from config import settings


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
        self.tasks_failed = 0
        self.current_task = None
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        log.info(
            f"SimpleWorker initialized",
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
        
        log.debug("Signal handlers registered for SIGTERM and SIGINT")
    
    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        signal_name = signal.Signals(signum).name
        
        log.info(
            f"Worker {self.worker_id} received {signal_name}, shutting down gracefully...",
            extra={"signal": signal_name, "signum": signum}
        )
        
        # Stop the worker
        self.stop()
        
        # Exit cleanly
        sys.exit(0)
    
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
        """
        Stop the worker gracefully.
        Waits for current task to complete if any.
        """
        log.info(f"Gracefully stopping worker {self.worker_id}")
        
        if self.current_task:
            log.info(
                f"Waiting for current task {self.current_task.id} to complete",
                extra={"task_id": self.current_task.id}
            )
        
        super().stop()
        
        # Disconnect from Redis
        if redis_broker.is_connected():
            redis_broker.disconnect()
            log.info(f"Worker {self.worker_id} disconnected from Redis")
        
        # Log final statistics
        log.info(
            f"Worker {self.worker_id} stopped",
            extra={
                "tasks_processed": self.tasks_processed,
                "tasks_failed": self.tasks_failed
            }
        )
    
    def run_loop(self):
        """
        Main worker loop - continuously pull and process tasks.
        Respects rate limits before dequeuing tasks.
        """
        log.info(f"Worker {self.worker_id} entering main loop")
        
        while self.is_running:
            try:
                # Check rate limit before dequeuing
                rate_config = queue_config_manager.get_rate_limit(self.queue_name)
                
                if not rate_config.is_unlimited:
                    # Try to acquire permission under rate limit
                    can_proceed = distributed_rate_limiter.acquire(self.queue_name)
                    
                    if not can_proceed:
                        # Rate limit hit, wait and retry
                        wait_time = distributed_rate_limiter.wait_time_until_capacity(self.queue_name)
                        
                        log.info(
                            f"Worker {self.worker_id} rate limit hit, sleeping {wait_time}s",
                            extra={
                                "queue": self.queue_name,
                                "wait_time": wait_time
                            }
                        )
                        
                        time.sleep(min(wait_time + 0.1, self.poll_timeout))
                        continue
                
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
                
                self.current_task = task
                self.process_task(task)
                self.current_task = None
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
        Process a single task by executing its function.
        Checks dependencies before execution.
        
        Args:
            task: Task to process
        """
        log.info(
            f"Processing task {task.id}",
            extra={"task_id": task.id, "task_name": task.name}
        )
        
        # Check dependencies before processing
        if task.depends_on:
            if not self._check_dependencies(task):
                return
        
        # Mark task as running
        task.mark_running(self.worker_id)
        task.started_at = datetime.utcnow()
        
        # Update worker status to ACTIVE
        self.set_status(WorkerStatus.ACTIVE)
        
        # Update status in dependency graph
        task_dependency_graph.set_task_status(task.id, TaskStatus.RUNNING)
        
        # Initialize timeout manager
        timeout_manager = None
        if task.timeout > 0:
            timeout_manager = TimeoutManager(
                timeout_seconds=task.timeout,
                soft_timeout_ratio=0.8,
                enable_soft_timeout=True
            )
            timeout_manager.start()
        
        try:
            # Get task function from registry
            task_func = task_registry.get_task(task.name)
            
            if task_func is None:
                raise ValueError(f"Task '{task.name}' not found in registry")
            
            # Execute the task function with args/kwargs
            log.debug(
                f"Executing task function: {task.name}",
                extra={
                    "task_id": task.id,
                    "args": task.args,
                    "kwargs": task.kwargs,
                    "timeout": task.timeout
                }
            )
            
            # Execute with timeout monitoring
            result = None
            timeout_exceeded = False
            
            def execute_task():
                """Execute task function."""
                return execute_task_sync_or_async(task_func, *task.args, **task.kwargs)
            
            # Execute task in a thread with timeout
            import threading
            result_container = {"result": None, "exception": None}
            
            def run_task():
                try:
                    result_container["result"] = execute_task()
                except Exception as e:
                    result_container["exception"] = e
            
            task_thread = threading.Thread(target=run_task, daemon=True)
            task_thread.start()
            task_thread.join(timeout=task.timeout if task.timeout > 0 else None)
            
            # Check if task timed out
            if task_thread.is_alive():
                timeout_exceeded = True
                log.error(
                    f"Task {task.id} exceeded timeout of {task.timeout}s",
                    extra={
                        "task_id": task.id,
                        "timeout": task.timeout,
                        "elapsed": timeout_manager.get_elapsed_time() if timeout_manager else 0
                    }
                )
                
                # Mark as timeout
                task.mark_timeout()
                task_dependency_graph.set_task_status(task.id, TaskStatus.TIMEOUT)
                
                # Try to kill the thread (limited support in Python)
                # Note: Python threads cannot be forcefully killed
                # In production, consider using multiprocessing for true timeout enforcement
                
                raise TimeoutError(f"Task exceeded timeout of {task.timeout} seconds")
            
            # Check for exceptions
            if result_container["exception"]:
                raise result_container["exception"]
            
            result = result_container["result"]
            
            # Check timeout status
            if timeout_manager:
                is_timed_out, soft_warning = timeout_manager.check()
                if is_timed_out:
                    timeout_exceeded = True
                    task.mark_timeout()
                    task_dependency_graph.set_task_status(task.id, TaskStatus.TIMEOUT)
                    raise TimeoutError(f"Task exceeded timeout of {task.timeout} seconds")
            
            # Task succeeded
            task.mark_success(result)
            
            # Update status in dependency graph
            task_dependency_graph.set_task_status(task.id, TaskStatus.SUCCESS)
            
            # Store result in Redis with TTL
            self._store_result(task)
            
            log.info(
                f"Task {task.id} completed successfully",
                extra={
                    "task_id": task.id,
                    "task_name": task.name,
                    "execution_time": task.execution_time()
                }
            )
            
        except TimeoutError as e:
            # Task timed out
            error_msg = str(e)
            task.mark_timeout()
            self.tasks_failed += 1
            
            # Store exception for DLQ
            task._last_exception = e
            
            # Update status in dependency graph
            task_dependency_graph.set_task_status(task.id, TaskStatus.TIMEOUT)
            
            # Cancel dependent tasks
            self._cancel_dependent_tasks(task)
            
            # Log timeout with details
            elapsed = timeout_manager.get_elapsed_time() if timeout_manager else 0
            log.error(
                f"Task {task.id} timed out after {elapsed:.2f}s (limit: {task.timeout}s)",
                extra={
                    "task_id": task.id,
                    "task_name": task.name,
                    "timeout": task.timeout,
                    "elapsed": elapsed,
                    "error": error_msg
                }
            )
            
            # Handle failure (retry or dead letter queue)
            # Timeouts typically shouldn't be retried, but allow it if configured
            self._handle_task_failure(task, exception=e)
            
        except Exception as e:
            # Task failed
            error_msg = str(e)
            task.mark_failed(error_msg)
            self.tasks_failed += 1
            
            # Store exception for DLQ (if needed)
            task._last_exception = e
            
            # Update status in dependency graph
            task_dependency_graph.set_task_status(task.id, TaskStatus.FAILED)
            
            # Cancel dependent tasks if this task failed
            self._cancel_dependent_tasks(task)
            
            # Log error with full details
            log.error(
                f"Task {task.id} failed: {error_msg}",
                extra={
                    "task_id": task.id,
                    "task_name": task.name,
                    "error": error_msg,
                    "retry_count": task.retry_count,
                    "max_retries": task.max_retries
                }
            )
            
            # Handle failure (retry or dead letter queue)
            self._handle_task_failure(task, exception=e)
        
        finally:
            # Stop timeout manager
            if timeout_manager:
                timeout_manager.stop()
            
            # Update worker status back to IDLE
            self.set_status(WorkerStatus.IDLE)
    
    def _store_result(self, task: Task):
        """
        Store task result in Redis with TTL.
        
        Args:
            task: Completed task with result
        """
        try:
            result_key = task.get_result_key()
            task_json = task.to_json()
            
            # Store with configured TTL
            redis_broker.set_with_ttl(
                result_key,
                task_json,
                settings.result_ttl
            )
            
            log.debug(
                f"Stored result for task {task.id}",
                extra={
                    "task_id": task.id,
                    "result_key": result_key,
                    "ttl": settings.result_ttl
                }
            )
            
        except Exception as e:
            log.error(f"Failed to store result for task {task.id}: {e}")
    
    def _handle_task_failure(self, task: Task, exception: Optional[Exception] = None):
        """
        Handle task failure - retry with exponential backoff or move to DLQ.
        
        Args:
            task: Failed task
            exception: Exception that caused failure (for stack trace)
        """
        try:
            # Check if task can be retried
            if task.can_retry():
                # Calculate exponential backoff delay
                backoff_delay = default_backoff.calculate_delay(task.retry_count)
                
                # Record retry attempt in history
                task.record_retry_attempt(
                    error=task.error or "Unknown error",
                    backoff_seconds=backoff_delay
                )
                
                # Increment retry count
                task.increment_retry()
                
                # Schedule retry with backoff delay
                if backoff_delay > 0:
                    # Use countdown for delayed retry
                    task.countdown = int(backoff_delay)
                    task.set_schedule_time()
                    
                    # Schedule for future execution
                    scheduled_task_store.schedule_task(task)
                    
                    log.info(
                        f"Task {task.id} scheduled for retry with {backoff_delay:.2f}s backoff",
                        extra={
                            "task_id": task.id,
                            "retry_count": task.retry_count,
                            "max_retries": task.max_retries,
                            "backoff_seconds": backoff_delay
                        }
                    )
                else:
                    # Immediate retry (no backoff)
                    self.queue.enqueue(task)
                    
                    log.info(
                        f"Task {task.id} re-queued for immediate retry",
                        extra={
                            "task_id": task.id,
                            "retry_count": task.retry_count,
                            "max_retries": task.max_retries
                        }
                    )
            else:
                # Move to dead letter queue
                self._move_to_dead_letter_queue(task, exception)
                
                log.error(
                    f"Task {task.id} moved to dead letter queue after {task.max_retries} retries",
                    extra={
                        "task_id": task.id,
                        "task_name": task.name,
                        "retry_history": task.retry_history
                    }
                )
                
        except Exception as e:
            log.error(f"Error handling task failure: {e}")
    
    def _move_to_dead_letter_queue(self, task: Task, exception: Optional[Exception] = None):
        """
        Move failed task to dead letter queue with failure reason and stack trace.
        
        Args:
            task: Failed task
            exception: Exception that caused failure (for stack trace)
        """
        try:
            # Get failure reason
            failure_reason = task.error or "Task exceeded maximum retry attempts"
            
            # Use stored exception if available
            if exception is None and hasattr(task, '_last_exception'):
                exception = task._last_exception
            
            # Add to Dead Letter Queue
            add_to_dlq(task, failure_reason, exception)
            
            log.info(
                f"Task {task.id} added to Dead Letter Queue",
                extra={
                    "task_id": task.id,
                    "error": task.error,
                    "retry_count": task.retry_count,
                    "max_retries": task.max_retries
                }
            )
            
        except Exception as e:
            log.error(f"Failed to move task {task.id} to Dead Letter Queue: {e}")
    
    def get_result(self, task_id: str) -> Optional[Task]:
        """
        Retrieve task result from Redis.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task object with result, or None if not found
        """
        try:
            result_key = f"result:{task_id}"
            task_json = redis_broker.get(result_key)
            
            if task_json:
                return Task.from_json(task_json)
            
            return None
            
        except Exception as e:
            log.error(f"Failed to retrieve result for task {task_id}: {e}")
            return None
    
    def _check_dependencies(self, task: Task) -> bool:
        """
        Check if task dependencies are satisfied.
        If not, re-queue the task.
        
        Args:
            task: Task to check
            
        Returns:
            True if dependencies satisfied, False if re-queued
        """
        # Check for failed dependencies
        has_failures, failed_deps = task_dependency_graph.has_failed_dependencies(task.id)
        
        if has_failures:
            # Parent task(s) failed, cancel this task
            log.warning(
                f"Task {task.id} cancelled due to failed dependencies",
                extra={
                    "task_id": task.id,
                    "failed_dependencies": failed_deps
                }
            )
            
            task.mark_failed(f"Cancelled: dependencies failed: {failed_deps}")
            task_dependency_graph.set_task_status(task.id, TaskStatus.CANCELLED)
            
            # Cancel any dependent tasks
            self._cancel_dependent_tasks(task)
            
            return False
        
        # Check if all dependencies are satisfied
        satisfied, pending = task_dependency_graph.are_dependencies_satisfied(task.id)
        
        if not satisfied:
            # Dependencies not ready, re-queue task
            log.info(
                f"Task {task.id} dependencies not satisfied, re-queuing",
                extra={
                    "task_id": task.id,
                    "pending_dependencies": pending
                }
            )
            
            # Re-enqueue with a small delay
            time.sleep(0.5)
            self.queue.enqueue(task)
            
            return False
        
        # All dependencies satisfied
        log.debug(
            f"Task {task.id} dependencies satisfied",
            extra={"task_id": task.id}
        )
        
        return True
    
    def _cancel_dependent_tasks(self, task: Task) -> None:
        """
        Cancel all tasks that depend on this failed task.
        
        Args:
            task: Failed task
        """
        try:
            cancelled = task_dependency_graph.cancel_dependent_tasks(task.id)
            
            if cancelled:
                log.warning(
                    f"Cancelled {len(cancelled)} dependent tasks",
                    extra={
                        "parent_task": task.id,
                        "cancelled_tasks": cancelled
                    }
                )
        except Exception as e:
            log.error(f"Error cancelling dependent tasks: {e}")
    
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
            "tasks_processed": self.tasks_processed,
            "tasks_failed": self.tasks_failed
        }

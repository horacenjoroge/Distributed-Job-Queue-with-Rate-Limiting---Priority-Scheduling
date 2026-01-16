"""
Task cancellation system for cancelling pending or running tasks.
"""
import time
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.utils.logger import log


class CancellationReason(str, Enum):
    """Cancellation reason types."""
    USER_REQUESTED = "user_requested"
    TIMEOUT = "timeout"
    DEPENDENCY_FAILED = "dependency_failed"
    WORKER_SHUTDOWN = "worker_shutdown"
    SYSTEM_ERROR = "system_error"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    OTHER = "other"


class TaskCancellation:
    """
    Manages task cancellation for pending and running tasks.
    """
    
    def __init__(self):
        """Initialize task cancellation system."""
        log.info("TaskCancellation system initialized")
    
    def _get_cancellation_key(self, task_id: str) -> str:
        """Get Redis key for cancellation flag."""
        return f"cancel:{task_id}"
    
    def _get_cancellation_reason_key(self, task_id: str) -> str:
        """Get Redis key for cancellation reason."""
        return f"cancel:reason:{task_id}"
    
    def request_cancellation(
        self,
        task_id: str,
        reason: CancellationReason = CancellationReason.USER_REQUESTED,
        force: bool = False
    ) -> bool:
        """
        Request cancellation of a task.
        
        Args:
            task_id: Task ID to cancel
            reason: Cancellation reason
            force: If True, force kill running tasks
            
        Returns:
            True if cancellation requested successfully
        """
        try:
            cancellation_key = self._get_cancellation_key(task_id)
            reason_key = self._get_cancellation_reason_key(task_id)
            
            # Set cancellation flag (TTL: 1 hour)
            redis_broker.client.setex(
                cancellation_key,
                3600,
                "1" if force else "0"  # 1 = force, 0 = graceful
            )
            
            # Store cancellation reason
            redis_broker.client.setex(
                reason_key,
                3600,
                reason.value
            )
            
            log.info(
                f"Cancellation requested for task {task_id}",
                extra={
                    "task_id": task_id,
                    "reason": reason.value,
                    "force": force
                }
            )
            
            return True
            
        except Exception as e:
            log.error(
                f"Failed to request cancellation for task {task_id}: {e}",
                extra={"task_id": task_id, "error": str(e)}
            )
            return False
    
    def should_cancel(self, task_id: str) -> tuple[bool, bool, Optional[str]]:
        """
        Check if task should be cancelled.
        
        Args:
            task_id: Task ID to check
            
        Returns:
            Tuple of (should_cancel, force, reason)
        """
        try:
            cancellation_key = self._get_cancellation_key(task_id)
            reason_key = self._get_cancellation_reason_key(task_id)
            
            # Check cancellation flag
            cancel_flag = redis_broker.client.get(cancellation_key)
            
            if cancel_flag is None:
                return False, False, None
            
            if isinstance(cancel_flag, bytes):
                cancel_flag = cancel_flag.decode()
            
            force = cancel_flag == "1"
            
            # Get reason
            reason_str = redis_broker.client.get(reason_key)
            if reason_str:
                if isinstance(reason_str, bytes):
                    reason_str = reason_str.decode()
                reason = reason_str
            else:
                reason = CancellationReason.USER_REQUESTED.value
            
            return True, force, reason
            
        except Exception as e:
            log.error(f"Error checking cancellation for task {task_id}: {e}")
            return False, False, None
    
    def cancel_pending_task(
        self,
        task: Task,
        reason: CancellationReason = CancellationReason.USER_REQUESTED
    ) -> bool:
        """
        Cancel a pending task by removing it from the queue.
        
        Args:
            task: Task to cancel
            reason: Cancellation reason
            
        Returns:
            True if cancelled successfully
        """
        try:
            # Remove from queue
            if task.priority:
                queue = PriorityQueue(task.queue_name)
            else:
                queue = Queue(task.queue_name)
            
            # Try to remove from queue
            removed = queue.remove_task(task.id)
            
            if removed:
                # Mark as cancelled
                task.mark_cancelled()
                task.error = f"Cancelled: {reason.value}"
                
                # Request cancellation (for tracking)
                self.request_cancellation(task.id, reason, force=False)
                
                log.info(
                    f"Cancelled pending task {task.id}",
                    extra={
                        "task_id": task.id,
                        "reason": reason.value,
                        "queue": task.queue_name
                    }
                )
                
                return True
            else:
                log.warning(
                    f"Could not remove pending task {task.id} from queue",
                    extra={"task_id": task.id}
                )
                return False
                
        except Exception as e:
            log.error(
                f"Error cancelling pending task {task.id}: {e}",
                extra={"task_id": task.id, "error": str(e)}
            )
            return False
    
    def cancel_running_task(
        self,
        task: Task,
        reason: CancellationReason = CancellationReason.USER_REQUESTED,
        force: bool = False
    ) -> bool:
        """
        Cancel a running task by requesting cancellation.
        Worker will check should_cancel() and stop execution.
        
        Args:
            task: Task to cancel
            reason: Cancellation reason
            force: If True, force kill the task
            
        Returns:
            True if cancellation requested successfully
        """
        try:
            # Request cancellation
            success = self.request_cancellation(task.id, reason, force=force)
            
            if success:
                log.info(
                    f"Cancellation requested for running task {task.id}",
                    extra={
                        "task_id": task.id,
                        "reason": reason.value,
                        "force": force,
                        "worker_id": task.worker_id
                    }
                )
            
            return success
            
        except Exception as e:
            log.error(
                f"Error cancelling running task {task.id}: {e}",
                extra={"task_id": task.id, "error": str(e)}
            )
            return False
    
    def cancel_task(
        self,
        task: Task,
        reason: CancellationReason = CancellationReason.USER_REQUESTED,
        force: bool = False
    ) -> bool:
        """
        Cancel a task (pending or running).
        
        Args:
            task: Task to cancel
            reason: Cancellation reason
            force: If True, force kill running tasks
            
        Returns:
            True if cancelled successfully
        """
        if task.status == TaskStatus.PENDING:
            return self.cancel_pending_task(task, reason)
        elif task.status == TaskStatus.RUNNING:
            return self.cancel_running_task(task, reason, force=force)
        elif task.status == TaskStatus.RETRY:
            # Treat retry tasks as pending
            return self.cancel_pending_task(task, reason)
        else:
            log.warning(
                f"Cannot cancel task {task.id} with status {task.status.value}",
                extra={"task_id": task.id, "status": task.status.value}
            )
            return False
    
    def complete_cancellation(self, task_id: str) -> None:
        """
        Mark cancellation as complete (cleanup).
        
        Args:
            task_id: Task ID
        """
        try:
            cancellation_key = self._get_cancellation_key(task_id)
            reason_key = self._get_cancellation_reason_key(task_id)
            
            # Remove cancellation flags
            redis_broker.client.delete(cancellation_key)
            redis_broker.client.delete(reason_key)
            
            log.debug(f"Cancellation completed for task {task_id}")
            
        except Exception as e:
            log.error(f"Error completing cancellation: {e}")
    
    def get_cancellation_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get cancellation information for a task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Dictionary with cancellation info or None
        """
        try:
            should_cancel, force, reason = self.should_cancel(task_id)
            
            if not should_cancel:
                return None
            
            return {
                "task_id": task_id,
                "cancelled": True,
                "force": force,
                "reason": reason
            }
            
        except Exception as e:
            log.error(f"Error getting cancellation info: {e}")
            return None


# Global task cancellation instance
task_cancellation = TaskCancellation()

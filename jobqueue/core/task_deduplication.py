"""
Task deduplication system to prevent duplicate task execution.
Uses task signatures (hash of name + args + kwargs) to detect duplicates.
"""
import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.backend.result_backend import result_backend
from jobqueue.utils.logger import log
from config import settings


class TaskDeduplication:
    """
    Manages task deduplication using signatures.
    Prevents duplicate task execution by tracking task signatures.
    """
    
    def __init__(self, default_ttl: int = 86400):
        """
        Initialize task deduplication system.
        
        Args:
            default_ttl: Default TTL for dedup keys in seconds (default: 24 hours)
        """
        self.default_ttl = default_ttl
        log.info(f"TaskDeduplication initialized with TTL: {default_ttl}s")
    
    def _get_dedup_key(self, signature: str) -> str:
        """Get Redis key for deduplication entry."""
        return f"dedup:{signature}"
    
    def _get_task_status_key(self, task_id: str) -> str:
        """Get Redis key for task status tracking."""
        return f"task:status:{task_id}"
    
    def check_duplicate(
        self,
        task: Task,
        allow_re_execution: bool = False
    ) -> Optional[str]:
        """
        Check if a task with the same signature already exists.
        
        Args:
            task: Task to check for duplicates
            allow_re_execution: If True, allow re-execution of completed tasks
            
        Returns:
            Existing task_id if duplicate found, None otherwise
        """
        try:
            # Generate signature if not present
            if task.task_signature is None:
                task.compute_and_set_signature()
            
            signature = task.task_signature
            dedup_key = self._get_dedup_key(signature)
            
            # Check if signature exists
            existing_task_id = redis_broker.client.get(dedup_key)
            
            if existing_task_id is None:
                return None
            
            # Decode if bytes
            if isinstance(existing_task_id, bytes):
                existing_task_id = existing_task_id.decode()
            
            # Check task status
            task_status = self._get_task_status(existing_task_id)
            
            if task_status is None:
                # Task not found, signature is stale
                log.debug(
                    f"Stale dedup entry for signature {signature}, removing",
                    extra={"signature": signature, "task_id": existing_task_id}
                )
                redis_broker.client.delete(dedup_key)
                return None
            
            # If task is pending/running, return existing task_id
            if task_status in [TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.RETRY]:
                log.info(
                    f"Duplicate task detected (pending/running): {existing_task_id}",
                    extra={
                        "signature": signature,
                        "existing_task_id": existing_task_id,
                        "new_task_id": task.id,
                        "status": task_status.value
                    }
                )
                return existing_task_id
            
            # If task is completed and re-execution not allowed
            if not allow_re_execution:
                log.info(
                    f"Duplicate task detected (completed): {existing_task_id}",
                    extra={
                        "signature": signature,
                        "existing_task_id": existing_task_id,
                        "new_task_id": task.id,
                        "status": task_status.value
                    }
                )
                return existing_task_id
            
            # Task completed but re-execution allowed
            log.debug(
                f"Duplicate task completed, allowing re-execution",
                extra={
                    "signature": signature,
                    "existing_task_id": existing_task_id,
                    "new_task_id": task.id
                }
            )
            return None
            
        except Exception as e:
            log.error(
                f"Error checking duplicate for task {task.id}: {e}",
                extra={"task_id": task.id, "error": str(e)}
            )
            return None
    
    def register_task(self, task: Task, ttl: Optional[int] = None) -> bool:
        """
        Register a task signature to prevent duplicates.
        
        Args:
            task: Task to register
            ttl: TTL in seconds (uses default if None)
            
        Returns:
            True if registered successfully
        """
        try:
            # Generate signature if not present
            if task.task_signature is None:
                task.compute_and_set_signature()
            
            signature = task.task_signature
            dedup_key = self._get_dedup_key(signature)
            
            # Use provided TTL or default
            result_ttl = ttl if ttl is not None else self.default_ttl
            
            # Store signature -> task_id mapping
            redis_broker.client.setex(
                dedup_key,
                result_ttl,
                task.id
            )
            
            # Also store task status for quick lookup
            status_key = self._get_task_status_key(task.id)
            redis_broker.client.setex(
                status_key,
                result_ttl,
                task.status.value
            )
            
            log.debug(
                f"Registered task signature for deduplication",
                extra={
                    "task_id": task.id,
                    "signature": signature,
                    "ttl": result_ttl
                }
            )
            
            return True
            
        except Exception as e:
            log.error(
                f"Failed to register task signature: {e}",
                extra={"task_id": task.id, "error": str(e)}
            )
            return False
    
    def update_task_status(self, task: Task) -> bool:
        """
        Update task status in deduplication tracking.
        
        Args:
            task: Task with updated status
            
        Returns:
            True if updated successfully
        """
        try:
            status_key = self._get_task_status_key(task.id)
            
            # Get current TTL
            ttl = redis_broker.client.ttl(status_key)
            
            if ttl > 0:
                # Update status with same TTL
                redis_broker.client.setex(
                    status_key,
                    ttl,
                    task.status.value
                )
                
                log.debug(
                    f"Updated task status in deduplication",
                    extra={
                        "task_id": task.id,
                        "status": task.status.value
                    }
                )
            
            return True
            
        except Exception as e:
            log.error(f"Failed to update task status: {e}")
            return False
    
    def _get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """
        Get task status from Redis.
        
        Args:
            task_id: Task ID
            
        Returns:
            TaskStatus or None if not found
        """
        try:
            status_key = self._get_task_status_key(task_id)
            status_str = redis_broker.client.get(status_key)
            
            if status_str is None:
                # Try to get from result backend
                result = result_backend.get_result(task_id)
                if result:
                    return result.status
                return None
            
            if isinstance(status_str, bytes):
                status_str = status_str.decode()
            
            return TaskStatus(status_str)
            
        except Exception as e:
            log.error(f"Error getting task status: {e}")
            return None
    
    def remove_task(self, task: Task) -> bool:
        """
        Remove task from deduplication tracking.
        
        Args:
            task: Task to remove
            
        Returns:
            True if removed successfully
        """
        try:
            if task.task_signature is None:
                return False
            
            signature = task.task_signature
            dedup_key = self._get_dedup_key(signature)
            
            # Check if this task is the one registered
            existing_task_id = redis_broker.client.get(dedup_key)
            
            if existing_task_id:
                if isinstance(existing_task_id, bytes):
                    existing_task_id = existing_task_id.decode()
                
                # Only remove if it's the same task
                if existing_task_id == task.id:
                    redis_broker.client.delete(dedup_key)
                    log.debug(f"Removed task from deduplication: {task.id}")
            
            # Remove status tracking
            status_key = self._get_task_status_key(task.id)
            redis_broker.client.delete(status_key)
            
            return True
            
        except Exception as e:
            log.error(f"Failed to remove task from deduplication: {e}")
            return False
    
    def get_duplicate_info(self, signature: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a duplicate task.
        
        Args:
            signature: Task signature
            
        Returns:
            Dictionary with duplicate info or None
        """
        try:
            dedup_key = self._get_dedup_key(signature)
            existing_task_id = redis_broker.client.get(dedup_key)
            
            if existing_task_id is None:
                return None
            
            if isinstance(existing_task_id, bytes):
                existing_task_id = existing_task_id.decode()
            
            status = self._get_task_status(existing_task_id)
            
            info = {
                "task_id": existing_task_id,
                "status": status.value if status else None,
                "signature": signature
            }
            
            # Get result if completed
            if status in [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.TIMEOUT]:
                result = result_backend.get_result(existing_task_id)
                if result:
                    info["result"] = result.result
                    info["error"] = result.error
                    info["duration"] = result.duration
            
            return info
            
        except Exception as e:
            log.error(f"Error getting duplicate info: {e}")
            return None
    
    def handle_hash_collision(self, signature: str, task_id: str) -> bool:
        """
        Handle potential hash collision by verifying task details.
        
        Args:
            signature: Task signature
            task_id: New task ID
            
        Returns:
            True if collision detected (same signature, different task)
        """
        try:
            dedup_key = self._get_dedup_key(signature)
            existing_task_id = redis_broker.client.get(dedup_key)
            
            if existing_task_id is None:
                return False
            
            if isinstance(existing_task_id, bytes):
                existing_task_id = existing_task_id.decode()
            
            # If same task_id, not a collision
            if existing_task_id == task_id:
                return False
            
            # Get both tasks and compare details
            # This is a simplified check - in production, you'd fetch full task details
            existing_status = self._get_task_status(existing_task_id)
            new_status = self._get_task_status(task_id)
            
            # If both exist and have different statuses, might be collision
            # In practice, hash collisions are extremely rare with SHA256
            if existing_status and new_status and existing_status != new_status:
                log.warning(
                    f"Potential hash collision detected for signature {signature}",
                    extra={
                        "signature": signature,
                        "existing_task_id": existing_task_id,
                        "new_task_id": task_id
                    }
                )
                return True
            
            return False
            
        except Exception as e:
            log.error(f"Error checking hash collision: {e}")
            return False


# Global deduplication instance
task_deduplication = TaskDeduplication(
    default_ttl=getattr(settings, 'dedup_ttl', 86400)
)

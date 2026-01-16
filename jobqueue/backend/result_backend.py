"""
Result backend for storing and retrieving task results.
Stores results in Redis with TTL support.
"""
from typing import Optional, Any
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.utils.logger import log
from config import settings


class TaskResult:
    """
    Task result model containing execution details.
    """
    
    def __init__(
        self,
        task_id: str,
        status: TaskStatus,
        result: Any = None,
        error: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        duration: Optional[float] = None
    ):
        """
        Initialize task result.
        
        Args:
            task_id: Task identifier
            status: Task status (SUCCESS, FAILED, etc.)
            result: Task return value (if successful)
            error: Error message (if failed)
            started_at: Task start time
            completed_at: Task completion time
            duration: Execution duration in seconds
        """
        self.task_id = task_id
        self.status = status
        self.result = result
        self.error = error
        self.started_at = started_at
        self.completed_at = completed_at
        self.duration = duration
    
    def to_dict(self) -> dict:
        """Convert result to dictionary."""
        return {
            "task_id": self.task_id,
            "status": self.status.value if isinstance(self.status, TaskStatus) else self.status,
            "result": self.result,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration": self.duration
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "TaskResult":
        """Create result from dictionary."""
        from jobqueue.core.serialization import deserialize_task_data
        
        # Parse datetime fields
        started_at = None
        if data.get("started_at"):
            started_at = datetime.fromisoformat(data["started_at"])
        
        completed_at = None
        if data.get("completed_at"):
            completed_at = datetime.fromisoformat(data["completed_at"])
        
        # Parse status
        status = data["status"]
        if isinstance(status, str):
            status = TaskStatus(status)
        
        # Deserialize result if present
        result = data.get("result")
        if result is not None:
            try:
                result = deserialize_task_data(result)
            except Exception:
                pass  # Keep as-is if deserialization fails
        
        return cls(
            task_id=data["task_id"],
            status=status,
            result=result,
            error=data.get("error"),
            started_at=started_at,
            completed_at=completed_at,
            duration=data.get("duration")
        )
    
    def to_json(self) -> str:
        """Convert result to JSON string."""
        import json
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> "TaskResult":
        """Create result from JSON string."""
        import json
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    @classmethod
    def from_task(cls, task: Task) -> "TaskResult":
        """
        Create TaskResult from a completed Task.
        
        Args:
            task: Completed task
            
        Returns:
            TaskResult instance
        """
        duration = task.execution_time()
        
        return cls(
            task_id=task.id,
            status=task.status,
            result=task.result if task.status == TaskStatus.SUCCESS else None,
            error=task.error if task.status != TaskStatus.SUCCESS else None,
            started_at=task.started_at,
            completed_at=task.completed_at,
            duration=duration
        )


class ResultBackend:
    """
    Backend for storing and retrieving task results in Redis.
    Results are stored with TTL (default 24 hours).
    """
    
    def __init__(self, default_ttl: int = 86400):
        """
        Initialize result backend.
        
        Args:
            default_ttl: Default TTL in seconds (default: 86400 = 24 hours)
        """
        self.default_ttl = default_ttl
        log.info(f"ResultBackend initialized with TTL: {default_ttl}s")
    
    def _get_result_key(self, task_id: str) -> str:
        """Get Redis key for task result."""
        return f"result:{task_id}"
    
    def store_result(
        self,
        task: Task,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Store task result in Redis.
        
        Args:
            task: Completed task
            ttl: TTL in seconds (uses default if None)
            
        Returns:
            True if stored successfully
        """
        try:
            # Create result from task
            result = TaskResult.from_task(task)
            
            # Get result key
            result_key = self._get_result_key(task.id)
            
            # Use provided TTL or default
            result_ttl = ttl if ttl is not None else self.default_ttl
            
            # Store in Redis
            result_json = result.to_json()
            redis_broker.set_with_ttl(result_key, result_json, result_ttl)
            
            log.debug(
                f"Stored result for task {task.id}",
                extra={
                    "task_id": task.id,
                    "status": task.status.value,
                    "result_key": result_key,
                    "ttl": result_ttl
                }
            )
            
            return True
            
        except Exception as e:
            log.error(
                f"Failed to store result for task {task.id}: {e}",
                extra={"task_id": task.id, "error": str(e)}
            )
            return False
    
    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """
        Retrieve task result from Redis.
        
        Args:
            task_id: Task identifier
            
        Returns:
            TaskResult or None if not found/expired
        """
        try:
            result_key = self._get_result_key(task_id)
            result_json = redis_broker.get(result_key)
            
            if result_json is None:
                log.debug(f"Result not found for task {task_id}")
                return None
            
            # Deserialize result
            if isinstance(result_json, bytes):
                result_json = result_json.decode()
            
            result = TaskResult.from_json(result_json)
            
            log.debug(f"Retrieved result for task {task_id}")
            return result
            
        except Exception as e:
            log.error(
                f"Failed to retrieve result for task {task_id}: {e}",
                extra={"task_id": task_id, "error": str(e)}
            )
            return None
    
    def delete_result(self, task_id: str) -> bool:
        """
        Delete task result from Redis.
        
        Args:
            task_id: Task identifier
            
        Returns:
            True if deleted successfully
        """
        try:
            result_key = self._get_result_key(task_id)
            deleted = redis_broker.client.delete(result_key)
            
            if deleted:
                log.debug(f"Deleted result for task {task_id}")
            else:
                log.debug(f"Result not found for task {task_id}")
            
            return bool(deleted)
            
        except Exception as e:
            log.error(
                f"Failed to delete result for task {task_id}: {e}",
                extra={"task_id": task_id, "error": str(e)}
            )
            return False
    
    def result_exists(self, task_id: str) -> bool:
        """
        Check if result exists for a task.
        
        Args:
            task_id: Task identifier
            
        Returns:
            True if result exists
        """
        try:
            result_key = self._get_result_key(task_id)
            exists = redis_broker.client.exists(result_key)
            return bool(exists)
        except Exception as e:
            log.error(f"Failed to check result existence: {e}")
            return False
    
    def get_result_ttl(self, task_id: str) -> Optional[int]:
        """
        Get remaining TTL for a result.
        
        Args:
            task_id: Task identifier
            
        Returns:
            TTL in seconds or None if not found
        """
        try:
            result_key = self._get_result_key(task_id)
            ttl = redis_broker.client.ttl(result_key)
            
            if ttl == -2:  # Key doesn't exist
                return None
            if ttl == -1:  # Key exists but no TTL
                return -1
            
            return ttl
            
        except Exception as e:
            log.error(f"Failed to get result TTL: {e}")
            return None
    
    def extend_result_ttl(self, task_id: str, ttl: int) -> bool:
        """
        Extend TTL for an existing result.
        
        Args:
            task_id: Task identifier
            ttl: New TTL in seconds
            
        Returns:
            True if extended successfully
        """
        try:
            result_key = self._get_result_key(task_id)
            extended = redis_broker.client.expire(result_key, ttl)
            
            if extended:
                log.debug(f"Extended TTL for task {task_id} to {ttl}s")
            else:
                log.debug(f"Result not found for task {task_id}")
            
            return bool(extended)
            
        except Exception as e:
            log.error(f"Failed to extend result TTL: {e}")
            return False


# Global result backend instance
result_backend = ResultBackend(default_ttl=getattr(settings, 'result_ttl', 86400))

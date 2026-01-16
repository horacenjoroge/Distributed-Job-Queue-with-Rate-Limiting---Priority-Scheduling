"""
Task models and status definitions.
"""
from enum import Enum
from typing import Optional, Any, List, Dict
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import uuid
import json
import hashlib


class TaskStatus(str, Enum):
    """Task status enumeration."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(str, Enum):
    """Task priority levels."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class Task(BaseModel):
    """
    Task model representing a unit of work to be executed.
    Supports versioning, advanced serialization, and validation.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    priority: TaskPriority = TaskPriority.MEDIUM
    status: TaskStatus = TaskStatus.PENDING
    queue_name: str = "default"
    
    # Task execution details
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    result: Optional[Any] = None
    error: Optional[str] = None
    
    # Retry configuration
    retry_count: int = 0
    max_retries: int = 3
    timeout: int = 300  # seconds
    retry_history: List[Dict[str, Any]] = Field(default_factory=list)  # Track retry attempts
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Scheduling
    eta: Optional[datetime] = None  # Execute after this time (absolute)
    countdown: Optional[int] = None  # Execute in X seconds (relative)
    schedule_time: Optional[datetime] = None  # Calculated execution time
    
    # Recurring tasks
    cron_expression: Optional[str] = None  # Cron-like schedule
    is_recurring: bool = False
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    
    # Worker information
    worker_id: Optional[str] = None
    
    # Task dependencies
    parent_task_id: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)
    
    # Versioning and metadata
    version: str = "1.0"
    task_signature: Optional[str] = None
    serialization_format: str = "json"
    
    # Deduplication
    unique: bool = False  # If True, prevent duplicate execution
    
    class Config:
        use_enum_values = True
    
    def generate_signature(self) -> str:
        """
        Generate a unique signature for task deduplication.
        Based on task name, args, and kwargs.
        
        Returns:
            SHA256 hash of task signature
        """
        signature_data = {
            "name": self.name,
            "args": str(self.args),
            "kwargs": str(sorted(self.kwargs.items())),
        }
        signature_str = json.dumps(signature_data, sort_keys=True)
        return hashlib.sha256(signature_str.encode()).hexdigest()
    
    def compute_and_set_signature(self) -> None:
        """Compute and set the task signature."""
        self.task_signature = self.generate_signature()
    
    def calculate_schedule_time(self) -> Optional[datetime]:
        """
        Calculate the scheduled execution time based on eta or countdown.
        
        Returns:
            Scheduled execution time, or None if not scheduled
        """
        if self.eta:
            return self.eta
        elif self.countdown:
            return datetime.utcnow() + timedelta(seconds=self.countdown)
        return None
    
    def set_schedule_time(self) -> None:
        """Calculate and set the schedule_time field."""
        self.schedule_time = self.calculate_schedule_time()
    
    def is_scheduled(self) -> bool:
        """Check if task is scheduled for future execution."""
        return self.schedule_time is not None or self.eta is not None or self.countdown is not None
    
    def is_ready_to_execute(self) -> bool:
        """
        Check if scheduled task is ready to execute.
        
        Returns:
            True if task should be executed now
        """
        if not self.schedule_time:
            return True  # Not scheduled, ready immediately
        
        return datetime.utcnow() >= self.schedule_time
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary for database storage."""
        return {
            "id": self.id,
            "name": self.name,
            "priority": self.priority,
            "status": self.status,
            "queue_name": self.queue_name,
            "args": json.dumps(self.args),
            "kwargs": json.dumps(self.kwargs),
            "result": json.dumps(self.result) if self.result else None,
            "error": self.error,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "timeout": self.timeout,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "worker_id": self.worker_id,
            "parent_task_id": self.parent_task_id,
            "depends_on": json.dumps(self.depends_on),
        }
    
    def to_json(self) -> str:
        """Serialize task to JSON string."""
        return self.model_dump_json()
    
    @classmethod
    def from_json(cls, json_str: str) -> "Task":
        """Deserialize task from JSON string."""
        return cls.model_validate_json(json_str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """Create task from dictionary."""
        # Parse JSON fields
        if "args" in data and isinstance(data["args"], str):
            data["args"] = json.loads(data["args"])
        if "kwargs" in data and isinstance(data["kwargs"], str):
            data["kwargs"] = json.loads(data["kwargs"])
        if "result" in data and isinstance(data["result"], str) and data["result"]:
            data["result"] = json.loads(data["result"])
        if "depends_on" in data and isinstance(data["depends_on"], str):
            data["depends_on"] = json.loads(data["depends_on"])
        
        # Parse datetime fields
        for field in ["created_at", "started_at", "completed_at"]:
            if field in data and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])
        
        return cls(**data)
    
    def is_ready(self) -> bool:
        """Check if task is ready to be executed (no pending dependencies)."""
        return len(self.depends_on) == 0
    
    def can_retry(self) -> bool:
        """Check if task can be retried."""
        return self.retry_count < self.max_retries
    
    def increment_retry(self) -> None:
        """Increment retry count."""
        self.retry_count += 1
        self.status = TaskStatus.RETRY
    
    def mark_running(self, worker_id: str) -> None:
        """Mark task as running."""
        self.status = TaskStatus.RUNNING
        self.worker_id = worker_id
        self.started_at = datetime.utcnow()
    
    def mark_success(self, result: Any = None) -> None:
        """Mark task as successfully completed."""
        self.status = TaskStatus.SUCCESS
        self.result = result
        self.completed_at = datetime.utcnow()
    
    def mark_failed(self, error: str) -> None:
        """Mark task as failed."""
        self.status = TaskStatus.FAILED
        self.error = error
        self.completed_at = datetime.utcnow()
    
    def mark_timeout(self) -> None:
        """Mark task as timed out."""
        self.status = TaskStatus.TIMEOUT
        self.error = f"Task exceeded timeout of {self.timeout} seconds"
        self.completed_at = datetime.utcnow()
    
    def mark_cancelled(self, reason: Optional[str] = None) -> None:
        """
        Mark task as cancelled.
        
        Args:
            reason: Cancellation reason
        """
        self.status = TaskStatus.CANCELLED
        if reason:
            self.error = f"Cancelled: {reason}"
        self.completed_at = datetime.utcnow()
    
    def record_retry_attempt(self, error: str, backoff_seconds: float) -> None:
        """
        Record a retry attempt in the task history.
        
        Args:
            error: Error message from failed attempt
            backoff_seconds: Backoff delay before next retry
        """
        retry_record = {
            "attempt": self.retry_count,
            "timestamp": datetime.utcnow().isoformat(),
            "error": error,
            "backoff_seconds": backoff_seconds,
            "worker_id": self.worker_id
        }
        self.retry_history.append(retry_record)
    
    def get_queue_key(self) -> str:
        """Get the Redis queue key for this task."""
        return f"queue:{self.queue_name}:{self.priority}"
    
    def get_result_key(self) -> str:
        """Get the Redis result key for this task."""
        return f"result:{self.id}"
    
    def execution_time(self) -> Optional[float]:
        """
        Calculate task execution time in seconds.
        
        Returns:
            Execution time in seconds or None if not completed
        """
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def serialize_with_format(self, format: str = "json") -> str:
        """
        Serialize task using advanced serializer.
        
        Args:
            format: Serialization format ('json' or 'pickle')
            
        Returns:
            Serialized task string
        """
        from jobqueue.core.serialization import task_serializer
        
        task_data = self.model_dump()
        return task_serializer.serialize(task_data, format=format)
    
    @classmethod
    def deserialize_with_format(cls, data: str, format: str = "json") -> "Task":
        """
        Deserialize task using advanced serializer.
        
        Args:
            data: Serialized task string
            format: Serialization format ('json' or 'pickle')
            
        Returns:
            Task instance
        """
        from jobqueue.core.serialization import task_serializer
        
        task_data = task_serializer.deserialize(data, format=format)
        return cls(**task_data)
    
    def validate_signature(self, func) -> bool:
        """
        Validate task args/kwargs against function signature.
        
        Args:
            func: Function to validate against
            
        Returns:
            True if valid
            
        Raises:
            ValidationError: If validation fails
        """
        from jobqueue.core.validation import task_validator
        
        return task_validator.validate_task_signature(
            func,
            tuple(self.args),
            self.kwargs
        )
    
    def is_duplicate_of(self, other: "Task") -> bool:
        """
        Check if this task is a duplicate of another task.
        Based on task signature.
        
        Args:
            other: Other task to compare with
            
        Returns:
            True if duplicate
        """
        if self.task_signature is None:
            self.compute_and_set_signature()
        if other.task_signature is None:
            other.compute_and_set_signature()
        
        return self.task_signature == other.task_signature

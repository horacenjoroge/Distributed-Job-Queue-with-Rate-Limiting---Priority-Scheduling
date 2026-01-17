"""
Unit tests for Task model.
"""
import pytest
from datetime import datetime
from jobqueue.core.task import Task, TaskStatus, TaskPriority, WorkerType


@pytest.mark.unit
class TestTask:
    """Test Task model."""
    
    def test_task_creation(self):
        """Test creating a task."""
        task = Task(
            name="test_task",
            args=[1, 2, 3],
            kwargs={"key": "value"},
            priority=TaskPriority.HIGH,
            max_retries=5,
            timeout=300
        )
        
        assert task.name == "test_task"
        assert task.args == [1, 2, 3]
        assert task.kwargs == {"key": "value"}
        assert task.priority == TaskPriority.HIGH
        assert task.max_retries == 5
        assert task.timeout == 300
        assert task.status == TaskStatus.PENDING
        assert task.retry_count == 0
        assert task.id is not None
        assert task.created_at is not None
    
    def test_task_defaults(self):
        """Test task with default values."""
        task = Task(name="test_task")
        
        assert task.priority == TaskPriority.MEDIUM
        assert task.max_retries == 3
        assert task.timeout == 300
        assert task.queue_name == "default"
        assert task.args == []
        assert task.kwargs == {}
    
    def test_task_mark_running(self):
        """Test marking task as running."""
        task = Task(name="test_task")
        worker_id = "worker-1"
        
        task.mark_running(worker_id)
        
        assert task.status == TaskStatus.RUNNING
        assert task.worker_id == worker_id
        assert task.started_at is not None
    
    def test_task_mark_success(self):
        """Test marking task as successful."""
        task = Task(name="test_task")
        result = {"output": "success"}
        
        task.mark_success(result)
        
        assert task.status == TaskStatus.SUCCESS
        assert task.result == result
        assert task.completed_at is not None
    
    def test_task_mark_failed(self):
        """Test marking task as failed."""
        task = Task(name="test_task")
        error = "Something went wrong"
        
        task.mark_failed(error)
        
        assert task.status == TaskStatus.FAILED
        assert task.error == error
        assert task.completed_at is not None
    
    def test_task_mark_cancelled(self):
        """Test marking task as cancelled."""
        task = Task(name="test_task")
        reason = "User requested"
        
        task.mark_cancelled(reason)
        
        assert task.status == TaskStatus.CANCELLED
        assert task.error == reason
        assert task.completed_at is not None
    
    def test_task_mark_timeout(self):
        """Test marking task as timeout."""
        task = Task(name="test_task")
        
        task.mark_timeout()
        
        assert task.status == TaskStatus.TIMEOUT
        assert task.completed_at is not None
    
    def test_task_can_retry(self):
        """Test checking if task can be retried."""
        task = Task(name="test_task", max_retries=3)
        
        assert task.can_retry() is True
        
        task.retry_count = 3
        assert task.can_retry() is False
    
    def test_task_increment_retry(self):
        """Test incrementing retry count."""
        task = Task(name="test_task", max_retries=3)
        
        task.increment_retry()
        assert task.retry_count == 1
        
        task.increment_retry()
        assert task.retry_count == 2
    
    def test_task_execution_time(self):
        """Test calculating execution time."""
        task = Task(name="test_task")
        task.started_at = datetime.utcnow()
        
        # No completed_at yet
        assert task.execution_time() is None
        
        task.completed_at = datetime.utcnow()
        duration = task.execution_time()
        assert duration is not None
        assert duration >= 0
    
    def test_task_is_ready(self):
        """Test checking if task is ready (no dependencies)."""
        task = Task(name="test_task")
        
        assert task.is_ready() is True
        
        task.depends_on = ["task-1", "task-2"]
        assert task.is_ready() is False
    
    def test_task_to_dict(self):
        """Test converting task to dictionary."""
        task = Task(name="test_task", args=[1, 2], kwargs={"k": "v"})
        task_dict = task.to_dict()
        
        assert task_dict["name"] == "test_task"
        assert task_dict["args"] == [1, 2]
        assert task_dict["kwargs"] == {"k": "v"}
        assert "id" in task_dict
        assert "created_at" in task_dict
    
    def test_task_from_dict(self):
        """Test creating task from dictionary."""
        task_dict = {
            "id": "test-id",
            "name": "test_task",
            "status": "pending",
            "priority": "high",
            "args": [1, 2],
            "kwargs": {"k": "v"},
            "retry_count": 0,
            "max_retries": 3,
            "timeout": 300,
            "queue_name": "default",
            "created_at": datetime.utcnow().isoformat(),
            "depends_on": []
        }
        
        task = Task.from_dict(task_dict)
        
        assert task.id == "test-id"
        assert task.name == "test_task"
        assert task.status == TaskStatus.PENDING
        assert task.priority == TaskPriority.HIGH
    
    def test_task_to_json(self):
        """Test converting task to JSON."""
        task = Task(name="test_task")
        json_str = task.to_json()
        
        assert isinstance(json_str, str)
        assert "test_task" in json_str
    
    def test_task_from_json(self):
        """Test creating task from JSON."""
        task = Task(name="test_task", args=[1, 2])
        json_str = task.to_json()
        
        task2 = Task.from_json(json_str)
        
        assert task2.name == "test_task"
        assert task2.args == [1, 2]
    
    def test_task_get_queue_key(self):
        """Test getting queue key."""
        task = Task(name="test_task", queue_name="my_queue", priority=TaskPriority.HIGH)
        key = task.get_queue_key()
        
        assert key == "queue:my_queue:high"
    
    def test_task_get_result_key(self):
        """Test getting result key."""
        task = Task(name="test_task", id="task-123")
        key = task.get_result_key()
        
        assert key == "result:task-123"
    
    def test_task_compute_signature(self):
        """Test computing task signature."""
        task = Task(name="test_task", args=[1, 2], kwargs={"k": "v"})
        signature = task.compute_signature()
        
        assert signature is not None
        assert isinstance(signature, str)
        assert len(signature) > 0
    
    def test_task_signature_consistency(self):
        """Test that same task produces same signature."""
        task1 = Task(name="test_task", args=[1, 2], kwargs={"k": "v"})
        task2 = Task(name="test_task", args=[1, 2], kwargs={"k": "v"})
        
        sig1 = task1.compute_signature()
        sig2 = task2.compute_signature()
        
        assert sig1 == sig2
    
    def test_task_signature_different_args(self):
        """Test that different args produce different signatures."""
        task1 = Task(name="test_task", args=[1, 2])
        task2 = Task(name="test_task", args=[3, 4])
        
        sig1 = task1.compute_signature()
        sig2 = task2.compute_signature()
        
        assert sig1 != sig2
    
    def test_task_worker_type(self):
        """Test task worker type."""
        task = Task(name="test_task", worker_type=WorkerType.CPU)
        
        assert task.worker_type == WorkerType.CPU
    
    def test_task_record_retry_attempt(self):
        """Test recording retry attempt."""
        task = Task(name="test_task")
        
        task.record_retry_attempt(error="Error", backoff_seconds=5.0)
        
        assert len(task.retry_history) == 1
        assert task.retry_history[0]["error"] == "Error"
        assert task.retry_history[0]["backoff_seconds"] == 5.0

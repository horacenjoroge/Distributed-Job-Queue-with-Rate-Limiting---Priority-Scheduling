"""
Unit tests for JobQueue.
"""
import pytest
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority, TaskStatus


@pytest.mark.unit
class TestJobQueue:
    """Test JobQueue class."""
    
    def test_queue_initialization(self):
        """Test queue initialization."""
        queue = JobQueue(name="test_queue")
        
        assert queue.name == "test_queue"
        assert queue.rate_limits is not None
    
    def test_submit_task(self, mock_redis_broker, mock_postgres_backend):
        """Test submitting a task."""
        queue = JobQueue(name="test_queue")
        
        task = queue.submit_task(
            task_name="test_task",
            args=[1, 2, 3],
            kwargs={"key": "value"},
            priority=TaskPriority.HIGH,
            max_retries=5,
            timeout=600
        )
        
        assert task.name == "test_task"
        assert task.args == [1, 2, 3]
        assert task.kwargs == {"key": "value"}
        assert task.priority == TaskPriority.HIGH
        assert task.max_retries == 5
        assert task.timeout == 600
        assert task.id is not None
        assert task.queue_name == "test_queue"
    
    def test_get_task(self, mock_redis_broker, mock_postgres_backend):
        """Test retrieving a task."""
        queue = JobQueue(name="test_queue")
        
        # Submit task
        submitted_task = queue.submit_task("test_task", args=[1, 2])
        
        # Retrieve task
        retrieved_task = queue.get_task(submitted_task.id)
        
        assert retrieved_task is not None
        assert retrieved_task.id == submitted_task.id
        assert retrieved_task.name == "test_task"
    
    def test_get_nonexistent_task(self, mock_redis_broker, mock_postgres_backend):
        """Test retrieving a non-existent task."""
        queue = JobQueue(name="test_queue")
        
        task = queue.get_task("nonexistent-id")
        
        assert task is None
    
    def test_get_queue_size(self, mock_redis_broker):
        """Test getting queue size."""
        queue = JobQueue(name="test_queue")
        
        size = queue.get_queue_size()
        
        assert isinstance(size, int)
        assert size >= 0
    
    def test_get_queue_size_by_priority(self, mock_redis_broker):
        """Test getting queue size by priority."""
        queue = JobQueue(name="test_queue")
        
        high_size = queue.get_queue_size(TaskPriority.HIGH)
        medium_size = queue.get_queue_size(TaskPriority.MEDIUM)
        low_size = queue.get_queue_size(TaskPriority.LOW)
        
        assert isinstance(high_size, int)
        assert isinstance(medium_size, int)
        assert isinstance(low_size, int)
        assert high_size >= 0
        assert medium_size >= 0
        assert low_size >= 0
    
    def test_get_queue_stats(self, mock_redis_broker, mock_postgres_backend):
        """Test getting queue statistics."""
        queue = JobQueue(name="test_queue")
        
        # Submit some tasks
        queue.submit_task("test_task", args=[1])
        queue.submit_task("test_task", args=[2])
        
        stats = queue.get_queue_stats()
        
        assert "queue_name" in stats
        assert "queued_by_priority" in stats
        assert "total_queued" in stats
        assert stats["queue_name"] == "test_queue"

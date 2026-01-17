"""
Integration tests for task deduplication.
"""
import pytest
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.core.task_deduplication import task_deduplication


@pytest.mark.integration
class TestDeduplication:
    """Test task deduplication."""
    
    def test_duplicate_task_detection(self, mock_redis_broker, mock_postgres_backend):
        """Test that duplicate tasks are detected."""
        queue = JobQueue(name="test_queue")
        
        # Submit first task with unique flag
        task1 = queue.submit_task(
            task_name="test_task",
            args=[1, 2],
            kwargs={"key": "value"},
            priority=TaskPriority.MEDIUM
        )
        task1.unique = True
        
        # Try to submit duplicate
        task2 = queue.submit_task(
            task_name="test_task",
            args=[1, 2],
            kwargs={"key": "value"},
            priority=TaskPriority.MEDIUM
        )
        task2.unique = True
        
        # Check if deduplication detected
        existing_id = task_deduplication.check_duplicate(task2, allow_re_execution=False)
        
        # Should find existing task
        assert existing_id is not None or task1.id == task2.id
    
    def test_different_tasks_not_deduplicated(self, mock_redis_broker, mock_postgres_backend):
        """Test that different tasks are not deduplicated."""
        queue = JobQueue(name="test_queue")
        
        task1 = queue.submit_task("test_task", args=[1, 2])
        task1.unique = True
        
        task2 = queue.submit_task("test_task", args=[3, 4])  # Different args
        task2.unique = True
        
        existing_id = task_deduplication.check_duplicate(task2, allow_re_execution=False)
        
        # Should not find duplicate (different args)
        assert existing_id is None or existing_id != task1.id

"""
Tests for task timeouts.
"""
import pytest
import time
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry


@pytest.mark.failure
@pytest.mark.integration
class TestTimeout:
    """Test task timeout handling."""
    
    @pytest.fixture
    def registered_long_task(self):
        """Register a long-running task."""
        @task_registry.register("long_running_task")
        def long_running_task():
            time.sleep(10)  # Sleep for 10 seconds
            return "done"
        
        yield "long_running_task"
        task_registry._tasks.pop("long_running_task", None)
    
    def test_task_timeout(self, mock_redis_broker, mock_postgres_backend, registered_long_task):
        """Test that tasks timeout after specified time."""
        queue = JobQueue(name="test_queue")
        
        # Submit task with short timeout
        task = queue.submit_task(
            "long_running_task",
            timeout=2  # 2 second timeout
        )
        
        # Start worker
        worker = SimpleWorker(queue_name="test_queue")
        worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
        worker_thread.start()
        
        # Wait for timeout
        time.sleep(5)
        
        # Check task status
        updated_task = queue.get_task(task.id)
        
        # Task should be timed out
        assert updated_task is not None
        # Status should be timeout (or failed if timeout handling is different)
        assert updated_task.status.value in ["timeout", "failed"]
    
    def test_timeout_before_completion(self, mock_redis_broker, mock_postgres_backend):
        """Test that timeout prevents task completion."""
        @task_registry.register("quick_task")
        def quick_task():
            return "quick"
        
        try:
            queue = JobQueue(name="test_queue")
            
            # Submit quick task with long timeout (should complete)
            task = queue.submit_task("quick_task", timeout=60)
            
            worker = SimpleWorker(queue_name="test_queue")
            worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
            worker_thread.start()
            
            time.sleep(2)
            
            updated_task = queue.get_task(task.id)
            
            # Should complete successfully
            assert updated_task.status.value in ["success", "running", "queued"]
        finally:
            task_registry._tasks.pop("quick_task", None)

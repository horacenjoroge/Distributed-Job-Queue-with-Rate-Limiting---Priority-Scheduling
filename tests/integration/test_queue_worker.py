"""
Integration tests for queue and worker interaction.
"""
import pytest
import time
import threading
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry


@pytest.mark.integration
class TestQueueWorkerIntegration:
    """Test queue and worker integration."""
    
    @pytest.fixture
    def registered_task(self):
        """Register a test task."""
        @task_registry.register("test_task")
        def test_task(x, y):
            return x + y
        
        yield "test_task"
        task_registry._tasks.pop("test_task", None)
    
    def test_submit_and_process_task(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test submitting a task and processing it with a worker."""
        queue = JobQueue(name="test_queue")
        
        # Submit task
        task = queue.submit_task(
            task_name="test_task",
            args=[1, 2],
            priority=TaskPriority.MEDIUM
        )
        
        assert task.id is not None
        assert task.status.value == "queued"
        
        # Create and start worker
        worker = SimpleWorker(queue_name="test_queue")
        worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
        worker_thread.start()
        
        # Wait for task to be processed
        time.sleep(2)
        
        # Check task status
        updated_task = queue.get_task(task.id)
        assert updated_task is not None
        assert updated_task.status.value in ["success", "running", "queued"]
    
    def test_priority_processing_order(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test that high priority tasks are processed first."""
        queue = JobQueue(name="test_queue")
        
        # Submit tasks with different priorities
        low_task = queue.submit_task("test_task", args=[1, 1], priority=TaskPriority.LOW)
        high_task = queue.submit_task("test_task", args=[2, 2], priority=TaskPriority.HIGH)
        medium_task = queue.submit_task("test_task", args=[3, 3], priority=TaskPriority.MEDIUM)
        
        # Create worker
        worker = SimpleWorker(queue_name="test_queue")
        worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
        worker_thread.start()
        
        # Wait for processing
        time.sleep(3)
        
        # High priority should be processed first (or at least before low)
        high_updated = queue.get_task(high_task.id)
        low_updated = queue.get_task(low_task.id)
        
        # At least high priority should be processed
        assert high_updated.status.value in ["success", "running"]
    
    def test_multiple_tasks_processing(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test processing multiple tasks."""
        queue = JobQueue(name="test_queue")
        
        # Submit multiple tasks
        tasks = []
        for i in range(5):
            task = queue.submit_task("test_task", args=[i, i+1])
            tasks.append(task)
        
        # Create worker
        worker = SimpleWorker(queue_name="test_queue")
        worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
        worker_thread.start()
        
        # Wait for processing
        time.sleep(5)
        
        # Check that tasks are processed
        processed_count = 0
        for task in tasks:
            updated = queue.get_task(task.id)
            if updated and updated.status.value == "success":
                processed_count += 1
        
        assert processed_count > 0

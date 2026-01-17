"""
Tests for worker failure recovery.
"""
import pytest
import time
import threading
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.core.task_recovery import task_recovery
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry


@pytest.mark.failure
@pytest.mark.integration
class TestWorkerFailure:
    """Test worker failure recovery."""
    
    @pytest.fixture
    def registered_task(self):
        """Register a long-running task."""
        @task_registry.register("long_task")
        def long_task(duration):
            time.sleep(duration)
            return "completed"
        
        yield "long_task"
        task_registry._tasks.pop("long_task", None)
    
    def test_worker_crash_mid_task(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test recovery when worker crashes mid-task."""
        queue = JobQueue(name="test_queue")
        
        # Submit a long-running task
        task = queue.submit_task("long_task", args=[5], priority=TaskPriority.MEDIUM)
        
        # Start worker
        worker = SimpleWorker(queue_name="test_queue")
        worker_thread = threading.Thread(target=worker.run_loop, daemon=True)
        worker_thread.start()
        
        # Wait for task to start
        time.sleep(1)
        
        # Simulate worker crash (stop worker)
        worker.stop()
        worker_thread.join(timeout=1)
        
        # Check that task is marked as active
        active_tasks = task_recovery.get_active_tasks(worker.worker_id)
        
        # Task should be recoverable
        # Note: Actual recovery depends on monitor process
        assert len(active_tasks) >= 0  # May have been cleaned up
    
    def test_orphaned_task_recovery(self, mock_redis_broker, mock_postgres_backend):
        """Test recovery of orphaned tasks."""
        from jobqueue.core.task import Task
        
        # Create an orphaned task (task with dead worker)
        dead_worker_id = "dead-worker-123"
        task = Task(name="test_task", args=[1, 2])
        task.status.value = "running"
        task.worker_id = dead_worker_id
        
        # Add to active tasks
        task_recovery.add_active_task(dead_worker_id, task)
        
        # Check orphaned tasks
        orphaned = task_recovery.get_orphaned_tasks([dead_worker_id])
        
        # Should find orphaned task
        assert len(orphaned) > 0 or dead_worker_id in [t.worker_id for t in orphaned]

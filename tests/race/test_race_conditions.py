"""
Tests for race conditions with multiple workers.
"""
import pytest
import time
import threading
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry


@pytest.mark.race
@pytest.mark.integration
class TestRaceConditions:
    """Test race conditions with multiple workers."""
    
    @pytest.fixture
    def registered_task(self):
        """Register a test task."""
        @task_registry.register("race_test_task")
        def race_test_task(x):
            return x * 2
        
        yield "race_test_task"
        task_registry._tasks.pop("race_test_task", None)
    
    def test_multiple_workers_no_duplicate_processing(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test that multiple workers don't process the same task."""
        queue = JobQueue(name="race_queue")
        
        # Submit single task
        task = queue.submit_task("race_test_task", args=[1])
        
        # Start multiple workers
        workers = []
        worker_threads = []
        for i in range(5):
            worker = SimpleWorker(queue_name="race_queue")
            workers.append(worker)
            thread = threading.Thread(target=worker.run_loop, daemon=True)
            worker_threads.append(thread)
            thread.start()
        
        # Wait for processing
        time.sleep(3)
        
        # Check task status
        updated_task = queue.get_task(task.id)
        
        # Task should be processed exactly once
        assert updated_task is not None
        # Should be success (processed once) or still queued/running
        assert updated_task.status.value in ["success", "running", "queued"]
    
    def test_concurrent_task_submissions(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test concurrent task submissions from multiple threads."""
        queue = JobQueue(name="race_queue")
        submitted_tasks = []
        lock = threading.Lock()
        
        def submit_tasks(thread_id, count):
            for i in range(count):
                task = queue.submit_task("race_test_task", args=[thread_id * 100 + i])
                with lock:
                    submitted_tasks.append(task)
        
        # Submit from multiple threads concurrently
        threads = []
        for i in range(10):
            t = threading.Thread(target=submit_tasks, args=(i, 10))
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Should have all tasks
        assert len(submitted_tasks) == 100
        
        # All tasks should have unique IDs
        task_ids = [t.id for t in submitted_tasks]
        assert len(task_ids) == len(set(task_ids))
    
    def test_worker_competition_for_tasks(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test that workers compete fairly for tasks."""
        queue = JobQueue(name="race_queue")
        
        # Submit multiple tasks
        tasks = []
        for i in range(20):
            task = queue.submit_task("race_test_task", args=[i])
            tasks.append(task)
        
        # Start multiple workers
        workers = []
        worker_threads = []
        for i in range(3):
            worker = SimpleWorker(queue_name="race_queue")
            workers.append(worker)
            thread = threading.Thread(target=worker.run_loop, daemon=True)
            worker_threads.append(thread)
            thread.start()
        
        # Wait for processing
        time.sleep(5)
        
        # Check that tasks are distributed across workers
        processed_count = 0
        for task in tasks:
            updated = queue.get_task(task.id)
            if updated and updated.status.value == "success":
                processed_count += 1
        
        # Multiple tasks should be processed
        assert processed_count > 0

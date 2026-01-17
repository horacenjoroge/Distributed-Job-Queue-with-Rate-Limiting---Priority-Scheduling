"""
Load tests for the job queue system.
"""
import pytest
import time
import threading
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.task_registry import task_registry


@pytest.mark.load
@pytest.mark.slow
class TestLoad:
    """Load tests."""
    
    @pytest.fixture
    def registered_task(self):
        """Register a simple task for load testing."""
        @task_registry.register("load_test_task")
        def load_test_task(x):
            return x * 2
        
        yield "load_test_task"
        task_registry._tasks.pop("load_test_task", None)
    
    def test_submit_1000_tasks(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test submitting 1000 tasks."""
        queue = JobQueue(name="load_queue")
        
        start_time = time.time()
        
        # Submit 1000 tasks
        tasks = []
        for i in range(1000):
            task = queue.submit_task("load_test_task", args=[i])
            tasks.append(task)
        
        submit_time = time.time() - start_time
        
        # All tasks should be submitted
        assert len(tasks) == 1000
        
        # Submission should be fast (< 10 seconds for 1000 tasks)
        assert submit_time < 10
    
    def test_process_1000_tasks(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test processing 1000 tasks with workers."""
        queue = JobQueue(name="load_queue")
        
        # Submit 1000 tasks
        tasks = []
        for i in range(1000):
            task = queue.submit_task("load_test_task", args=[i])
            tasks.append(task)
        
        # Start multiple workers
        workers = []
        worker_threads = []
        for i in range(5):  # 5 workers
            worker = SimpleWorker(queue_name="load_queue")
            workers.append(worker)
            thread = threading.Thread(target=worker.run_loop, daemon=True)
            worker_threads.append(thread)
            thread.start()
        
        # Wait for processing
        max_wait = 60  # 60 seconds max
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            # Check how many are completed
            completed = 0
            for task in tasks[:100]:  # Check first 100
                updated = queue.get_task(task.id)
                if updated and updated.status.value == "success":
                    completed += 1
            
            if completed >= 50:  # At least 50% of sample
                break
            
            time.sleep(1)
        
        # At least some tasks should be processed
        completed_count = 0
        for task in tasks[:100]:
            updated = queue.get_task(task.id)
            if updated and updated.status.value == "success":
                completed_count += 1
        
        assert completed_count > 0
    
    def test_concurrent_submissions(self, mock_redis_broker, mock_postgres_backend, registered_task):
        """Test concurrent task submissions."""
        queue = JobQueue(name="load_queue")
        tasks = []
        
        def submit_tasks(start, count):
            for i in range(start, start + count):
                task = queue.submit_task("load_test_task", args=[i])
                tasks.append(task)
        
        # Submit from multiple threads
        threads = []
        tasks_per_thread = 200
        num_threads = 5
        
        for i in range(num_threads):
            t = threading.Thread(
                target=submit_tasks,
                args=(i * tasks_per_thread, tasks_per_thread)
            )
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Should have all tasks
        assert len(tasks) == num_threads * tasks_per_thread

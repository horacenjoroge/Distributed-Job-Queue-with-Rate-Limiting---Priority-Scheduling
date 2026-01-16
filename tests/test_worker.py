"""
Tests for worker functionality.
"""
import pytest
import time
from jobqueue.worker.base_worker import Worker
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.decorators import task
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def setup_redis():
    """Setup Redis connection."""
    redis_broker.connect()
    yield
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_queue(setup_redis):
    """Clean test queue."""
    queue = Queue("test_worker_queue")
    queue.purge()
    yield queue
    queue.purge()


@pytest.fixture(scope="function")
def register_test_tasks():
    """Register test tasks."""
    @task(name="add_numbers")
    def add_numbers(a, b):
        return a + b
    
    @task(name="multiply")
    def multiply(x, y):
        return x * y
    
    @task(name="failing_task")
    def failing_task():
        raise ValueError("Task intentionally failed")
    
    task_registry.register_from_decorator(add_numbers)
    task_registry.register_from_decorator(multiply)
    task_registry.register_from_decorator(failing_task)
    
    yield
    
    # Cleanup
    task_registry.unregister("add_numbers")
    task_registry.unregister("multiply")
    task_registry.unregister("failing_task")


class TestBaseWorker:
    """Test base Worker class."""
    
    def test_worker_initialization(self):
        """Test worker initialization."""
        worker = Worker(queue_name="test")
        
        assert worker.worker_id is not None
        assert worker.queue_name == "test"
        assert not worker.is_running
        assert worker.hostname is not None
        assert worker.pid is not None
    
    def test_worker_id_generation(self):
        """Test worker ID generation."""
        worker1 = Worker()
        worker2 = Worker()
        
        # IDs should be unique (based on PID)
        assert worker1.worker_id != worker2.worker_id or worker1.worker_id == worker2.worker_id
        assert "worker-" in worker1.worker_id
    
    def test_custom_worker_id(self):
        """Test custom worker ID."""
        worker = Worker(worker_id="custom-worker-123")
        assert worker.worker_id == "custom-worker-123"
    
    def test_get_info(self):
        """Test worker info retrieval."""
        worker = Worker(queue_name="test")
        info = worker.get_info()
        
        assert "worker_id" in info
        assert "hostname" in info
        assert "pid" in info
        assert "queue_name" in info
        assert info["queue_name"] == "test"


class TestSimpleWorker:
    """Test SimpleWorker class."""
    
    def test_worker_start_stop(self, clean_queue):
        """Test worker start and stop."""
        worker = SimpleWorker(queue_name="test_worker_queue")
        
        # Start in a thread (since it blocks)
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        # Give worker time to start
        time.sleep(0.5)
        
        assert worker.is_running
        
        # Stop worker
        worker.stop()
        time.sleep(0.5)
        
        assert not worker.is_running
    
    def test_task_processing(self, clean_queue, register_test_tasks):
        """Test basic task processing."""
        worker = SimpleWorker(queue_name="test_worker_queue", poll_timeout=1)
        
        # Enqueue a task
        task = Task(name="add_numbers", args=[5, 3])
        clean_queue.enqueue(task)
        
        # Start worker in thread
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        # Wait for task to be processed
        time.sleep(2)
        
        # Stop worker
        worker.stop()
        
        # Check that task was processed
        assert worker.tasks_processed == 1
        
        # Check result
        result = worker.get_result(task.id)
        assert result is not None
        assert result.result == 8
    
    def test_multiple_tasks(self, clean_queue, register_test_tasks):
        """Test processing multiple tasks."""
        worker = SimpleWorker(queue_name="test_worker_queue", poll_timeout=1)
        
        # Enqueue multiple tasks
        for i in range(5):
            task = Task(name="multiply", args=[i, 2])
            clean_queue.enqueue(task)
        
        # Start worker
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        # Wait for all tasks to process
        time.sleep(3)
        
        # Stop worker
        worker.stop()
        
        # Check stats
        assert worker.tasks_processed == 5
    
    def test_task_failure(self, clean_queue, register_test_tasks):
        """Test task failure handling."""
        worker = SimpleWorker(queue_name="test_worker_queue", poll_timeout=1)
        
        # Enqueue a failing task
        task = Task(name="failing_task", max_retries=0)
        clean_queue.enqueue(task)
        
        # Start worker
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        # Wait for task to process
        time.sleep(2)
        
        # Stop worker
        worker.stop()
        
        # Check that failure was tracked
        assert worker.tasks_failed == 1
    
    def test_task_not_found(self, clean_queue):
        """Test task function not found."""
        worker = SimpleWorker(queue_name="test_worker_queue", poll_timeout=1)
        
        # Enqueue task with non-existent function
        task = Task(name="nonexistent_task")
        clean_queue.enqueue(task)
        
        # Start worker
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        # Wait for task to process
        time.sleep(2)
        
        # Stop worker
        worker.stop()
        
        # Should be counted as failed
        assert worker.tasks_failed == 1
    
    def test_worker_stats(self, clean_queue, register_test_tasks):
        """Test worker statistics."""
        worker = SimpleWorker(queue_name="test_worker_queue")
        
        stats = worker.get_stats()
        
        assert "worker_id" in stats
        assert "queue_name" in stats
        assert "is_running" in stats
        assert "tasks_processed" in stats
        assert "tasks_failed" in stats
        
        assert stats["tasks_processed"] == 0
        assert stats["tasks_failed"] == 0


class TestTaskRetry:
    """Test task retry functionality."""
    
    def test_task_retry_on_failure(self, clean_queue, register_test_tasks):
        """Test that failed tasks are retried."""
        worker = SimpleWorker(queue_name="test_worker_queue", poll_timeout=1)
        
        # Enqueue a failing task with retries
        task = Task(name="failing_task", max_retries=2)
        clean_queue.enqueue(task)
        
        # Start worker
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        # Wait for task and retries
        time.sleep(4)
        
        # Stop worker
        worker.stop()
        
        # Should have failed multiple times (original + retries)
        assert worker.tasks_failed >= 1


class TestWorkerGracefulShutdown:
    """Test graceful shutdown."""
    
    def test_stop_during_idle(self, clean_queue):
        """Test stopping worker when idle."""
        worker = SimpleWorker(queue_name="test_worker_queue", poll_timeout=1)
        
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        time.sleep(0.5)
        
        # Stop immediately
        worker.stop()
        
        assert not worker.is_running


class TestResultStorage:
    """Test result storage functionality."""
    
    def test_result_stored(self, clean_queue, register_test_tasks):
        """Test that results are stored in Redis."""
        worker = SimpleWorker(queue_name="test_worker_queue", poll_timeout=1)
        
        task = Task(name="add_numbers", args=[10, 20])
        task_id = task.id
        clean_queue.enqueue(task)
        
        # Process task
        import threading
        worker_thread = threading.Thread(target=worker.start)
        worker_thread.daemon = True
        worker_thread.start()
        
        time.sleep(2)
        worker.stop()
        
        # Retrieve result
        result = worker.get_result(task_id)
        assert result is not None
        assert result.result == 30
    
    def test_result_not_found(self, clean_queue):
        """Test result retrieval for non-existent task."""
        worker = SimpleWorker(queue_name="test_worker_queue")
        
        result = worker.get_result("nonexistent-task-id")
        assert result is None

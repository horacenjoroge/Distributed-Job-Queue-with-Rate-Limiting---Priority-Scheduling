"""
Tests for Redis FIFO queue implementation.
"""
import pytest
import time
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task, TaskPriority
from jobqueue.broker.redis_broker import redis_broker


@pytest.fixture(scope="function")
def setup_redis():
    """Setup Redis connection before each test."""
    redis_broker.connect()
    yield
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_queue(setup_redis):
    """Clean test queue before and after each test."""
    queue = Queue("test_queue")
    queue.purge()
    yield queue
    queue.purge()


class TestBasicQueueOperations:
    """Test basic queue operations."""
    
    def test_enqueue_single_task(self, clean_queue):
        """Test enqueuing a single task."""
        task = Task(name="test_task", args=[1, 2, 3])
        
        task_id = clean_queue.enqueue(task)
        
        assert task_id == task.id
        assert clean_queue.size() == 1
    
    def test_dequeue_single_task(self, clean_queue):
        """Test dequeuing a single task."""
        task = Task(name="test_task", args=[1, 2, 3])
        clean_queue.enqueue(task)
        
        dequeued_task = clean_queue.dequeue_nowait()
        
        assert dequeued_task is not None
        assert dequeued_task.id == task.id
        assert dequeued_task.name == task.name
        assert clean_queue.size() == 0
    
    def test_fifo_order(self, clean_queue):
        """Test FIFO ordering of tasks."""
        task1 = Task(name="task1")
        task2 = Task(name="task2")
        task3 = Task(name="task3")
        
        clean_queue.enqueue(task1)
        clean_queue.enqueue(task2)
        clean_queue.enqueue(task3)
        
        # Should dequeue in FIFO order
        dequeued1 = clean_queue.dequeue_nowait()
        dequeued2 = clean_queue.dequeue_nowait()
        dequeued3 = clean_queue.dequeue_nowait()
        
        assert dequeued1.id == task1.id
        assert dequeued2.id == task2.id
        assert dequeued3.id == task3.id
    
    def test_dequeue_empty_queue_nowait(self, clean_queue):
        """Test dequeuing from empty queue (non-blocking)."""
        result = clean_queue.dequeue_nowait()
        assert result is None
    
    def test_queue_size(self, clean_queue):
        """Test queue size tracking."""
        assert clean_queue.size() == 0
        
        clean_queue.enqueue(Task(name="task1"))
        assert clean_queue.size() == 1
        
        clean_queue.enqueue(Task(name="task2"))
        assert clean_queue.size() == 2
        
        clean_queue.dequeue_nowait()
        assert clean_queue.size() == 1
        
        clean_queue.dequeue_nowait()
        assert clean_queue.size() == 0
    
    def test_purge_queue(self, clean_queue):
        """Test purging all tasks from queue."""
        for i in range(5):
            clean_queue.enqueue(Task(name=f"task{i}"))
        
        assert clean_queue.size() == 5
        
        removed = clean_queue.purge()
        
        assert removed == 5
        assert clean_queue.size() == 0
    
    def test_is_empty(self, clean_queue):
        """Test is_empty method."""
        assert clean_queue.is_empty() is True
        
        clean_queue.enqueue(Task(name="task"))
        assert clean_queue.is_empty() is False
        
        clean_queue.dequeue_nowait()
        assert clean_queue.is_empty() is True


class TestBlockingOperations:
    """Test blocking queue operations."""
    
    def test_blocking_dequeue_with_timeout(self, clean_queue):
        """Test blocking dequeue with timeout."""
        start_time = time.time()
        
        # Should timeout after 1 second
        result = clean_queue.dequeue(timeout=1)
        
        elapsed = time.time() - start_time
        
        assert result is None
        assert elapsed >= 1.0
        assert elapsed < 2.0
    
    def test_blocking_dequeue_success(self, clean_queue):
        """Test successful blocking dequeue."""
        task = Task(name="test_task")
        clean_queue.enqueue(task)
        
        # Should return immediately
        result = clean_queue.dequeue(timeout=5)
        
        assert result is not None
        assert result.id == task.id


class TestAdvancedOperations:
    """Test advanced queue operations."""
    
    def test_peek(self, clean_queue):
        """Test peeking at tasks without removing."""
        task1 = Task(name="task1")
        task2 = Task(name="task2")
        
        clean_queue.enqueue(task1)
        clean_queue.enqueue(task2)
        
        # Peek at next task to dequeue
        peeked = clean_queue.peek()
        
        assert peeked is not None
        assert peeked.id == task1.id
        assert clean_queue.size() == 2  # Size unchanged
    
    def test_get_all_tasks(self, clean_queue):
        """Test getting all tasks without removing."""
        tasks = [Task(name=f"task{i}") for i in range(5)]
        
        for task in tasks:
            clean_queue.enqueue(task)
        
        all_tasks = clean_queue.get_all_tasks()
        
        assert len(all_tasks) == 5
        assert clean_queue.size() == 5  # Size unchanged
        
        # Should be in FIFO order
        for i, task in enumerate(all_tasks):
            assert task.id == tasks[i].id
    
    def test_remove_task(self, clean_queue):
        """Test removing specific task by ID."""
        task1 = Task(name="task1")
        task2 = Task(name="task2")
        task3 = Task(name="task3")
        
        clean_queue.enqueue(task1)
        clean_queue.enqueue(task2)
        clean_queue.enqueue(task3)
        
        # Remove middle task
        success = clean_queue.remove_task(task2.id)
        
        assert success is True
        assert clean_queue.size() == 2
        
        # Verify task2 is gone
        all_tasks = clean_queue.get_all_tasks()
        task_ids = [t.id for t in all_tasks]
        assert task2.id not in task_ids
    
    def test_remove_nonexistent_task(self, clean_queue):
        """Test removing task that doesn't exist."""
        success = clean_queue.remove_task("nonexistent-id")
        assert success is False


class TestQueueNaming:
    """Test queue naming and isolation."""
    
    def test_multiple_queues(self, setup_redis):
        """Test multiple named queues are isolated."""
        queue1 = Queue("queue1")
        queue2 = Queue("queue2")
        
        # Clean queues
        queue1.purge()
        queue2.purge()
        
        try:
            task1 = Task(name="task1")
            task2 = Task(name="task2")
            
            queue1.enqueue(task1)
            queue2.enqueue(task2)
            
            assert queue1.size() == 1
            assert queue2.size() == 1
            
            # Dequeue from queue1 shouldn't affect queue2
            dequeued1 = queue1.dequeue_nowait()
            assert dequeued1.id == task1.id
            assert queue1.size() == 0
            assert queue2.size() == 1
            
            # Dequeue from queue2
            dequeued2 = queue2.dequeue_nowait()
            assert dequeued2.id == task2.id
            assert queue2.size() == 0
        finally:
            queue1.purge()
            queue2.purge()
    
    def test_queue_naming_convention(self, setup_redis):
        """Test queue naming conventions."""
        default_queue = Queue("default")
        high_priority_queue = Queue("high_priority")
        
        default_queue.purge()
        high_priority_queue.purge()
        
        try:
            assert default_queue.queue_key == "queue:default"
            assert high_priority_queue.queue_key == "queue:high_priority"
        finally:
            default_queue.purge()
            high_priority_queue.purge()


class TestLargeScale:
    """Test queue with large number of tasks."""
    
    def test_enqueue_dequeue_100_tasks(self, clean_queue):
        """Test enqueuing and dequeuing 100 tasks."""
        num_tasks = 100
        tasks = []
        
        # Enqueue 100 tasks
        for i in range(num_tasks):
            task = Task(
                name=f"task_{i}",
                args=[i],
                kwargs={"index": i}
            )
            tasks.append(task)
            clean_queue.enqueue(task)
        
        # Verify all enqueued
        assert clean_queue.size() == num_tasks
        
        # Dequeue all tasks
        dequeued_tasks = []
        for i in range(num_tasks):
            task = clean_queue.dequeue_nowait()
            assert task is not None
            dequeued_tasks.append(task)
        
        # Verify all dequeued
        assert len(dequeued_tasks) == num_tasks
        assert clean_queue.size() == 0
        
        # Verify FIFO order
        for i in range(num_tasks):
            assert dequeued_tasks[i].id == tasks[i].id
            assert dequeued_tasks[i].name == f"task_{i}"
    
    def test_large_scale_performance(self, clean_queue):
        """Test performance with 1000 tasks."""
        num_tasks = 1000
        
        # Enqueue
        start_time = time.time()
        for i in range(num_tasks):
            task = Task(name=f"task_{i}")
            clean_queue.enqueue(task)
        enqueue_time = time.time() - start_time
        
        assert clean_queue.size() == num_tasks
        
        # Dequeue
        start_time = time.time()
        count = 0
        while not clean_queue.is_empty():
            task = clean_queue.dequeue_nowait()
            if task:
                count += 1
        dequeue_time = time.time() - start_time
        
        assert count == num_tasks
        
        # Performance assertions (should be fast)
        assert enqueue_time < 5.0  # Should take less than 5 seconds
        assert dequeue_time < 5.0
        
        print(f"\nPerformance metrics for {num_tasks} tasks:")
        print(f"  Enqueue time: {enqueue_time:.2f}s")
        print(f"  Dequeue time: {dequeue_time:.2f}s")
        print(f"  Total time: {enqueue_time + dequeue_time:.2f}s")


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_complex_task_data(self, clean_queue):
        """Test queue with complex task data."""
        task = Task(
            name="complex_task",
            args=[1, "string", [1, 2, 3], {"key": "value"}],
            kwargs={
                "nested": {"level": 2, "data": [4, 5, 6]},
                "list": [7, 8, 9]
            }
        )
        
        clean_queue.enqueue(task)
        dequeued = clean_queue.dequeue_nowait()
        
        assert dequeued.args == task.args
        assert dequeued.kwargs == task.kwargs
    
    def test_queue_with_different_priorities(self, clean_queue):
        """Test tasks with different priorities in same queue."""
        high_task = Task(name="high", priority=TaskPriority.HIGH)
        medium_task = Task(name="medium", priority=TaskPriority.MEDIUM)
        low_task = Task(name="low", priority=TaskPriority.LOW)
        
        clean_queue.enqueue(high_task)
        clean_queue.enqueue(medium_task)
        clean_queue.enqueue(low_task)
        
        # Should still be FIFO regardless of priority
        # (Priority handling is done at a higher level)
        first = clean_queue.dequeue_nowait()
        assert first.id == high_task.id
    
    def test_len_magic_method(self, clean_queue):
        """Test __len__ magic method."""
        assert len(clean_queue) == 0
        
        clean_queue.enqueue(Task(name="task1"))
        clean_queue.enqueue(Task(name="task2"))
        
        assert len(clean_queue) == 2
    
    def test_repr(self, clean_queue):
        """Test string representation."""
        repr_str = repr(clean_queue)
        assert "Queue" in repr_str
        assert "test_queue" in repr_str

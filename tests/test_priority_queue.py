"""
Tests for priority queue functionality.
"""
import pytest
import time
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task import Task, TaskPriority
from jobqueue.broker.redis_broker import redis_broker


@pytest.fixture(scope="function")
def setup_redis():
    """Setup Redis connection."""
    redis_broker.connect()
    yield
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_priority_queue(setup_redis):
    """Clean test priority queue."""
    queue = PriorityQueue("test_priority_queue")
    queue.purge()
    yield queue
    queue.purge()


class TestPriorityQueue:
    """Test priority queue operations."""
    
    def test_priority_queue_initialization(self, setup_redis):
        """Test priority queue initialization."""
        queue = PriorityQueue("test_queue")
        
        assert queue.name == "test_queue"
        assert queue.queue_key == "priority_queue:test_queue"
        assert queue.enable_aging is True
    
    def test_enqueue_dequeue(self, clean_priority_queue):
        """Test basic enqueue and dequeue."""
        task = Task(name="test_task", priority=TaskPriority.HIGH)
        
        clean_priority_queue.enqueue(task)
        assert clean_priority_queue.size() == 1
        
        dequeued = clean_priority_queue.dequeue_nowait()
        assert dequeued is not None
        assert dequeued.id == task.id
        assert clean_priority_queue.size() == 0
    
    def test_priority_ordering(self, clean_priority_queue):
        """Test tasks are dequeued by priority (HIGH > MEDIUM > LOW)."""
        # Enqueue in random order
        low_task = Task(name="low", priority=TaskPriority.LOW)
        high_task = Task(name="high", priority=TaskPriority.HIGH)
        medium_task = Task(name="medium", priority=TaskPriority.MEDIUM)
        
        clean_priority_queue.enqueue(low_task)
        clean_priority_queue.enqueue(high_task)
        clean_priority_queue.enqueue(medium_task)
        
        # Should dequeue in priority order
        first = clean_priority_queue.dequeue_nowait()
        assert first.priority == TaskPriority.HIGH
        
        second = clean_priority_queue.dequeue_nowait()
        assert second.priority == TaskPriority.MEDIUM
        
        third = clean_priority_queue.dequeue_nowait()
        assert third.priority == TaskPriority.LOW
    
    def test_fifo_within_same_priority(self, clean_priority_queue):
        """Test FIFO ordering for tasks with same priority."""
        # Enqueue multiple tasks with same priority
        task1 = Task(name="task1", priority=TaskPriority.HIGH)
        time.sleep(0.01)  # Ensure different timestamps
        task2 = Task(name="task2", priority=TaskPriority.HIGH)
        time.sleep(0.01)
        task3 = Task(name="task3", priority=TaskPriority.HIGH)
        
        clean_priority_queue.enqueue(task1)
        clean_priority_queue.enqueue(task2)
        clean_priority_queue.enqueue(task3)
        
        # Should dequeue in FIFO order
        first = clean_priority_queue.dequeue_nowait()
        assert first.id == task1.id
        
        second = clean_priority_queue.dequeue_nowait()
        assert second.id == task2.id
        
        third = clean_priority_queue.dequeue_nowait()
        assert third.id == task3.id
    
    def test_priority_with_fifo(self, clean_priority_queue):
        """Test combination of priority and FIFO ordering."""
        # Create tasks with mixed priorities
        tasks = [
            Task(name="high1", priority=TaskPriority.HIGH),
            Task(name="low1", priority=TaskPriority.LOW),
            Task(name="high2", priority=TaskPriority.HIGH),
            Task(name="medium1", priority=TaskPriority.MEDIUM),
            Task(name="low2", priority=TaskPriority.LOW),
        ]
        
        # Enqueue all
        for task in tasks:
            clean_priority_queue.enqueue(task)
            time.sleep(0.01)
        
        # Expected order: high1, high2, medium1, low1, low2
        dequeued_names = []
        while not clean_priority_queue.is_empty():
            task = clean_priority_queue.dequeue_nowait()
            dequeued_names.append(task.name)
        
        assert dequeued_names == ["high1", "high2", "medium1", "low1", "low2"]
    
    def test_size_by_priority(self, clean_priority_queue):
        """Test getting size broken down by priority."""
        # Enqueue tasks with different priorities
        for i in range(3):
            clean_priority_queue.enqueue(Task(name=f"high{i}", priority=TaskPriority.HIGH))
        for i in range(2):
            clean_priority_queue.enqueue(Task(name=f"medium{i}", priority=TaskPriority.MEDIUM))
        for i in range(1):
            clean_priority_queue.enqueue(Task(name=f"low{i}", priority=TaskPriority.LOW))
        
        sizes = clean_priority_queue.size_by_priority()
        
        assert sizes[TaskPriority.HIGH] == 3
        assert sizes[TaskPriority.MEDIUM] == 2
        assert sizes[TaskPriority.LOW] == 1
    
    def test_peek(self, clean_priority_queue):
        """Test peeking at highest priority task."""
        high_task = Task(name="high", priority=TaskPriority.HIGH)
        low_task = Task(name="low", priority=TaskPriority.LOW)
        
        clean_priority_queue.enqueue(low_task)
        clean_priority_queue.enqueue(high_task)
        
        # Peek should return high priority task
        peeked = clean_priority_queue.peek()
        assert peeked is not None
        assert peeked.priority == TaskPriority.HIGH
        
        # Queue size should not change
        assert clean_priority_queue.size() == 2


class TestDynamicPriority:
    """Test dynamic priority adjustment."""
    
    def test_change_task_priority(self, clean_priority_queue):
        """Test changing task priority."""
        task = Task(name="test", priority=TaskPriority.LOW)
        clean_priority_queue.enqueue(task)
        
        # Change priority
        success = clean_priority_queue.change_task_priority(task.id, TaskPriority.HIGH)
        assert success is True
        
        # Dequeue and verify new priority
        dequeued = clean_priority_queue.dequeue_nowait()
        assert dequeued.priority == TaskPriority.HIGH
    
    def test_promote_task(self, clean_priority_queue):
        """Test promoting a task."""
        task = Task(name="test", priority=TaskPriority.LOW)
        clean_priority_queue.enqueue(task)
        
        # Promote from LOW to MEDIUM
        success = clean_priority_queue.promote_task(task.id)
        assert success is True
        
        # Verify new priority
        dequeued = clean_priority_queue.dequeue_nowait()
        assert dequeued.priority == TaskPriority.MEDIUM
    
    def test_demote_task(self, clean_priority_queue):
        """Test demoting a task."""
        task = Task(name="test", priority=TaskPriority.HIGH)
        clean_priority_queue.enqueue(task)
        
        # Demote from HIGH to MEDIUM
        success = clean_priority_queue.demote_task(task.id)
        assert success is True
        
        # Verify new priority
        dequeued = clean_priority_queue.dequeue_nowait()
        assert dequeued.priority == TaskPriority.MEDIUM
    
    def test_bulk_change_priority(self, clean_priority_queue):
        """Test bulk priority changes."""
        tasks = [
            Task(name=f"task{i}", priority=TaskPriority.LOW)
            for i in range(5)
        ]
        
        task_ids = []
        for task in tasks:
            task_ids.append(task.id)
            clean_priority_queue.enqueue(task)
        
        # Change all to HIGH priority
        updated = clean_priority_queue.bulk_change_priority(task_ids, TaskPriority.HIGH)
        assert updated == 5
        
        # Verify all have HIGH priority
        for _ in range(5):
            task = clean_priority_queue.dequeue_nowait()
            assert task.priority == TaskPriority.HIGH


class TestStarvationPrevention:
    """Test starvation prevention mechanisms."""
    
    def test_aging_disabled(self, setup_redis):
        """Test queue with aging disabled."""
        queue = PriorityQueue("no_aging", enable_aging=False)
        queue.purge()
        
        task = Task(name="test", priority=TaskPriority.LOW)
        queue.enqueue(task)
        
        # Apply aging (should do nothing)
        boosted = queue.apply_aging()
        assert boosted == 0
        
        queue.purge()
    
    def test_get_starved_tasks(self, clean_priority_queue):
        """Test detecting starved tasks."""
        # Create an old task (simulate by setting created_at in past)
        import datetime
        task = Task(name="old_task", priority=TaskPriority.LOW)
        task.created_at = datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
        
        clean_priority_queue.enqueue(task)
        
        # Check for starved tasks (threshold 10 min)
        starved = clean_priority_queue.get_starved_tasks(threshold_seconds=600)
        assert len(starved) == 1
        assert starved[0].id == task.id


class TestQueueOperations:
    """Test various queue operations."""
    
    def test_purge(self, clean_priority_queue):
        """Test purging queue."""
        for i in range(10):
            clean_priority_queue.enqueue(Task(name=f"task{i}"))
        
        assert clean_priority_queue.size() == 10
        
        removed = clean_priority_queue.purge()
        assert removed == 10
        assert clean_priority_queue.size() == 0
    
    def test_get_by_priority(self, clean_priority_queue):
        """Test getting tasks by priority."""
        for i in range(3):
            clean_priority_queue.enqueue(Task(name=f"high{i}", priority=TaskPriority.HIGH))
        for i in range(2):
            clean_priority_queue.enqueue(Task(name=f"low{i}", priority=TaskPriority.LOW))
        
        high_tasks = clean_priority_queue.get_by_priority(TaskPriority.HIGH)
        assert len(high_tasks) == 3
        
        low_tasks = clean_priority_queue.get_by_priority(TaskPriority.LOW)
        assert len(low_tasks) == 2
    
    def test_is_empty(self, clean_priority_queue):
        """Test is_empty check."""
        assert clean_priority_queue.is_empty()
        
        clean_priority_queue.enqueue(Task(name="test"))
        assert not clean_priority_queue.is_empty()
        
        clean_priority_queue.dequeue_nowait()
        assert clean_priority_queue.is_empty()
    
    def test_len(self, clean_priority_queue):
        """Test __len__ magic method."""
        assert len(clean_priority_queue) == 0
        
        for i in range(5):
            clean_priority_queue.enqueue(Task(name=f"task{i}"))
        
        assert len(clean_priority_queue) == 5


class TestPriorityScenarios:
    """Test real-world priority scenarios."""
    
    def test_high_priority_always_first(self, clean_priority_queue):
        """Test that HIGH priority tasks always go first."""
        # Enqueue LOW priority tasks
        for i in range(10):
            clean_priority_queue.enqueue(Task(name=f"low{i}", priority=TaskPriority.LOW))
        
        # Add a HIGH priority task
        high_task = Task(name="urgent", priority=TaskPriority.HIGH)
        clean_priority_queue.enqueue(high_task)
        
        # HIGH priority should be dequeued first
        first = clean_priority_queue.dequeue_nowait()
        assert first.id == high_task.id
    
    def test_mixed_priority_processing(self, clean_priority_queue):
        """Test processing mixed priorities."""
        # Enqueue 100 tasks with random priorities
        import random
        priorities = [TaskPriority.HIGH, TaskPriority.MEDIUM, TaskPriority.LOW]
        
        for i in range(100):
            priority = random.choice(priorities)
            clean_priority_queue.enqueue(Task(name=f"task{i}", priority=priority))
        
        # Dequeue all and verify priority ordering
        last_priority_weight = float('inf')
        
        while not clean_priority_queue.is_empty():
            task = clean_priority_queue.dequeue_nowait()
            current_weight = PriorityQueue.PRIORITY_WEIGHTS[task.priority]
            
            # Current task's priority should be <= last task's priority
            assert current_weight <= last_priority_weight
            last_priority_weight = current_weight

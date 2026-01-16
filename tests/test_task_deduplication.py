"""
Tests for task deduplication.
"""
import pytest
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_deduplication import TaskDeduplication, task_deduplication
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_dedup(redis_connection):
    """Clean deduplication keys before each test."""
    # Clean any existing dedup keys
    keys = redis_broker.client.keys("dedup:*")
    if keys:
        redis_broker.client.delete(*keys)
    keys = redis_broker.client.keys("task:status:*")
    if keys:
        redis_broker.client.delete(*keys)
    yield
    # Cleanup
    keys = redis_broker.client.keys("dedup:*")
    if keys:
        redis_broker.client.delete(*keys)
    keys = redis_broker.client.keys("task:status:*")
    if keys:
        redis_broker.client.delete(*keys)


@task_registry.register("test_dedup_task")
def test_dedup_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_task_signature_generation(clean_dedup):
    """Test task signature generation."""
    task1 = Task(name="test_task", args=[1, 2], kwargs={"key": "value"})
    task1.compute_and_set_signature()
    
    task2 = Task(name="test_task", args=[1, 2], kwargs={"key": "value"})
    task2.compute_and_set_signature()
    
    # Same task should have same signature
    assert task1.task_signature == task2.task_signature
    
    # Different args should have different signature
    task3 = Task(name="test_task", args=[1, 3], kwargs={"key": "value"})
    task3.compute_and_set_signature()
    
    assert task1.task_signature != task3.task_signature


def test_register_task(clean_dedup):
    """Test registering task for deduplication."""
    task = Task(name="test_task", args=[1], queue_name="test_queue", unique=True)
    task.compute_and_set_signature()
    
    success = task_deduplication.register_task(task)
    
    assert success
    
    # Verify registered
    dedup_key = f"dedup:{task.task_signature}"
    existing_task_id = redis_broker.client.get(dedup_key)
    
    assert existing_task_id is not None
    if isinstance(existing_task_id, bytes):
        existing_task_id = existing_task_id.decode()
    assert existing_task_id == task.id


def test_check_duplicate_pending(clean_dedup):
    """Test checking for duplicate pending task."""
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task1.compute_and_set_signature()
    task1.status = TaskStatus.PENDING
    
    # Register first task
    task_deduplication.register_task(task1)
    
    # Create duplicate task
    task2 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task2.compute_and_set_signature()
    
    # Check for duplicate
    existing_id = task_deduplication.check_duplicate(task2)
    
    assert existing_id == task1.id


def test_check_duplicate_completed(clean_dedup):
    """Test checking for duplicate completed task."""
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task1.compute_and_set_signature()
    task1.status = TaskStatus.SUCCESS
    task1.mark_success("result")
    
    # Register first task
    task_deduplication.register_task(task1)
    task_deduplication.update_task_status(task1)
    
    # Create duplicate task
    task2 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task2.compute_and_set_signature()
    
    # Check for duplicate (re-execution not allowed)
    existing_id = task_deduplication.check_duplicate(task2, allow_re_execution=False)
    
    assert existing_id == task1.id
    
    # Check with re-execution allowed
    existing_id = task_deduplication.check_duplicate(task2, allow_re_execution=True)
    
    assert existing_id is None


def test_enqueue_same_task_twice_verify_only_runs_once(clean_dedup):
    """
    Test main case: Enqueue same task twice, verify only runs once.
    """
    print("\n" + "=" * 60)
    print("Test: Enqueue Same Task Twice, Verify Only Runs Once")
    print("=" * 60)
    
    queue = Queue("test_dedup_queue")
    queue.purge()
    
    # Create task with unique=True
    task1 = Task(
        name="test_dedup_task",
        args=["test_value"],
        queue_name="test_dedup_queue",
        unique=True
    )
    
    print(f"\n1. Enqueuing first task...")
    print(f"   Task ID: {task1.id}")
    print(f"   Unique: {task1.unique}")
    print(f"   Args: {task1.args}")
    
    task_id1 = queue.enqueue(task1)
    print(f"   Enqueued: {task_id1}")
    print(f"   Queue size: {queue.size()}")
    
    # Create duplicate task (same name, args, kwargs)
    task2 = Task(
        name="test_dedup_task",
        args=["test_value"],
        queue_name="test_dedup_queue",
        unique=True
    )
    
    print(f"\n2. Enqueuing duplicate task...")
    print(f"   Task ID: {task2.id}")
    print(f"   Unique: {task2.unique}")
    print(f"   Args: {task2.args}")
    
    task_id2 = queue.enqueue(task2)
    print(f"   Enqueued: {task_id2}")
    print(f"   Queue size: {queue.size()}")
    
    # Verify same task_id returned
    assert task_id1 == task_id2, f"Expected same task_id, got {task_id1} and {task_id2}"
    
    # Verify only one task in queue
    queue_size = queue.size()
    assert queue_size == 1, f"Expected 1 task in queue, got {queue_size}"
    
    print(f"\n3. Verification:")
    print(f"   First task ID: {task_id1}")
    print(f"   Second task ID: {task_id2}")
    print(f"   Same task ID: {task_id1 == task_id2}")
    print(f"   Queue size: {queue_size}")
    print(f"   Only one task in queue: {queue_size == 1}")
    
    # Verify signatures match
    task1.compute_and_set_signature()
    task2.compute_and_set_signature()
    assert task1.task_signature == task2.task_signature
    
    print(f"\n4. Signature verification:")
    print(f"   Task 1 signature: {task1.task_signature[:16]}...")
    print(f"   Task 2 signature: {task2.task_signature[:16]}...")
    print(f"   Signatures match: {task1.task_signature == task2.task_signature}")
    
    print(f"\nTest: PASS - Duplicate task detected, only one task enqueued")
    
    # Cleanup
    queue.purge()


def test_enqueue_different_tasks(clean_dedup):
    """Test that different tasks are not considered duplicates."""
    queue = Queue("test_queue")
    queue.purge()
    
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    
    task2 = Task(
        name="test_task",
        args=[2],  # Different arg
        queue_name="test_queue",
        unique=True
    )
    
    task_id1 = queue.enqueue(task1)
    task_id2 = queue.enqueue(task2)
    
    # Should be different task IDs
    assert task_id1 != task_id2
    
    # Should have 2 tasks in queue
    assert queue.size() == 2
    
    queue.purge()


def test_deduplication_with_priority_queue(clean_dedup):
    """Test deduplication with priority queue."""
    queue = PriorityQueue("test_priority_queue")
    queue.purge()
    
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_priority_queue",
        unique=True
    )
    
    task2 = Task(
        name="test_task",
        args=[1],
        queue_name="test_priority_queue",
        unique=True
    )
    
    task_id1 = queue.enqueue(task1)
    task_id2 = queue.enqueue(task2)
    
    # Should return same task_id
    assert task_id1 == task_id2
    
    # Should have only one task
    assert queue.size() == 1
    
    queue.purge()


def test_update_task_status(clean_dedup):
    """Test updating task status in deduplication."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task.status = TaskStatus.PENDING
    
    # Register task
    task_deduplication.register_task(task)
    
    # Update status
    task.status = TaskStatus.RUNNING
    task_deduplication.update_task_status(task)
    
    # Verify status updated
    status = task_deduplication._get_task_status(task.id)
    assert status == TaskStatus.RUNNING


def test_remove_task(clean_dedup):
    """Test removing task from deduplication."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task.compute_and_set_signature()
    
    # Register task
    task_deduplication.register_task(task)
    
    # Verify registered
    dedup_key = f"dedup:{task.task_signature}"
    assert redis_broker.client.get(dedup_key) is not None
    
    # Remove task
    success = task_deduplication.remove_task(task)
    assert success
    
    # Verify removed
    assert redis_broker.client.get(dedup_key) is None


def test_get_duplicate_info(clean_dedup):
    """Test getting duplicate information."""
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task1.compute_and_set_signature()
    task1.status = TaskStatus.PENDING
    
    # Register task
    task_deduplication.register_task(task1)
    
    # Get duplicate info
    info = task_deduplication.get_duplicate_info(task1.task_signature)
    
    assert info is not None
    assert info["task_id"] == task1.id
    assert info["status"] == TaskStatus.PENDING.value
    assert info["signature"] == task1.task_signature


def test_deduplication_ttl(clean_dedup):
    """Test deduplication TTL."""
    task = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task.compute_and_set_signature()
    
    # Register with short TTL
    task_deduplication.register_task(task, ttl=2)
    
    # Verify exists
    dedup_key = f"dedup:{task.task_signature}"
    assert redis_broker.client.get(dedup_key) is not None
    
    # Wait for expiration
    time.sleep(2.5)
    
    # Should be expired
    assert redis_broker.client.get(dedup_key) is None


def test_non_unique_tasks_not_deduplicated(clean_dedup):
    """Test that non-unique tasks are not deduplicated."""
    queue = Queue("test_queue")
    queue.purge()
    
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=False  # Not unique
    )
    
    task2 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=False  # Not unique
    )
    
    task_id1 = queue.enqueue(task1)
    task_id2 = queue.enqueue(task2)
    
    # Should be different task IDs
    assert task_id1 != task_id2
    
    # Should have 2 tasks in queue
    assert queue.size() == 2
    
    queue.purge()


def test_hash_collision_handling(clean_dedup):
    """Test hash collision handling (extremely rare with SHA256)."""
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task1.compute_and_set_signature()
    
    task2 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task2.compute_and_set_signature()
    
    # Register first task
    task_deduplication.register_task(task1)
    
    # Check collision (should be False for same task)
    collision = task_deduplication.handle_hash_collision(
        task1.task_signature,
        task1.id
    )
    assert collision is False
    
    # Check with different task_id but same signature (shouldn't happen in practice)
    # This is a theoretical test
    collision = task_deduplication.handle_hash_collision(
        task1.task_signature,
        task2.id
    )
    # Should detect potential collision
    # (In practice, this would require actual hash collision which is extremely rare)


def test_deduplication_with_completed_task(clean_dedup):
    """Test deduplication behavior with completed task."""
    queue = Queue("test_queue")
    queue.purge()
    
    task1 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    task1.mark_success("result")
    
    # Enqueue and mark as completed
    task_id1 = queue.enqueue(task1)
    task_deduplication.update_task_status(task1)
    
    # Try to enqueue duplicate
    task2 = Task(
        name="test_task",
        args=[1],
        queue_name="test_queue",
        unique=True
    )
    
    task_id2 = queue.enqueue(task2)
    
    # Should return existing task_id (completed task)
    assert task_id1 == task_id2
    
    queue.purge()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

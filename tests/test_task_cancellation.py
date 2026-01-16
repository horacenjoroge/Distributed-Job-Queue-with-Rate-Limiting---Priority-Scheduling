"""
Tests for task cancellation.
"""
import pytest
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_cancellation import (
    TaskCancellation,
    task_cancellation,
    CancellationReason
)
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
def clean_cancellation(redis_connection):
    """Clean cancellation keys before each test."""
    keys = redis_broker.client.keys("cancel:*")
    if keys:
        redis_broker.client.delete(*keys)
    yield
    keys = redis_broker.client.keys("cancel:*")
    if keys:
        redis_broker.client.delete(*keys)


@task_registry.register("test_cancel_task")
def test_cancel_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_request_cancellation(clean_cancellation):
    """Test requesting cancellation."""
    task_id = "test_task_1"
    
    success = task_cancellation.request_cancellation(
        task_id,
        reason=CancellationReason.USER_REQUESTED,
        force=False
    )
    
    assert success
    
    # Verify cancellation flag set
    should_cancel, force, reason = task_cancellation.should_cancel(task_id)
    assert should_cancel is True
    assert force is False
    assert reason == CancellationReason.USER_REQUESTED.value


def test_should_cancel(clean_cancellation):
    """Test checking if task should be cancelled."""
    task_id = "test_task_2"
    
    # Should not be cancelled initially
    should_cancel, force, reason = task_cancellation.should_cancel(task_id)
    assert should_cancel is False
    
    # Request cancellation
    task_cancellation.request_cancellation(task_id)
    
    # Should be cancelled now
    should_cancel, force, reason = task_cancellation.should_cancel(task_id)
    assert should_cancel is True


def test_cancel_pending_task(clean_cancellation):
    """Test cancelling a pending task."""
    queue = Queue("test_queue")
    queue.purge()
    
    task = Task(
        name="test_cancel_task",
        args=[1],
        queue_name="test_queue"
    )
    task.status = TaskStatus.PENDING
    
    # Enqueue task
    queue.enqueue(task)
    assert queue.size() == 1
    
    # Cancel task
    success = task_cancellation.cancel_pending_task(
        task,
        reason=CancellationReason.USER_REQUESTED
    )
    
    assert success
    assert task.status == TaskStatus.CANCELLED
    assert queue.size() == 0  # Removed from queue
    
    queue.purge()


def test_cancel_running_task(clean_cancellation):
    """Test cancelling a running task."""
    task = Task(
        name="test_cancel_task",
        args=[1],
        queue_name="test_queue"
    )
    task.status = TaskStatus.RUNNING
    task.worker_id = "worker_1"
    
    # Request cancellation
    success = task_cancellation.cancel_running_task(
        task,
        reason=CancellationReason.USER_REQUESTED,
        force=False
    )
    
    assert success
    
    # Verify cancellation requested
    should_cancel, force, reason = task_cancellation.should_cancel(task.id)
    assert should_cancel is True
    assert force is False


def test_cancel_task_pending(clean_cancellation):
    """Test cancel_task() with pending task."""
    queue = Queue("test_queue")
    queue.purge()
    
    task = Task(
        name="test_cancel_task",
        args=[1],
        queue_name="test_queue"
    )
    task.status = TaskStatus.PENDING
    
    queue.enqueue(task)
    
    # Cancel using cancel_task()
    success = task_cancellation.cancel_task(
        task,
        reason=CancellationReason.USER_REQUESTED
    )
    
    assert success
    assert task.status == TaskStatus.CANCELLED
    assert queue.size() == 0
    
    queue.purge()


def test_cancel_task_running(clean_cancellation):
    """Test cancel_task() with running task."""
    task = Task(
        name="test_cancel_task",
        args=[1],
        queue_name="test_queue"
    )
    task.status = TaskStatus.RUNNING
    
    # Cancel using cancel_task()
    success = task_cancellation.cancel_task(
        task,
        reason=CancellationReason.USER_REQUESTED,
        force=True
    )
    
    assert success
    
    # Verify cancellation requested
    should_cancel, force, reason = task_cancellation.should_cancel(task.id)
    assert should_cancel is True
    assert force is True


def test_complete_cancellation(clean_cancellation):
    """Test completing cancellation (cleanup)."""
    task_id = "test_task_3"
    
    # Request cancellation
    task_cancellation.request_cancellation(task_id)
    
    # Verify set
    should_cancel, _, _ = task_cancellation.should_cancel(task_id)
    assert should_cancel is True
    
    # Complete cancellation
    task_cancellation.complete_cancellation(task_id)
    
    # Verify removed
    should_cancel, _, _ = task_cancellation.should_cancel(task_id)
    assert should_cancel is False


def test_get_cancellation_info(clean_cancellation):
    """Test getting cancellation information."""
    task_id = "test_task_4"
    
    # No cancellation initially
    info = task_cancellation.get_cancellation_info(task_id)
    assert info is None
    
    # Request cancellation
    task_cancellation.request_cancellation(
        task_id,
        reason=CancellationReason.TIMEOUT,
        force=True
    )
    
    # Get info
    info = task_cancellation.get_cancellation_info(task_id)
    
    assert info is not None
    assert info["task_id"] == task_id
    assert info["cancelled"] is True
    assert info["force"] is True
    assert info["reason"] == CancellationReason.TIMEOUT.value


def test_cancel_pending_and_running_tasks(clean_cancellation):
    """
    Test main case: Cancel pending and running tasks.
    """
    print("\n" + "=" * 60)
    print("Test: Cancel Pending and Running Tasks")
    print("=" * 60)
    
    queue = Queue("test_cancel_queue")
    queue.purge()
    
    # Test 1: Cancel pending task
    print(f"\n1. Cancelling pending task...")
    task1 = Task(
        name="test_cancel_task",
        args=["pending_test"],
        queue_name="test_cancel_queue"
    )
    task1.status = TaskStatus.PENDING
    
    queue.enqueue(task1)
    print(f"   Task ID: {task1.id}")
    print(f"   Status: {task1.status.value}")
    print(f"   Queue size: {queue.size()}")
    
    success = task_cancellation.cancel_pending_task(
        task1,
        reason=CancellationReason.USER_REQUESTED
    )
    
    print(f"   Cancelled: {success}")
    print(f"   New status: {task1.status.value}")
    print(f"   Queue size: {queue.size()}")
    
    assert success
    assert task1.status == TaskStatus.CANCELLED
    assert queue.size() == 0
    
    # Test 2: Cancel running task
    print(f"\n2. Cancelling running task...")
    task2 = Task(
        name="test_cancel_task",
        args=["running_test"],
        queue_name="test_cancel_queue"
    )
    task2.status = TaskStatus.RUNNING
    task2.worker_id = "worker_1"
    
    print(f"   Task ID: {task2.id}")
    print(f"   Status: {task2.status.value}")
    print(f"   Worker ID: {task2.worker_id}")
    
    success = task_cancellation.cancel_running_task(
        task2,
        reason=CancellationReason.USER_REQUESTED,
        force=False
    )
    
    print(f"   Cancellation requested: {success}")
    
    # Verify cancellation requested
    should_cancel, force, reason = task_cancellation.should_cancel(task2.id)
    print(f"   Should cancel: {should_cancel}")
    print(f"   Force: {force}")
    print(f"   Reason: {reason}")
    
    assert success
    assert should_cancel is True
    assert force is False
    assert reason == CancellationReason.USER_REQUESTED.value
    
    print(f"\nTest: PASS - Both pending and running tasks cancelled successfully")
    
    queue.purge()


def test_cancellation_reasons(clean_cancellation):
    """Test different cancellation reasons."""
    task_id = "test_task_5"
    
    reasons = [
        CancellationReason.USER_REQUESTED,
        CancellationReason.TIMEOUT,
        CancellationReason.DEPENDENCY_FAILED,
        CancellationReason.WORKER_SHUTDOWN,
        CancellationReason.SYSTEM_ERROR
    ]
    
    for reason in reasons:
        task_cancellation.request_cancellation(task_id, reason=reason)
        
        should_cancel, _, stored_reason = task_cancellation.should_cancel(task_id)
        assert should_cancel is True
        assert stored_reason == reason.value
        
        task_cancellation.complete_cancellation(task_id)


def test_force_cancellation(clean_cancellation):
    """Test force cancellation flag."""
    task_id = "test_task_6"
    
    # Request graceful cancellation
    task_cancellation.request_cancellation(task_id, force=False)
    should_cancel, force, _ = task_cancellation.should_cancel(task_id)
    assert should_cancel is True
    assert force is False
    
    task_cancellation.complete_cancellation(task_id)
    
    # Request force cancellation
    task_cancellation.request_cancellation(task_id, force=True)
    should_cancel, force, _ = task_cancellation.should_cancel(task_id)
    assert should_cancel is True
    assert force is True


def test_cancel_completed_task(clean_cancellation):
    """Test that completed tasks cannot be cancelled."""
    task = Task(
        name="test_cancel_task",
        args=[1],
        queue_name="test_queue"
    )
    task.status = TaskStatus.SUCCESS
    task.mark_success("result")
    
    # Try to cancel
    success = task_cancellation.cancel_task(task)
    
    # Should fail (task already completed)
    assert success is False


def test_cancel_retry_task(clean_cancellation):
    """Test cancelling a task in RETRY status."""
    queue = Queue("test_queue")
    queue.purge()
    
    task = Task(
        name="test_cancel_task",
        args=[1],
        queue_name="test_queue"
    )
    task.status = TaskStatus.RETRY
    
    queue.enqueue(task)
    
    # Cancel retry task (treated as pending)
    success = task_cancellation.cancel_task(task)
    
    assert success
    assert task.status == TaskStatus.CANCELLED
    
    queue.purge()


def test_cancellation_with_priority_queue(clean_cancellation):
    """Test cancellation with priority queue."""
    queue = PriorityQueue("test_priority_queue")
    queue.purge()
    
    task = Task(
        name="test_cancel_task",
        args=[1],
        queue_name="test_priority_queue"
    )
    task.status = TaskStatus.PENDING
    
    queue.enqueue(task)
    assert queue.size() == 1
    
    # Cancel task
    success = task_cancellation.cancel_pending_task(task)
    
    assert success
    assert task.status == TaskStatus.CANCELLED
    
    queue.purge()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

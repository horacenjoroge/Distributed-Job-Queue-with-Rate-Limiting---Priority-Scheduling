"""
Tests for Dead Letter Queue functionality.
"""
import pytest
import traceback
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority, TaskStatus
from jobqueue.core.dead_letter_queue import DeadLetterQueue, dead_letter_queue, add_to_dlq
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_dlq(redis_connection):
    """Clean Dead Letter Queue before each test."""
    dlq = DeadLetterQueue()
    dlq.purge()
    
    yield dlq
    
    dlq.purge()


# Counter for tracking failures
failure_counter = {}


@task_registry.register("always_failing_task")
def always_failing_task(task_id: str):
    """Task that always fails."""
    if task_id not in failure_counter:
        failure_counter[task_id] = 0
    
    failure_counter[task_id] += 1
    raise Exception(f"Intentional failure {failure_counter[task_id]}")


def test_add_task_to_dlq(clean_dlq):
    """Test adding a task to Dead Letter Queue."""
    dlq = clean_dlq
    
    # Create failed task
    task = Task(
        name="test_task",
        args=[1, 2],
        max_retries=3,
        retry_count=3
    )
    task.mark_failed("Test failure")
    
    # Add to DLQ
    success = dlq.add_task(task, "Test failure reason")
    
    assert success
    assert dlq.size() == 1


def test_dlq_stores_failure_reason_and_stack_trace(clean_dlq):
    """Test that DLQ stores failure reason and stack trace."""
    dlq = clean_dlq
    
    # Create task with exception
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3,
        retry_count=3
    )
    task.mark_failed("Test error")
    
    # Create exception for stack trace
    try:
        raise ValueError("Test exception")
    except ValueError as e:
        exception = e
    
    # Add to DLQ
    dlq.add_task(task, "Test failure", traceback.format_exc())
    
    # Retrieve task
    tasks = dlq.get_tasks(limit=1)
    assert len(tasks) == 1
    
    entry = tasks[0]
    assert entry["failure_reason"] == "Test failure"
    assert entry["stack_trace"] is not None
    assert "ValueError" in entry["stack_trace"]


def test_task_fails_3_times_lands_in_dlq(clean_dlq):
    """
    Test main case: Task fails 3 times → lands in DLQ.
    """
    dlq = clean_dlq
    queue = Queue("test_dlq_queue")
    queue.purge()
    
    task_id = "test_fail_3_times"
    failure_counter[task_id] = 0
    
    # Create task with max_retries=3
    task = Task(
        name="always_failing_task",
        args=[task_id],
        max_retries=3,
        queue_name="test_dlq_queue"
    )
    
    # Simulate 3 failures (exhausting retries)
    for attempt in range(3):
        try:
            always_failing_task(task_id)
        except Exception as e:
            task.mark_failed(str(e))
            task.record_retry_attempt(str(e), 2.0 ** attempt)
            task.increment_retry()
    
    # Task should have exhausted retries
    assert task.retry_count == 3
    assert not task.can_retry()
    
    # Add to DLQ (worker would do this automatically)
    dlq.add_task(task, f"Task failed after {task.max_retries} retries")
    
    # Verify task is in DLQ
    assert dlq.size() == 1
    
    # Retrieve and verify
    dlq_tasks = dlq.get_tasks()
    assert len(dlq_tasks) == 1
    
    entry = dlq_tasks[0]
    assert entry["task_id"] == task.id
    assert entry["retry_count"] == 3
    assert entry["max_retries"] == 3
    assert entry["failure_reason"] == f"Task failed after {task.max_retries} retries"
    
    # Cleanup
    queue.purge()


def test_get_dlq_tasks_with_pagination(clean_dlq):
    """Test getting DLQ tasks with pagination."""
    dlq = clean_dlq
    
    # Add multiple tasks
    for i in range(5):
        task = Task(
            name=f"task_{i}",
            args=[i],
            max_retries=3,
            retry_count=3
        )
        task.mark_failed(f"Error {i}")
        dlq.add_task(task, f"Failure {i}")
    
    assert dlq.size() == 5
    
    # Get first 2 tasks
    tasks = dlq.get_tasks(limit=2, offset=0)
    assert len(tasks) == 2
    
    # Get next 2 tasks
    tasks = dlq.get_tasks(limit=2, offset=2)
    assert len(tasks) == 2
    
    # Get remaining tasks
    tasks = dlq.get_tasks(limit=2, offset=4)
    assert len(tasks) == 1


def test_get_task_by_id(clean_dlq):
    """Test getting a specific task from DLQ by ID."""
    dlq = clean_dlq
    
    # Add task
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3,
        retry_count=3
    )
    task.mark_failed("Test error")
    dlq.add_task(task, "Test failure")
    
    # Get by ID
    entry = dlq.get_task_by_id(task.id)
    
    assert entry is not None
    assert entry["task_id"] == task.id
    assert entry["task_name"] == "test_task"
    
    # Get non-existent task
    entry = dlq.get_task_by_id("non_existent_id")
    assert entry is None


def test_dlq_size(clean_dlq):
    """Test getting DLQ size."""
    dlq = clean_dlq
    
    assert dlq.size() == 0
    
    # Add tasks
    for i in range(3):
        task = Task(name=f"task_{i}", args=[i], max_retries=3, retry_count=3)
        task.mark_failed("Error")
        dlq.add_task(task, "Failure")
    
    assert dlq.size() == 3


def test_purge_dlq(clean_dlq):
    """Test purging Dead Letter Queue."""
    dlq = clean_dlq
    
    # Add tasks
    for i in range(5):
        task = Task(name=f"task_{i}", args=[i], max_retries=3, retry_count=3)
        task.mark_failed("Error")
        dlq.add_task(task, "Failure")
    
    assert dlq.size() == 5
    
    # Purge
    purged = dlq.purge()
    
    assert purged == 5
    assert dlq.size() == 0


def test_remove_task_from_dlq(clean_dlq):
    """Test removing a specific task from DLQ."""
    dlq = clean_dlq
    
    # Add tasks
    task1 = Task(name="task1", args=[1], max_retries=3, retry_count=3)
    task1.mark_failed("Error")
    dlq.add_task(task1, "Failure 1")
    
    task2 = Task(name="task2", args=[2], max_retries=3, retry_count=3)
    task2.mark_failed("Error")
    dlq.add_task(task2, "Failure 2")
    
    assert dlq.size() == 2
    
    # Remove task1
    success = dlq.remove_task(task1.id)
    
    assert success
    assert dlq.size() == 1
    
    # Verify task2 still exists
    entry = dlq.get_task_by_id(task2.id)
    assert entry is not None


def test_retry_task_from_dlq(clean_dlq):
    """Test retrying a task from Dead Letter Queue."""
    dlq = clean_dlq
    queue = Queue("test_retry_queue")
    queue.purge()
    
    # Add task to DLQ
    task = Task(
        name="test_task",
        args=[1, 2],
        max_retries=3,
        retry_count=3,
        queue_name="test_retry_queue"
    )
    task.mark_failed("Test error")
    dlq.add_task(task, "Test failure")
    
    assert dlq.size() == 1
    
    # Retry task
    retried_task = dlq.retry_task(task.id, reset_retry_count=True)
    
    assert retried_task is not None
    assert retried_task.id == task.id
    assert retried_task.retry_count == 0  # Reset
    assert retried_task.status == TaskStatus.PENDING
    assert retried_task.error is None
    
    # Task should be removed from DLQ
    assert dlq.size() == 0
    
    # Task should be in queue
    assert queue.size() == 1
    
    # Cleanup
    queue.purge()


def test_retry_task_without_reset(clean_dlq):
    """Test retrying task without resetting retry count."""
    dlq = clean_dlq
    queue = Queue("test_retry_queue2")
    queue.purge()
    
    # Add task with retry_count=3
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3,
        retry_count=3,
        queue_name="test_retry_queue2"
    )
    task.mark_failed("Error")
    dlq.add_task(task, "Failure")
    
    # Retry without reset
    retried_task = dlq.retry_task(task.id, reset_retry_count=False)
    
    assert retried_task.retry_count == 3  # Not reset
    
    # Cleanup
    queue.purge()


def test_dlq_stats(clean_dlq):
    """Test getting DLQ statistics."""
    dlq = clean_dlq
    
    # Add tasks from different queues
    for i in range(3):
        task = Task(
            name="test_task",
            args=[i],
            max_retries=3,
            retry_count=3,
            queue_name="queue1"
        )
        task.mark_failed("Error")
        dlq.add_task(task, "Failure")
    
    for i in range(2):
        task = Task(
            name="other_task",
            args=[i],
            max_retries=3,
            retry_count=3,
            queue_name="queue2"
        )
        task.mark_failed("Error")
        dlq.add_task(task, "Failure")
    
    # Get stats
    stats = dlq.get_stats()
    
    assert stats["size"] == 5
    assert stats["by_task_name"]["test_task"] == 3
    assert stats["by_task_name"]["other_task"] == 2
    assert stats["by_queue"]["queue1"] == 3
    assert stats["by_queue"]["queue2"] == 2


def test_check_alert_threshold(clean_dlq):
    """Test checking DLQ alert threshold."""
    dlq = clean_dlq
    
    # Add tasks below threshold
    for i in range(50):
        task = Task(name=f"task_{i}", args=[i], max_retries=3, retry_count=3)
        task.mark_failed("Error")
        dlq.add_task(task, "Failure")
    
    # Check threshold (100)
    exceeds, size = dlq.check_alert_threshold(threshold=100)
    
    assert not exceeds
    assert size == 50
    
    # Add more tasks to exceed threshold
    for i in range(51):
        task = Task(name=f"task_{i+50}", args=[i+50], max_retries=3, retry_count=3)
        task.mark_failed("Error")
        dlq.add_task(task, "Failure")
    
    # Check threshold again
    exceeds, size = dlq.check_alert_threshold(threshold=100)
    
    assert exceeds
    assert size == 101


def test_add_to_dlq_helper(clean_dlq):
    """Test add_to_dlq helper function."""
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3,
        retry_count=3
    )
    task.mark_failed("Test error")
    
    # Create exception
    try:
        raise ValueError("Test exception")
    except ValueError as e:
        exception = e
    
    # Use helper
    success = add_to_dlq(task, "Test failure", exception)
    
    assert success
    assert dead_letter_queue.size() == 1
    
    # Verify stack trace stored
    entry = dead_letter_queue.get_task_by_id(task.id)
    assert entry is not None
    assert "ValueError" in entry["stack_trace"]


def test_dlq_stores_retry_history(clean_dlq):
    """Test that DLQ stores retry history."""
    dlq = clean_dlq
    
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3,
        retry_count=3
    )
    
    # Add retry history
    task.record_retry_attempt("Error 1", 1.0)
    task.increment_retry()
    task.record_retry_attempt("Error 2", 2.0)
    task.increment_retry()
    task.record_retry_attempt("Error 3", 4.0)
    task.increment_retry()
    
    task.mark_failed("Final failure")
    dlq.add_task(task, "Exceeded max retries")
    
    # Verify retry history stored
    entry = dlq.get_task_by_id(task.id)
    assert len(entry["retry_history"]) == 3
    assert entry["retry_history"][0]["error"] == "Error 1"
    assert entry["retry_history"][1]["backoff_seconds"] == 2.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

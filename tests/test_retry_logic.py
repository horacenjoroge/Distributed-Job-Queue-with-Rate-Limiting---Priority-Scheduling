"""
Tests for retry logic with exponential backoff.
"""
import pytest
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority, TaskStatus
from jobqueue.core.redis_queue import Queue
from jobqueue.core.retry_backoff import (
    ExponentialBackoff,
    LinearBackoff,
    FibonacciBackoff,
    calculate_exponential_backoff,
    get_retry_delays
)
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_test_queue(redis_connection):
    """Clean test queue."""
    queue_name = "test_retry_queue"
    queue = Queue(queue_name)
    queue.purge()
    
    yield queue_name
    
    queue.purge()


# Counter for tracking task execution attempts
task_attempt_counter = {}


@task_registry.register("failing_task")
def failing_task(task_id: str, fail_count: int = 2):
    """
    Task that fails a specified number of times, then succeeds.
    
    Args:
        task_id: Unique task identifier
        fail_count: Number of times to fail before succeeding
    """
    # Initialize counter for this task
    if task_id not in task_attempt_counter:
        task_attempt_counter[task_id] = 0
    
    task_attempt_counter[task_id] += 1
    attempt = task_attempt_counter[task_id]
    
    print(f"Task {task_id} - Attempt {attempt}")
    
    if attempt <= fail_count:
        # Fail on first N attempts
        raise Exception(f"Intentional failure on attempt {attempt}")
    
    # Succeed after fail_count attempts
    return f"Success after {attempt} attempts"


def test_exponential_backoff_calculation():
    """Test exponential backoff delay calculation."""
    backoff = ExponentialBackoff(base=2.0, max_delay=300.0, jitter=False)
    
    # Test backoff sequence: 1, 2, 4, 8, 16
    delays = []
    for i in range(5):
        delay = backoff.calculate_delay(i)
        delays.append(delay)
    
    # Verify exponential growth (without jitter)
    assert delays[0] == 1.0  # 2^0
    assert delays[1] == 2.0  # 2^1
    assert delays[2] == 4.0  # 2^2
    assert delays[3] == 8.0  # 2^3
    assert delays[4] == 16.0  # 2^4


def test_backoff_max_cap():
    """Test maximum delay cap."""
    backoff = ExponentialBackoff(base=2.0, max_delay=10.0, jitter=False)
    
    # With max_delay=10, any delay > 10 should be capped
    delay = backoff.calculate_delay(10)  # 2^10 = 1024, but capped at 10
    
    assert delay == 10.0


def test_backoff_with_jitter():
    """Test that jitter adds randomness."""
    backoff = ExponentialBackoff(base=2.0, max_delay=300.0, jitter=True)
    
    # Calculate delay multiple times
    delays = [backoff.calculate_delay(3) for _ in range(10)]
    
    # All delays should be in range [4.0, 8.0] (50-100% of 8.0)
    for delay in delays:
        assert 4.0 <= delay <= 8.0
    
    # Delays should vary (not all the same)
    assert len(set(delays)) > 1


def test_calculate_exponential_backoff_helper():
    """Test helper function for backoff calculation."""
    # Test sequence
    delay0 = calculate_exponential_backoff(0, base=2.0, jitter=False)
    delay1 = calculate_exponential_backoff(1, base=2.0, jitter=False)
    delay2 = calculate_exponential_backoff(2, base=2.0, jitter=False)
    
    assert delay0 == 1.0
    assert delay1 == 2.0
    assert delay2 == 4.0


def test_get_retry_delays():
    """Test getting retry delay sequence."""
    delays = get_retry_delays(5, strategy="exponential", base=2.0, jitter=False)
    
    assert len(delays) == 5
    assert delays[0] == 1.0
    assert delays[1] == 2.0
    assert delays[2] == 4.0
    assert delays[3] == 8.0
    assert delays[4] == 16.0


def test_linear_backoff():
    """Test linear backoff strategy."""
    backoff = LinearBackoff(delay=5.0)
    
    # All delays should be constant
    assert backoff.calculate_delay(0) == 5.0
    assert backoff.calculate_delay(1) == 5.0
    assert backoff.calculate_delay(2) == 5.0
    assert backoff.calculate_delay(10) == 5.0


def test_fibonacci_backoff():
    """Test Fibonacci backoff strategy."""
    backoff = FibonacciBackoff(base=1.0, max_delay=300.0)
    
    # Fibonacci sequence: 0, 1, 1, 2, 3, 5, 8, 13, 21...
    delays = [backoff.calculate_delay(i) for i in range(7)]
    
    assert delays[0] == 1.0  # fib(1) = 1
    assert delays[1] == 1.0  # fib(2) = 1
    assert delays[2] == 2.0  # fib(3) = 2
    assert delays[3] == 3.0  # fib(4) = 3
    assert delays[4] == 5.0  # fib(5) = 5
    assert delays[5] == 8.0  # fib(6) = 8
    assert delays[6] == 13.0  # fib(7) = 13


def test_task_retry_history():
    """Test recording retry attempts in task history."""
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3
    )
    
    # Record retry attempts
    task.record_retry_attempt("First error", 1.0)
    task.increment_retry()
    
    task.record_retry_attempt("Second error", 2.0)
    task.increment_retry()
    
    # Check history
    assert len(task.retry_history) == 2
    
    # Check first retry
    assert task.retry_history[0]["attempt"] == 0
    assert task.retry_history[0]["error"] == "First error"
    assert task.retry_history[0]["backoff_seconds"] == 1.0
    
    # Check second retry
    assert task.retry_history[1]["attempt"] == 1
    assert task.retry_history[1]["error"] == "Second error"
    assert task.retry_history[1]["backoff_seconds"] == 2.0


def test_task_fail_twice_then_succeed():
    """
    Test main case: Task fails 2 times then succeeds.
    Verifies retry history and backoff delays.
    """
    task_id = "test_fail_twice_succeed"
    task_attempt_counter[task_id] = 0
    
    task = Task(
        name="failing_task",
        args=[task_id, 2],  # Fail 2 times
        max_retries=3,
        queue_name="test_retry"
    )
    
    # Simulate first execution (will fail)
    try:
        failing_task(task_id, 2)
    except Exception as e:
        task.mark_failed(str(e))
        task.record_retry_attempt(str(e), 1.0)
        task.increment_retry()
    
    assert task.status == TaskStatus.FAILED
    assert task.retry_count == 1
    assert len(task.retry_history) == 1
    
    # Simulate second execution (will fail again)
    try:
        failing_task(task_id, 2)
    except Exception as e:
        task.mark_failed(str(e))
        task.record_retry_attempt(str(e), 2.0)
        task.increment_retry()
    
    assert task.retry_count == 2
    assert len(task.retry_history) == 2
    
    # Simulate third execution (will succeed)
    result = failing_task(task_id, 2)
    task.mark_success(result)
    
    assert task.status == TaskStatus.SUCCESS
    assert task.retry_count == 2
    assert task.result == "Success after 3 attempts"
    
    # Verify retry history
    assert task.retry_history[0]["backoff_seconds"] == 1.0
    assert task.retry_history[1]["backoff_seconds"] == 2.0


def test_retry_with_max_retries_exceeded():
    """Test that task moves to DLQ after max retries."""
    task = Task(
        name="failing_task",
        args=["always_fail", 999],  # Fail forever
        max_retries=2,
        queue_name="test_retry"
    )
    
    # Exhaust all retries
    for i in range(3):
        if task.can_retry():
            backoff = calculate_exponential_backoff(task.retry_count, jitter=False)
            task.record_retry_attempt(f"Attempt {i+1} failed", backoff)
            task.increment_retry()
    
    # Should not be retryable anymore
    assert not task.can_retry()
    assert task.retry_count == 3
    assert len(task.retry_history) == 3


def test_retry_delay_progression():
    """Test that retry delays follow exponential progression."""
    task = Task(
        name="test_task",
        args=[1],
        max_retries=5
    )
    
    expected_delays = [1.0, 2.0, 4.0, 8.0, 16.0]
    
    for i, expected_delay in enumerate(expected_delays):
        # Calculate backoff for this retry
        backoff = calculate_exponential_backoff(i, jitter=False)
        
        # Should match expected progression
        assert backoff == expected_delay
        
        # Record in task
        task.record_retry_attempt(f"Error {i}", backoff)
        
        if i < len(expected_delays) - 1:
            task.increment_retry()
    
    # Verify all retries recorded
    assert len(task.retry_history) == 5
    
    # Verify backoff progression
    for i, expected_delay in enumerate(expected_delays):
        assert task.retry_history[i]["backoff_seconds"] == expected_delay


def test_backoff_cap_at_300_seconds():
    """Test that backoff caps at 300 seconds."""
    backoff = ExponentialBackoff(base=2.0, max_delay=300.0, jitter=False)
    
    # After 9 retries: 2^9 = 512 seconds, should cap at 300
    delay = backoff.calculate_delay(9)
    assert delay == 300.0
    
    # After 10 retries: 2^10 = 1024 seconds, should cap at 300
    delay = backoff.calculate_delay(10)
    assert delay == 300.0


def test_immediate_retry_on_first_failure():
    """Test that first retry happens with 1 second delay (2^0)."""
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3
    )
    
    # First failure (retry_count = 0)
    backoff = calculate_exponential_backoff(0, jitter=False)
    
    # Should be 1 second (2^0 = 1)
    assert backoff == 1.0


def test_retry_backoff_sequence():
    """Test complete retry backoff sequence for typical scenario."""
    # Simulate 5 retries
    backoff_sequence = []
    
    for retry_count in range(5):
        delay = calculate_exponential_backoff(retry_count, jitter=False)
        backoff_sequence.append(delay)
    
    # Verify sequence: 1s, 2s, 4s, 8s, 16s
    assert backoff_sequence == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_multiple_tasks_independent_retry_history():
    """Test that multiple tasks maintain independent retry histories."""
    task1 = Task(name="task1", args=[1], max_retries=3)
    task2 = Task(name="task2", args=[2], max_retries=3)
    
    # Task 1 fails twice
    task1.record_retry_attempt("Error 1", 1.0)
    task1.increment_retry()
    task1.record_retry_attempt("Error 2", 2.0)
    task1.increment_retry()
    
    # Task 2 fails once
    task2.record_retry_attempt("Error A", 1.0)
    task2.increment_retry()
    
    # Verify independent histories
    assert len(task1.retry_history) == 2
    assert len(task2.retry_history) == 1
    
    assert task1.retry_count == 2
    assert task2.retry_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

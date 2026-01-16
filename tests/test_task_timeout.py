"""
Tests for task timeout enforcement.
"""
import pytest
import time
import threading
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_timeout import (
    TimeoutHandler,
    TimeoutManager,
    execute_with_timeout,
    ProcessTimeoutKiller
)
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@task_registry.register("long_running_task")
def long_running_task(duration: float):
    """Task that runs for specified duration."""
    time.sleep(duration)
    return f"Completed after {duration}s"


@task_registry.register("quick_task")
def quick_task():
    """Task that completes quickly."""
    return "Quick success"


def test_timeout_handler_basic():
    """Test basic timeout handler functionality."""
    handler = TimeoutHandler(timeout_seconds=5)
    
    handler.start()
    
    # Check elapsed time
    time.sleep(0.1)
    elapsed = handler.elapsed_time()
    assert elapsed >= 0.1
    
    # Check remaining time
    remaining = handler.remaining_time()
    assert remaining <= 5.0
    assert remaining > 0
    
    handler.stop()


def test_timeout_handler_timeout_detection():
    """Test timeout detection."""
    handler = TimeoutHandler(timeout_seconds=1)
    handler.start()
    
    # Should not be timed out initially
    assert not handler.is_timed_out()
    
    # Wait for timeout
    time.sleep(1.1)
    
    # Should be timed out
    assert handler.is_timed_out()
    
    handler.stop()


def test_soft_timeout_warning():
    """Test soft timeout warning."""
    handler = TimeoutHandler(timeout_seconds=10)
    handler.start()
    
    # Should not trigger initially
    assert not handler.check_soft_timeout(soft_timeout_ratio=0.8)
    
    # Wait for 80% of timeout (8 seconds)
    time.sleep(8.1)
    
    # Should trigger soft timeout
    assert handler.check_soft_timeout(soft_timeout_ratio=0.8)
    
    # Should not trigger again
    assert not handler.check_soft_timeout(soft_timeout_ratio=0.8)
    
    handler.stop()


def test_task_with_5s_timeout_runs_10s_killed():
    """
    Test main case: Task with 5s timeout, runs for 10s → killed.
    """
    # Create task with 5 second timeout
    task = Task(
        name="long_running_task",
        args=[10.0],  # Task runs for 10 seconds
        timeout=5  # But timeout is 5 seconds
    )
    
    # Execute with timeout
    result, error = execute_with_timeout(
        long_running_task,
        timeout_seconds=5,
        10.0  # Run for 10 seconds
    )
    
    # Should have timed out
    assert error is not None
    assert "timeout" in error.lower() or "exceeded" in error.lower()
    assert result is None
    
    # Task should be marked as timed out
    task.mark_timeout()
    assert task.status == TaskStatus.TIMEOUT


def test_task_completes_before_timeout():
    """Test task that completes before timeout."""
    task = Task(
        name="quick_task",
        args=[],
        timeout=10
    )
    
    # Execute with timeout
    result, error = execute_with_timeout(
        quick_task,
        timeout_seconds=10
    )
    
    # Should complete successfully
    assert error is None
    assert result == "Quick success"


def test_timeout_manager():
    """Test TimeoutManager."""
    manager = TimeoutManager(
        timeout_seconds=5,
        soft_timeout_ratio=0.8,
        enable_soft_timeout=True
    )
    
    manager.start()
    
    # Check not timed out initially
    is_timed_out, soft_warning = manager.check()
    assert not is_timed_out
    assert not soft_warning
    
    # Wait for soft timeout (80% = 4 seconds)
    time.sleep(4.1)
    
    # Check soft timeout
    is_timed_out, soft_warning = manager.check()
    assert not is_timed_out  # Not hard timeout yet
    assert soft_warning  # But soft timeout triggered
    
    # Wait for hard timeout
    time.sleep(1.1)
    
    # Check hard timeout
    is_timed_out, soft_warning = manager.check()
    assert is_timed_out
    
    manager.stop()


def test_timeout_manager_remaining_time():
    """Test getting remaining time from timeout manager."""
    manager = TimeoutManager(timeout_seconds=10)
    manager.start()
    
    # Should have remaining time
    remaining = manager.get_remaining_time()
    assert remaining <= 10.0
    assert remaining > 0
    
    # Wait a bit
    time.sleep(1)
    
    # Remaining should decrease
    new_remaining = manager.get_remaining_time()
    assert new_remaining < remaining
    
    manager.stop()


def test_timeout_manager_elapsed_time():
    """Test getting elapsed time from timeout manager."""
    manager = TimeoutManager(timeout_seconds=10)
    manager.start()
    
    # Should start at 0
    elapsed = manager.get_elapsed_time()
    assert elapsed >= 0
    
    # Wait a bit
    time.sleep(0.5)
    
    # Elapsed should increase
    new_elapsed = manager.get_elapsed_time()
    assert new_elapsed > elapsed
    assert new_elapsed >= 0.5
    
    manager.stop()


def test_timeout_with_threading():
    """Test timeout enforcement with threading."""
    def long_function():
        time.sleep(10)
        return "Done"
    
    # Execute with 2 second timeout
    result, error = execute_with_timeout(long_function, timeout_seconds=2)
    
    # Should timeout
    assert error is not None
    assert result is None


def test_soft_timeout_ratio():
    """Test different soft timeout ratios."""
    handler = TimeoutHandler(timeout_seconds=10)
    handler.start()
    
    # 50% ratio
    time.sleep(5.1)
    assert handler.check_soft_timeout(soft_timeout_ratio=0.5)
    
    handler.stop()
    
    # 90% ratio
    handler2 = TimeoutHandler(timeout_seconds=10)
    handler2.start()
    
    time.sleep(9.1)
    assert handler2.check_soft_timeout(soft_timeout_ratio=0.9)
    
    handler2.stop()


def test_timeout_manager_disabled_soft_timeout():
    """Test timeout manager with soft timeout disabled."""
    manager = TimeoutManager(
        timeout_seconds=5,
        enable_soft_timeout=False
    )
    
    manager.start()
    
    # Wait for soft timeout threshold
    time.sleep(4.1)
    
    # Should not trigger soft warning
    is_timed_out, soft_warning = manager.check()
    assert not soft_warning
    
    manager.stop()


def test_multiple_timeout_checks():
    """Test multiple timeout checks."""
    manager = TimeoutManager(timeout_seconds=2)
    manager.start()
    
    # Check multiple times
    for _ in range(5):
        is_timed_out, soft_warning = manager.check()
        time.sleep(0.1)
    
    # Should eventually timeout
    time.sleep(2.1)
    is_timed_out, _ = manager.check()
    assert is_timed_out
    
    manager.stop()


def test_timeout_with_task_execution():
    """Test timeout with actual task execution."""
    task = Task(
        name="long_running_task",
        args=[3.0],  # Runs for 3 seconds
        timeout=2  # Timeout at 2 seconds
    )
    
    # Execute task function with timeout
    result, error = execute_with_timeout(
        long_running_task,
        timeout_seconds=task.timeout,
        3.0
    )
    
    # Should timeout
    assert error is not None
    assert result is None


def test_timeout_zero_disabled():
    """Test that timeout=0 disables timeout."""
    handler = TimeoutHandler(timeout_seconds=0)
    handler.start()
    
    # Should not timeout
    assert not handler.is_timed_out()
    
    time.sleep(1)
    
    # Still should not timeout (0 = disabled)
    assert not handler.is_timed_out()
    
    handler.stop()


def test_timeout_cleanup():
    """Test timeout cleanup."""
    manager = TimeoutManager(timeout_seconds=5)
    manager.start()
    
    # Should be active
    assert manager.handler is not None
    
    # Stop
    manager.stop()
    
    # Should be cleaned up
    assert manager.handler is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

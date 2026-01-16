"""
Tests for rate limiting functionality.
"""
import pytest
import time
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.distributed_rate_limiter import distributed_rate_limiter
from jobqueue.core.queue_config import queue_config_manager, QueueRateLimitConfig
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task, TaskPriority


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_test_queue(redis_connection):
    """Clean test queue before each test."""
    queue_name = "test_rate_limit_queue"
    
    # Reset rate limiter
    distributed_rate_limiter.reset(queue_name)
    
    # Clean queue
    queue = Queue(queue_name)
    queue.purge()
    
    yield queue_name
    
    # Cleanup after test
    distributed_rate_limiter.reset(queue_name)
    queue.purge()


def test_rate_limit_configuration(clean_test_queue):
    """Test rate limit configuration management."""
    queue_name = clean_test_queue
    
    # Set rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=10,
        burst_allowance=2,
        enabled=True
    )
    
    # Get configuration
    config = queue_config_manager.get_rate_limit(queue_name)
    
    assert config.max_tasks_per_minute == 10
    assert config.burst_allowance == 2
    assert config.enabled is True
    assert config.max_tasks_per_window == 12  # 10 + 2 burst


def test_basic_rate_limiting(clean_test_queue):
    """Test basic rate limiting functionality."""
    queue_name = clean_test_queue
    
    # Set rate limit to 5 tasks per minute
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=5,
        burst_allowance=0,
        enabled=True
    )
    
    # Acquire 5 times should succeed
    for i in range(5):
        acquired = distributed_rate_limiter.acquire(queue_name)
        assert acquired is True, f"Acquisition {i+1} should succeed"
    
    # 6th acquisition should fail
    acquired = distributed_rate_limiter.acquire(queue_name)
    assert acquired is False, "6th acquisition should fail (rate limit hit)"
    
    # Check stats
    stats = distributed_rate_limiter.get_stats(queue_name)
    assert stats["current_count"] == 5
    assert stats["remaining_capacity"] == 0


def test_burst_allowance(clean_test_queue):
    """Test burst allowance feature."""
    queue_name = clean_test_queue
    
    # Set rate limit with burst
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=5,
        burst_allowance=3,
        enabled=True
    )
    
    # Acquire 5 times (normal limit)
    for i in range(5):
        acquired = distributed_rate_limiter.acquire(queue_name)
        assert acquired is True
    
    # Acquire 3 more times using burst
    for i in range(3):
        acquired = distributed_rate_limiter.acquire(queue_name)
        assert acquired is True, f"Burst acquisition {i+1} should succeed"
    
    # Check burst usage
    burst_used = distributed_rate_limiter.get_burst_usage(queue_name)
    assert burst_used == 3
    
    # 9th acquisition should fail (5 + 3 burst exhausted)
    acquired = distributed_rate_limiter.acquire(queue_name)
    assert acquired is False


def test_sliding_window(clean_test_queue):
    """Test sliding window behavior."""
    queue_name = clean_test_queue
    
    # Set rate limit with small window for testing
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=3,
        burst_allowance=0,
        enabled=True
    )
    
    # Use a short window (2 seconds) for testing
    distributed_rate_limiter.window_size = 2
    
    # Acquire 3 times
    for i in range(3):
        acquired = distributed_rate_limiter.acquire(queue_name)
        assert acquired is True
    
    # 4th should fail
    acquired = distributed_rate_limiter.acquire(queue_name)
    assert acquired is False
    
    # Wait for window to slide (2+ seconds)
    time.sleep(2.5)
    
    # Should be able to acquire again
    acquired = distributed_rate_limiter.acquire(queue_name)
    assert acquired is True, "Should acquire after window slides"
    
    # Reset window size
    distributed_rate_limiter.window_size = 60


def test_wait_time_calculation(clean_test_queue):
    """Test wait time until capacity calculation."""
    queue_name = clean_test_queue
    
    # Set rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=2,
        burst_allowance=0,
        enabled=True
    )
    
    # Use short window
    distributed_rate_limiter.window_size = 3
    
    # Acquire to hit limit
    distributed_rate_limiter.acquire(queue_name)
    distributed_rate_limiter.acquire(queue_name)
    
    # Check wait time
    wait_time = distributed_rate_limiter.wait_time_until_capacity(queue_name)
    
    # Wait time should be approximately the window size
    assert wait_time > 0
    assert wait_time <= 3.5
    
    # Reset
    distributed_rate_limiter.window_size = 60


def test_unlimited_rate_limit(clean_test_queue):
    """Test unlimited (disabled) rate limiting."""
    queue_name = clean_test_queue
    
    # Set unlimited
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=0,
        burst_allowance=0,
        enabled=False
    )
    
    # Should acquire unlimited times
    for i in range(100):
        acquired = distributed_rate_limiter.acquire(queue_name)
        assert acquired is True


def test_rate_limit_reset(clean_test_queue):
    """Test rate limit reset functionality."""
    queue_name = clean_test_queue
    
    # Set rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=2,
        burst_allowance=1,
        enabled=True
    )
    
    # Acquire some tasks
    distributed_rate_limiter.acquire(queue_name)
    distributed_rate_limiter.acquire(queue_name)
    distributed_rate_limiter.acquire(queue_name)  # Uses burst
    
    # Check usage
    stats = distributed_rate_limiter.get_stats(queue_name)
    assert stats["current_count"] == 3
    assert stats["burst_used"] == 1
    
    # Reset
    distributed_rate_limiter.reset(queue_name)
    
    # Check stats after reset
    stats = distributed_rate_limiter.get_stats(queue_name)
    assert stats["current_count"] == 0
    assert stats["burst_used"] == 0
    
    # Should be able to acquire again
    acquired = distributed_rate_limiter.acquire(queue_name)
    assert acquired is True


def test_distributed_rate_limiting():
    """Test rate limiting across multiple simulated workers."""
    queue_name = "test_distributed_queue"
    
    # Setup
    redis_broker.connect()
    distributed_rate_limiter.reset(queue_name)
    
    # Set rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=10,
        burst_allowance=0,
        enabled=True
    )
    
    # Simulate multiple workers acquiring concurrently
    acquired_count = 0
    for i in range(15):
        if distributed_rate_limiter.acquire(queue_name):
            acquired_count += 1
    
    # Only 10 should succeed
    assert acquired_count == 10
    
    # Cleanup
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


def test_rate_limit_stats(clean_test_queue):
    """Test rate limit statistics."""
    queue_name = clean_test_queue
    
    # Set rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=10,
        burst_allowance=2,
        enabled=True
    )
    
    # Acquire some tasks
    for i in range(7):
        distributed_rate_limiter.acquire(queue_name)
    
    # Get stats
    stats = distributed_rate_limiter.get_stats(queue_name)
    
    assert stats["queue"] == queue_name
    assert stats["max_tasks_per_minute"] == 10
    assert stats["burst_allowance"] == 2
    assert stats["current_count"] == 7
    assert stats["remaining_capacity"] == 3
    assert stats["burst_remaining"] == 2
    assert stats["rate_limit_enabled"] is True


def test_rate_limit_with_actual_queue(clean_test_queue):
    """Test rate limiting integrated with actual queue operations."""
    queue_name = clean_test_queue
    queue = Queue(queue_name)
    
    # Set rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=5,
        burst_allowance=0,
        enabled=True
    )
    
    # Enqueue tasks
    tasks = []
    for i in range(10):
        task = Task(
            name=f"test_task_{i}",
            args=[i],
            priority=TaskPriority.MEDIUM,
            queue_name=queue_name
        )
        queue.enqueue(task)
        tasks.append(task)
    
    # Verify tasks are queued
    assert queue.size() == 10
    
    # Simulate worker dequeuing with rate limiting
    dequeued_count = 0
    for i in range(10):
        # Check rate limit
        can_proceed = distributed_rate_limiter.acquire(queue_name)
        
        if can_proceed:
            task = queue.dequeue_nowait()
            if task:
                dequeued_count += 1
    
    # Only 5 should be dequeued due to rate limit
    assert dequeued_count == 5
    assert queue.size() == 5  # 5 remaining


def test_concurrent_rate_limiting():
    """Test that rate limiting is atomic across concurrent accesses."""
    import threading
    
    queue_name = "test_concurrent_queue"
    redis_broker.connect()
    distributed_rate_limiter.reset(queue_name)
    
    # Set strict rate limit
    queue_config_manager.set_rate_limit(
        queue_name=queue_name,
        max_tasks_per_minute=20,
        burst_allowance=0,
        enabled=True
    )
    
    acquired = []
    lock = threading.Lock()
    
    def worker_acquire(worker_id):
        """Simulate worker acquiring tasks."""
        for _ in range(5):
            if distributed_rate_limiter.acquire(queue_name):
                with lock:
                    acquired.append(worker_id)
            time.sleep(0.01)
    
    # Create 10 threads (simulating 10 workers)
    threads = []
    for i in range(10):
        t = threading.Thread(target=worker_acquire, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads
    for t in threads:
        t.join()
    
    # Total acquired should not exceed limit
    assert len(acquired) == 20, f"Expected 20 acquisitions, got {len(acquired)}"
    
    # Cleanup
    distributed_rate_limiter.reset(queue_name)
    redis_broker.disconnect()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

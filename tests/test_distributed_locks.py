"""
Tests for distributed locks to prevent concurrent execution of same task.
"""
import pytest
import time
import threading
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task
from jobqueue.core.redis_queue import Queue
from jobqueue.core.distributed_lock import (
    DistributedLock,
    TaskLockManager,
    task_lock_manager
)
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_locks(redis_connection):
    """Clean locks before each test."""
    # Clean all test locks
    keys = redis_broker.client.keys("lock:*")
    if keys:
        redis_broker.client.delete(*keys)
    yield
    # Cleanup
    keys = redis_broker.client.keys("lock:*")
    if keys:
        redis_broker.client.delete(*keys)


@task_registry.register("test_lock_task")
def test_lock_task(value):
    """Test task function."""
    time.sleep(0.1)  # Simulate work
    return f"Processed: {value}"


def test_lock_acquisition(clean_locks):
    """Test basic lock acquisition."""
    lock = DistributedLock("lock:test", ttl=60)
    
    # Acquire lock
    acquired = lock.acquire()
    assert acquired is True
    assert lock.is_acquired is True
    
    # Release lock
    released = lock.release()
    assert released is True
    assert lock.is_acquired is False


def test_lock_already_held(clean_locks):
    """Test that lock cannot be acquired if already held."""
    lock1 = DistributedLock("lock:test", ttl=60)
    lock2 = DistributedLock("lock:test", ttl=60)
    
    # First lock acquires
    acquired1 = lock1.acquire()
    assert acquired1 is True
    
    # Second lock cannot acquire (no timeout)
    acquired2 = lock2.acquire(timeout=0)
    assert acquired2 is False
    
    # Release first lock
    lock1.release()
    
    # Now second lock can acquire
    acquired2 = lock2.acquire()
    assert acquired2 is True


def test_lock_ttl(clean_locks):
    """Test that lock has TTL and expires."""
    lock = DistributedLock("lock:test", ttl=2)  # 2 second TTL
    
    # Acquire lock
    acquired = lock.acquire()
    assert acquired is True
    
    # Check TTL
    ttl = lock.get_remaining_ttl()
    assert ttl > 0
    assert ttl <= 2
    
    # Wait for expiration
    time.sleep(3)
    
    # Lock should be expired
    is_locked = lock.is_locked()
    assert is_locked is False


def test_lock_renewal(clean_locks):
    """Test lock renewal for long-running tasks."""
    lock = DistributedLock("lock:test", ttl=5)
    
    # Acquire lock
    acquired = lock.acquire()
    assert acquired is True
    
    # Wait a bit
    time.sleep(2)
    
    # Renew lock
    renewed = lock.renew(additional_ttl=10)
    assert renewed is True
    
    # Check TTL is extended
    ttl = lock.get_remaining_ttl()
    assert ttl > 5  # Should be around 10 seconds now


def test_lock_context_manager(clean_locks):
    """Test lock as context manager."""
    with DistributedLock("lock:test", ttl=60) as lock:
        assert lock.is_acquired is True
        assert lock.is_locked() is True
    
    # Lock should be released after context exit
    assert lock.is_acquired is False
    assert lock.is_locked() is False


def test_lock_retry_with_timeout(clean_locks):
    """Test lock acquisition with retry and timeout."""
    lock1 = DistributedLock("lock:test", ttl=60)
    lock2 = DistributedLock("lock:test", ttl=60)
    
    # First lock acquires
    lock1.acquire()
    
    # Second lock tries with timeout
    start_time = time.time()
    acquired2 = lock2.acquire(timeout=2, retry_interval=0.1)
    elapsed = time.time() - start_time
    
    # Should have retried for about 2 seconds
    assert acquired2 is False
    assert elapsed >= 1.5  # At least 1.5 seconds (allowing for timing variance)
    assert elapsed <= 3.0  # But not too long


def test_task_lock_manager(clean_locks):
    """Test TaskLockManager."""
    task_signature = "test_signature_123"
    
    # Acquire task lock
    lock = task_lock_manager.acquire_task_lock(
        task_signature=task_signature,
        ttl=60,
        timeout=0
    )
    
    assert lock is not None
    assert lock.is_acquired is True
    
    # Check if task is locked
    is_locked = task_lock_manager.is_task_locked(task_signature)
    assert is_locked is True
    
    # Get TTL
    ttl = task_lock_manager.get_task_lock_ttl(task_signature)
    assert ttl > 0
    
    # Release lock
    lock.release()
    
    # Check if task is still locked
    is_locked = task_lock_manager.is_task_locked(task_signature)
    assert is_locked is False


def test_same_task_twice_second_waits_for_first(clean_locks):
    """
    Test main case: Same task twice → second waits for first.
    """
    print("\n" + "=" * 60)
    print("Test: Same Task Twice → Second Waits for First")
    print("=" * 60)
    
    queue = Queue("test_lock_queue")
    queue.purge()
    
    print(f"\nScenario:")
    print(f"  1. Enqueue same task twice")
    print(f"  2. First task acquires lock and executes")
    print(f"  3. Second task waits for lock")
    print(f"  4. First task completes and releases lock")
    print(f"  5. Second task acquires lock and executes\n")
    
    # Step 1: Create task
    print(f"[Step 1] Creating task...")
    task1 = Task(
        name="test_lock_task",
        args=[1],
        queue_name="test_lock_queue"
    )
    task1.compute_and_set_signature()
    
    task2 = Task(
        name="test_lock_task",
        args=[1],  # Same args = same signature
        queue_name="test_lock_queue"
    )
    task2.compute_and_set_signature()
    
    print(f"  Task 1 signature: {task1.task_signature[:16]}")
    print(f"  Task 2 signature: {task2.task_signature[:16]}")
    assert task1.task_signature == task2.task_signature
    
    # Step 2: Enqueue both tasks
    print(f"\n[Step 2] Enqueuing both tasks...")
    queue.enqueue(task1)
    queue.enqueue(task2)
    print(f"  Queue size: {queue.size()}")
    
    # Step 3: Simulate worker processing
    print(f"\n[Step 3] Simulating worker processing...")
    execution_order = []
    execution_lock = threading.Lock()
    
    def worker_process(task_num, task):
        """Worker function that processes task."""
        # Acquire lock
        lock = task_lock_manager.acquire_task_lock(
            task_signature=task.task_signature,
            ttl=60,
            timeout=10,  # Wait up to 10 seconds
            retry_interval=0.1
        )
        
        if lock:
            with execution_lock:
                execution_order.append(f"Task {task_num} started")
            
            # Simulate work
            time.sleep(0.5)
            
            with execution_lock:
                execution_order.append(f"Task {task_num} completed")
            
            # Release lock
            lock.release()
        else:
            with execution_lock:
                execution_order.append(f"Task {task_num} failed to acquire lock")
    
    # Start both workers
    thread1 = threading.Thread(target=worker_process, args=(1, task1))
    thread2 = threading.Thread(target=worker_process, args=(2, task2))
    
    thread1.start()
    time.sleep(0.1)  # Small delay to ensure thread1 starts first
    thread2.start()
    
    # Wait for both threads
    thread1.join(timeout=5)
    thread2.join(timeout=5)
    
    # Step 4: Verify execution order
    print(f"\n[Step 4] Execution order:")
    for i, event in enumerate(execution_order, 1):
        print(f"  {i}. {event}")
    
    # Step 5: Verification
    print(f"\n[Step 5] Verification:")
    
    # Both tasks should have executed
    assert len(execution_order) == 4, f"Expected 4 events, got {len(execution_order)}"
    
    # Task 1 should start first
    assert execution_order[0] == "Task 1 started"
    print(f"  ✓ Task 1 started first")
    
    # Task 1 should complete before Task 2 starts
    # (Task 2 waits for lock)
    task1_completed_idx = execution_order.index("Task 1 completed")
    task2_started_idx = execution_order.index("Task 2 started")
    
    assert task1_completed_idx < task2_started_idx, \
        "Task 1 should complete before Task 2 starts"
    print(f"  ✓ Task 1 completed before Task 2 started")
    
    # Task 2 should start after Task 1 completes
    assert task2_started_idx > task1_completed_idx
    print(f"  ✓ Task 2 started after Task 1 completed")
    
    # Task 2 should complete
    assert "Task 2 completed" in execution_order
    print(f"  ✓ Task 2 completed")
    
    print(f"\nTest: PASS - Same task twice, second waited for first")
    
    queue.purge()


def test_lock_prevents_deadlock_with_ttl(clean_locks):
    """Test that TTL prevents deadlocks if worker dies."""
    lock = DistributedLock("lock:test", ttl=2)
    
    # Acquire lock
    lock.acquire()
    assert lock.is_locked() is True
    
    # Simulate worker death (lock not released)
    # Lock should expire after TTL
    
    # Wait for expiration
    time.sleep(3)
    
    # Lock should be expired
    is_locked = lock.is_locked()
    assert is_locked is False
    
    # New lock can be acquired
    lock2 = DistributedLock("lock:test", ttl=60)
    acquired = lock2.acquire()
    assert acquired is True


def test_lock_renewal_for_long_tasks(clean_locks):
    """Test lock renewal for long-running tasks."""
    lock = DistributedLock("lock:test", ttl=5)
    
    # Acquire lock
    lock.acquire()
    
    # Simulate long-running task
    for i in range(3):
        time.sleep(1)
        # Renew lock every second
        lock.renew(additional_ttl=5)
    
    # Lock should still be held
    assert lock.is_locked() is True
    assert lock.is_acquired is True
    
    # TTL should be extended
    ttl = lock.get_remaining_ttl()
    assert ttl > 0
    
    # Release lock
    lock.release()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

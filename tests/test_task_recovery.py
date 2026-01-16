"""
Tests for task recovery from crashed workers.
"""
import pytest
import time
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_recovery import TaskRecovery, task_recovery
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_registry import task_registry
from datetime import datetime


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_recovery(redis_connection):
    """Clean recovery state before each test."""
    # Clean workers
    workers = worker_heartbeat.get_all_workers()
    for worker_id in workers:
        worker_heartbeat.remove_worker(worker_id)
    
    yield
    
    # Cleanup
    workers = worker_heartbeat.get_all_workers()
    for worker_id in workers:
        worker_heartbeat.remove_worker(worker_id)


@task_registry.register("test_task")
def test_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_add_active_task(clean_recovery):
    """Test adding task to active set."""
    worker_id = "test_worker_1"
    task = Task(name="test_task", args=[1], queue_name="test_queue")
    
    # Add to active set
    success = task_recovery.add_active_task(worker_id, task)
    
    assert success
    
    # Verify task in active set
    active_tasks = task_recovery.get_active_tasks(worker_id)
    assert len(active_tasks) == 1
    assert active_tasks[0].id == task.id


def test_remove_active_task(clean_recovery):
    """Test removing task from active set."""
    worker_id = "test_worker_2"
    task = Task(name="test_task", args=[1], queue_name="test_queue")
    
    # Add then remove
    task_recovery.add_active_task(worker_id, task)
    assert len(task_recovery.get_active_tasks(worker_id)) == 1
    
    task_recovery.remove_active_task(worker_id, task)
    assert len(task_recovery.get_active_tasks(worker_id)) == 0


def test_task_lock_mechanism(clean_recovery):
    """Test task lock mechanism for duplicate prevention."""
    task_id = "test_task_lock"
    worker_id = "test_worker_3"
    
    task = Task(id=task_id, name="test_task", args=[1], queue_name="test_queue")
    
    # Add active task (creates lock)
    task_recovery.add_active_task(worker_id, task)
    
    # Check if locked
    assert task_recovery.is_task_locked(task_id)
    
    # Get lock owner
    owner = task_recovery.get_task_lock_owner(task_id)
    assert owner == worker_id
    
    # Remove task (releases lock)
    task_recovery.remove_active_task(worker_id, task)
    
    # Should not be locked
    assert not task_recovery.is_task_locked(task_id)


def test_get_orphaned_tasks(clean_recovery):
    """Test detecting orphaned tasks."""
    worker_id = "dead_worker"
    queue_name = "test_queue"
    
    # Create task
    task = Task(name="test_task", args=[1], queue_name=queue_name)
    
    # Add to active set
    task_recovery.add_active_task(worker_id, task)
    
    # Mark worker as dead (no heartbeat)
    # Wait for stale threshold
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    time.sleep(2.5)
    
    # Get orphaned tasks
    orphaned = task_recovery.get_orphaned_tasks()
    
    # Should find orphaned task
    assert len(orphaned) >= 1
    
    # Find our task
    found = False
    for w_id, t in orphaned:
        if t.id == task.id and w_id == worker_id:
            found = True
            break
    
    assert found


def test_crash_worker_verify_task_recovery(clean_recovery):
    """
    Test main case: Crash worker, verify task recovery.
    """
    worker_id = "crashed_worker"
    queue_name = "test_recovery_queue"
    
    # Clean queue
    queue = Queue(queue_name)
    queue.purge()
    
    # Create task
    task = Task(
        name="test_task",
        args=["recovery_test"],
        queue_name=queue_name
    )
    
    print(f"\nScenario:")
    print(f"  1. Worker starts processing task")
    print(f"  2. Worker crashes mid-execution")
    print(f"  3. Monitor detects orphaned task")
    print(f"  4. Task is recovered and re-queued\n")
    
    # Simulate worker starting task
    task.mark_running(worker_id)
    task.started_at = datetime.utcnow()
    
    # Add to active set
    task_recovery.add_active_task(worker_id, task)
    
    print(f"Task {task.id} added to active set for worker {worker_id}")
    print(f"Queue size: {queue.size()}")
    
    # Verify task is active
    active_tasks = task_recovery.get_active_tasks(worker_id)
    assert len(active_tasks) == 1
    assert active_tasks[0].id == task.id
    
    # Simulate worker crash (stop sending heartbeats)
    print(f"\nWorker {worker_id} crashes (stops sending heartbeats)...")
    
    # Wait for stale threshold
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    time.sleep(2.5)
    
    # Worker should be detected as dead
    stale_workers = heartbeat_manager.get_stale_workers()
    assert worker_id in stale_workers
    
    print(f"Dead worker detected: {worker_id}")
    
    # Get orphaned tasks
    orphaned = task_recovery.get_orphaned_tasks()
    print(f"Orphaned tasks found: {len(orphaned)}")
    
    # Recover orphaned tasks
    recovered_count = task_recovery.recover_orphaned_tasks()
    
    print(f"\nRecovery Results:")
    print(f"  Tasks recovered: {recovered_count}")
    print(f"  Queue size: {queue.size()}")
    
    # Verify task recovered
    assert recovered_count == 1
    assert queue.size() == 1
    
    # Get recovered task
    recovered_task = queue.dequeue_nowait()
    assert recovered_task is not None
    assert recovered_task.id == task.id
    assert recovered_task.status == TaskStatus.PENDING
    assert recovered_task.worker_id is None
    
    print(f"\nRecovered Task:")
    print(f"  Task ID: {recovered_task.id}")
    print(f"  Status: {recovered_task.status.value}")
    print(f"  Worker ID: {recovered_task.worker_id}")
    
    print(f"\nTest: PASS - Task successfully recovered")
    
    # Cleanup
    queue.purge()


def test_prevent_duplicate_execution(clean_recovery):
    """Test lock mechanism prevents duplicate execution."""
    task_id = "duplicate_test_task"
    worker1 = "worker_1"
    worker2 = "worker_2"
    
    task = Task(id=task_id, name="test_task", args=[1], queue_name="test_queue")
    
    # Worker 1 starts task (acquires lock)
    task_recovery.add_active_task(worker1, task)
    
    assert task_recovery.is_task_locked(task_id)
    assert task_recovery.get_task_lock_owner(task_id) == worker1
    
    # Worker 2 tries to start same task
    # Should detect it's already locked
    is_locked = task_recovery.is_task_locked(task_id)
    assert is_locked
    
    # Worker 1 completes (releases lock)
    task_recovery.remove_active_task(worker1, task)
    
    # Now worker 2 can acquire lock
    assert not task_recovery.is_task_locked(task_id)
    
    task2 = Task(id=task_id, name="test_task", args=[1], queue_name="test_queue")
    task_recovery.add_active_task(worker2, task2)
    
    assert task_recovery.get_task_lock_owner(task_id) == worker2


def test_recovery_lock_prevents_duplicates(clean_recovery):
    """Test recovery lock prevents duplicate recovery."""
    task_id = "recovery_lock_test"
    worker_id = "dead_worker_2"
    
    task = Task(id=task_id, name="test_task", args=[1], queue_name="test_queue")
    
    # Add to active set
    task_recovery.add_active_task(worker_id, task)
    
    # Try to acquire recovery lock
    lock1 = task_recovery._acquire_recovery_lock(task_id)
    assert lock1 is True
    
    # Try to acquire again (should fail - already locked)
    lock2 = task_recovery._acquire_recovery_lock(task_id)
    assert lock2 is False
    
    # Release lock
    task_recovery._release_recovery_lock(task_id)
    
    # Now can acquire again
    lock3 = task_recovery._acquire_recovery_lock(task_id)
    assert lock3 is True
    
    task_recovery._release_recovery_lock(task_id)


def test_multiple_orphaned_tasks(clean_recovery):
    """Test recovering multiple orphaned tasks."""
    worker_id = "dead_worker_3"
    queue_name = "test_queue"
    
    # Create multiple tasks
    tasks = []
    for i in range(3):
        task = Task(name="test_task", args=[i], queue_name=queue_name)
        task.mark_running(worker_id)
        task_recovery.add_active_task(worker_id, task)
        tasks.append(task)
    
    # Mark worker as dead
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    time.sleep(2.5)
    
    # Get orphaned tasks
    orphaned = task_recovery.get_orphaned_tasks()
    
    # Should find all 3 tasks
    assert len(orphaned) >= 3
    
    # Recover all
    recovered = task_recovery.recover_orphaned_tasks()
    assert recovered == 3


def test_recovery_stats(clean_recovery):
    """Test getting recovery statistics."""
    worker_id = "stats_worker"
    
    # Add orphaned tasks
    for i in range(2):
        task = Task(name="test_task", args=[i], queue_name="queue1")
        task_recovery.add_active_task(worker_id, task)
    
    for i in range(1):
        task = Task(name="test_task", args=[i], queue_name="queue2")
        task_recovery.add_active_task(worker_id, task)
    
    # Mark worker as dead
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    time.sleep(2.5)
    
    # Get stats
    stats = task_recovery.get_recovery_stats()
    
    assert stats["orphaned_tasks"] >= 3
    assert worker_id in stats["by_worker"]
    assert "queue1" in stats["by_queue"]
    assert "queue2" in stats["by_queue"]


def test_active_tasks_persistence(clean_recovery):
    """Test that active tasks persist across operations."""
    worker_id = "persistence_worker"
    
    # Add multiple tasks
    tasks = []
    for i in range(5):
        task = Task(name="test_task", args=[i], queue_name="test_queue")
        task_recovery.add_active_task(worker_id, task)
        tasks.append(task)
    
    # Verify all stored
    active = task_recovery.get_active_tasks(worker_id)
    assert len(active) == 5
    
    # Remove one
    task_recovery.remove_active_task(worker_id, tasks[0])
    
    # Verify count decreased
    active = task_recovery.get_active_tasks(worker_id)
    assert len(active) == 4


def test_recovery_with_task_lock(clean_recovery):
    """Test recovery respects task locks."""
    task_id = "lock_test_task"
    worker_id = "dead_worker_4"
    queue_name = "test_queue"
    
    task = Task(id=task_id, name="test_task", args=[1], queue_name=queue_name)
    
    # Add to active set (creates lock)
    task_recovery.add_active_task(worker_id, task)
    
    # Verify locked
    assert task_recovery.is_task_locked(task_id)
    
    # Mark worker as dead
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    time.sleep(2.5)
    
    # Recover task
    queue = Queue(queue_name)
    queue.purge()
    
    recovered = task_recovery.recover_orphaned_tasks()
    
    assert recovered == 1
    
    # Lock should be released after recovery
    # (Task is re-queued, new worker can acquire lock)
    time.sleep(0.2)
    
    # Cleanup
    queue.purge()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

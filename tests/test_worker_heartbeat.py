"""
Tests for worker heartbeat and monitoring.
"""
import pytest
import time
import threading
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.worker_heartbeat import (
    WorkerHeartbeat,
    worker_heartbeat,
    WorkerStatus
)
from jobqueue.core.worker_monitor import WorkerMonitor
from jobqueue.core.task import Task, TaskStatus
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
def clean_workers(redis_connection):
    """Clean worker heartbeats before each test."""
    # Get all workers and remove them
    workers = worker_heartbeat.get_all_workers()
    for worker_id in workers:
        worker_heartbeat.remove_worker(worker_id)
    
    yield
    
    # Cleanup
    workers = worker_heartbeat.get_all_workers()
    for worker_id in workers:
        worker_heartbeat.remove_worker(worker_id)


@task_registry.register("long_task")
def long_task(duration: float):
    """Task that runs for specified duration."""
    time.sleep(duration)
    return f"Completed after {duration}s"


def test_send_heartbeat(clean_workers):
    """Test sending heartbeat from worker."""
    worker_id = "test_worker_1"
    
    # Send heartbeat
    success = worker_heartbeat.send_heartbeat(
        worker_id=worker_id,
        status=WorkerStatus.IDLE
    )
    
    assert success
    
    # Verify heartbeat stored
    heartbeat = worker_heartbeat.get_heartbeat(worker_id)
    assert heartbeat is not None
    assert isinstance(heartbeat, float)


def test_heartbeat_ttl(clean_workers):
    """Test that heartbeat has TTL."""
    worker_id = "test_worker_2"
    
    # Send heartbeat with short TTL
    heartbeat_manager = WorkerHeartbeat(heartbeat_ttl=2)
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    # Should exist
    assert heartbeat_manager.get_heartbeat(worker_id) is not None
    
    # Wait for TTL to expire
    time.sleep(2.5)
    
    # Should be gone
    assert heartbeat_manager.get_heartbeat(worker_id) is None


def test_worker_status_tracking(clean_workers):
    """Test worker status tracking."""
    worker_id = "test_worker_3"
    
    # Send heartbeat with ACTIVE status
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    
    status = worker_heartbeat.get_worker_status(worker_id)
    assert status == WorkerStatus.ACTIVE
    
    # Change to IDLE
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    status = worker_heartbeat.get_worker_status(worker_id)
    assert status == WorkerStatus.IDLE


def test_is_worker_alive(clean_workers):
    """Test checking if worker is alive."""
    worker_id = "test_worker_4"
    
    # No heartbeat - not alive
    assert not worker_heartbeat.is_worker_alive(worker_id)
    
    # Send heartbeat
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    # Should be alive
    assert worker_heartbeat.is_worker_alive(worker_id)
    
    # Wait for stale threshold (30s)
    # Use shorter threshold for test
    heartbeat_manager = WorkerHeartbeat(stale_threshold=2)
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    time.sleep(2.5)
    
    # Should be stale (dead)
    assert not heartbeat_manager.is_worker_alive(worker_id)


def test_get_stale_workers(clean_workers):
    """Test detecting stale workers."""
    # Create heartbeat manager with short threshold
    heartbeat_manager = WorkerHeartbeat(stale_threshold=2)
    
    # Add alive worker
    worker1 = "worker_alive"
    heartbeat_manager.send_heartbeat(worker1, WorkerStatus.IDLE)
    
    # Add stale worker (old heartbeat)
    worker2 = "worker_stale"
    # Manually set old timestamp
    key = heartbeat_manager._get_heartbeat_key(worker2)
    old_time = time.time() - 10  # 10 seconds ago
    redis_broker.client.setex(key, 30, str(old_time))
    
    # Get stale workers
    stale = heartbeat_manager.get_stale_workers()
    
    # worker2 should be stale
    assert worker2 in stale
    # worker1 should not be stale (yet)
    assert worker1 not in stale


def test_worker_metadata(clean_workers):
    """Test storing worker metadata."""
    worker_id = "test_worker_5"
    
    metadata = {
        "hostname": "test-host",
        "pid": 12345,
        "queue_name": "test_queue"
    }
    
    worker_heartbeat.send_heartbeat(
        worker_id=worker_id,
        status=WorkerStatus.ACTIVE,
        metadata=metadata
    )
    
    # Get worker info
    info = worker_heartbeat.get_worker_info(worker_id)
    
    assert info["metadata"]["hostname"] == "test-host"
    assert info["metadata"]["pid"] == 12345
    assert info["metadata"]["queue_name"] == "test_queue"


def test_kill_worker_mid_task_verify_task_requeued(clean_workers):
    """
    Test main case: Kill worker mid-task, verify task re-queued.
    """
    worker_id = "worker_killed_mid_task"
    queue_name = "test_recovery_queue"
    
    # Clean queue
    queue = Queue(queue_name)
    queue.purge()
    
    # Create task
    task = Task(
        name="long_task",
        args=[5.0],  # Runs for 5 seconds
        queue_name=queue_name,
        timeout=10
    )
    
    # Mark task as running on worker
    task.mark_running(worker_id)
    task.started_at = datetime.utcnow()
    
    # Store task as running
    key = f"task:running:{worker_id}:{task.id}"
    task_json = task.to_json()
    redis_broker.client.setex(key, 3600, task_json)
    
    # Verify task is stored
    stored_task_json = redis_broker.client.get(key)
    assert stored_task_json is not None
    
    # Simulate worker death (stop sending heartbeats)
    # Wait for stale threshold
    heartbeat_manager = WorkerHeartbeat(stale_threshold=2)
    
    # Send initial heartbeat
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    
    # Wait for worker to become stale
    time.sleep(2.5)
    
    # Worker should be detected as dead
    stale_workers = heartbeat_manager.get_stale_workers()
    assert worker_id in stale_workers
    
    # Create monitor to recover tasks
    monitor = WorkerMonitor(stale_threshold=2)
    
    # Recover tasks
    recovered = monitor._recover_worker_tasks(worker_id)
    
    # Should have recovered 1 task
    assert recovered == 1
    
    # Task should be re-queued
    assert queue.size() == 1
    
    # Get re-queued task
    recovered_task = queue.dequeue_nowait()
    assert recovered_task is not None
    assert recovered_task.id == task.id
    assert recovered_task.status == TaskStatus.PENDING
    assert recovered_task.worker_id is None
    
    # Cleanup
    queue.purge()


def test_worker_monitor_detection(clean_workers):
    """Test worker monitor detects dead workers."""
    # Create monitor with short threshold
    monitor = WorkerMonitor(check_interval=1, stale_threshold=2)
    
    # Add worker
    worker_id = "monitor_test_worker"
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    # Wait for stale
    time.sleep(2.5)
    
    # Check for stale workers
    stale = worker_heartbeat.get_stale_workers()
    
    assert worker_id in stale


def test_get_all_workers(clean_workers):
    """Test getting all workers."""
    # Add multiple workers
    for i in range(3):
        worker_id = f"worker_{i}"
        worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    # Get all workers
    workers = worker_heartbeat.get_all_workers()
    
    assert len(workers) == 3
    assert "worker_0" in workers
    assert "worker_1" in workers
    assert "worker_2" in workers


def test_get_all_workers_info(clean_workers):
    """Test getting information about all workers."""
    # Add workers
    worker1 = "info_worker_1"
    worker2 = "info_worker_2"
    
    worker_heartbeat.send_heartbeat(worker1, WorkerStatus.ACTIVE)
    worker_heartbeat.send_heartbeat(worker2, WorkerStatus.IDLE)
    
    # Get all info
    workers_info = worker_heartbeat.get_all_workers_info()
    
    assert len(workers_info) == 2
    
    worker1_info = next(w for w in workers_info if w["worker_id"] == worker1)
    assert worker1_info["status"] == WorkerStatus.ACTIVE.value
    
    worker2_info = next(w for w in workers_info if w["worker_id"] == worker2)
    assert worker2_info["status"] == WorkerStatus.IDLE.value


def test_remove_worker(clean_workers):
    """Test removing worker from tracking."""
    worker_id = "remove_test_worker"
    
    # Add worker
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    
    # Verify exists
    assert worker_heartbeat.get_heartbeat(worker_id) is not None
    
    # Remove
    worker_heartbeat.remove_worker(worker_id)
    
    # Verify removed
    assert worker_heartbeat.get_heartbeat(worker_id) is None


def test_worker_status_dead(clean_workers):
    """Test worker status when dead."""
    worker_id = "dead_worker"
    
    # No heartbeat - should be DEAD
    status = worker_heartbeat.get_worker_status(worker_id)
    assert status == WorkerStatus.DEAD
    
    # Mark as dead explicitly
    worker_heartbeat.mark_worker_dead(worker_id)
    
    status = worker_heartbeat.get_worker_status(worker_id)
    assert status == WorkerStatus.DEAD


def test_heartbeat_interval(clean_workers):
    """Test heartbeat interval."""
    worker_id = "interval_test_worker"
    
    # Send first heartbeat
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    heartbeat1 = worker_heartbeat.get_heartbeat(worker_id)
    
    # Wait for interval
    time.sleep(0.5)
    
    # Send second heartbeat
    worker_heartbeat.send_heartbeat(worker_id, WorkerStatus.IDLE)
    heartbeat2 = worker_heartbeat.get_heartbeat(worker_id)
    
    # Second heartbeat should be newer
    assert heartbeat2 > heartbeat1


def test_worker_monitor_stats(clean_workers):
    """Test worker monitor statistics."""
    monitor = WorkerMonitor()
    
    # Add some workers
    for i in range(3):
        worker_heartbeat.send_heartbeat(f"worker_{i}", WorkerStatus.IDLE)
    
    stats = monitor.get_stats()
    
    assert "total_workers" in stats
    assert "alive_workers" in stats
    assert "dead_workers" in stats
    assert stats["total_workers"] == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

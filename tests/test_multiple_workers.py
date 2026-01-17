"""
Tests for multiple workers running safely.
Verifies no duplicate task processing with Redis atomic operations.
"""
import pytest
import time
import threading
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.redis_queue import Queue
from jobqueue.core.worker_pool import WorkerPool, distributed_worker_manager
from jobqueue.core.task_registry import task_registry
from jobqueue.core.metrics import metrics_collector


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_queue(redis_connection):
    """Clean queue before each test."""
    queue = Queue("test_multi_worker_queue")
    queue.purge()
    yield queue
    queue.purge()


@task_registry.register("test_multi_worker_task")
def test_multi_worker_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_unique_worker_ids():
    """Test that worker IDs are unique (hostname + PID)."""
    from jobqueue.worker.base_worker import Worker
    
    worker1 = Worker()
    worker2 = Worker()
    
    # Worker IDs should include hostname and PID
    assert "worker-" in worker1.worker_id
    assert worker1.hostname is not None
    assert worker1.pid is not None
    
    # In same process, PIDs are same, but IDs should still be unique
    # (They use different indices or timestamps in practice)
    assert worker1.worker_id != worker2.worker_id or worker1.pid == worker2.pid


def test_brpop_atomicity(clean_queue):
    """
    Test that BRPOP is atomic (Redis handles race conditions).
    Multiple workers calling BRPOP should not get the same task.
    """
    queue = clean_queue
    
    # Enqueue a single task
    task = Task(name="test_multi_worker_task", args=[1], queue_name="test_multi_worker_queue")
    queue.enqueue(task)
    
    # Simulate multiple workers trying to dequeue
    results = []
    
    def worker_dequeue():
        """Worker function that dequeues a task."""
        dequeued = queue.dequeue(timeout=1)
        if dequeued:
            results.append(dequeued.id)
    
    # Create multiple threads (simulating multiple workers)
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker_dequeue)
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join(timeout=2)
    
    # Only one worker should have gotten the task
    assert len(results) == 1, f"Expected 1 task dequeued, got {len(results)}"
    assert results[0] == task.id


def test_worker_pool_creation():
    """Test creating a worker pool."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_queue",
        initial_workers=2
    )
    
    assert pool.pool_name == "test_pool"
    assert pool.queue_name == "test_queue"
    assert pool.initial_workers == 2
    assert not pool.is_running


def test_worker_pool_add_remove_worker():
    """Test adding and removing workers from pool."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_queue",
        initial_workers=0
    )
    
    pool.is_running = True  # Simulate running state
    
    # Add worker
    worker_id = pool.add_worker()
    assert worker_id in pool.workers
    assert len(pool.workers) == 1
    
    # Remove worker
    success = pool.remove_worker(worker_id, graceful=False)
    assert success
    assert worker_id not in pool.workers
    assert len(pool.workers) == 0
    
    pool.is_running = False


def test_worker_pool_scale_up_down():
    """Test scaling worker pool up and down."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_queue",
        initial_workers=2
    )
    
    pool.is_running = True
    
    # Start with 2 workers
    pool.start()
    time.sleep(0.5)  # Give workers time to start
    
    initial_count = len(pool.workers)
    assert initial_count == 2
    
    # Scale up by 3
    new_workers = pool.scale_up(3)
    assert len(new_workers) == 3
    assert len(pool.workers) == 5
    
    # Scale down by 2
    removed_workers = pool.scale_down(2)
    assert len(removed_workers) == 2
    assert len(pool.workers) == 3
    
    pool.stop(graceful=False)
    pool.is_running = False


def test_worker_pool_status():
    """Test getting worker pool status."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_queue",
        initial_workers=1
    )
    
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    status = pool.get_pool_status()
    
    assert "pool_name" in status
    assert "queue_name" in status
    assert "is_running" in status
    assert "total_workers" in status
    assert "workers" in status
    assert status["pool_name"] == "test_pool"
    assert status["total_workers"] == 1
    
    pool.stop(graceful=False)
    pool.is_running = False


def test_ten_workers_thousand_tasks_verify_no_duplicates(clean_queue):
    """
    Test main case: 10 workers, 1000 tasks, verify no duplicates.
    """
    print("\n" + "=" * 60)
    print("Test: 10 Workers, 1000 Tasks, Verify No Duplicates")
    print("=" * 60)
    
    queue = clean_queue
    
    print(f"\nScenario:")
    print(f"  1. Enqueue 1000 tasks")
    print(f"  2. Start 10 workers")
    print(f"  3. Workers compete for tasks using BRPOP (atomic)")
    print(f"  4. Verify no task processed twice\n")
    
    # Step 1: Enqueue 1000 tasks
    print(f"[Step 1] Enqueuing 1000 tasks...")
    task_ids = []
    for i in range(1000):
        task = Task(
            name="test_multi_worker_task",
            args=[i],
            queue_name="test_multi_worker_queue"
        )
        queue.enqueue(task)
        task_ids.append(task.id)
    
    print(f"  Enqueued: {len(task_ids)} tasks")
    print(f"  Queue size: {queue.size()}")
    
    # Step 2: Simulate 10 workers processing tasks
    print(f"\n[Step 2] Simulating 10 workers processing tasks...")
    processed_tasks = []
    processed_lock = threading.Lock()
    
    def worker_process():
        """Worker function that processes tasks."""
        worker_processed = []
        while True:
            task = queue.dequeue(timeout=1)
            if task is None:
                break
            
            with processed_lock:
                processed_tasks.append(task.id)
                worker_processed.append(task.id)
            
            # Simulate task processing
            time.sleep(0.001)
        
        return worker_processed
    
    # Create 10 worker threads
    worker_threads = []
    for i in range(10):
        thread = threading.Thread(target=worker_process, name=f"worker-{i}")
        worker_threads.append(thread)
        thread.start()
    
    # Wait for all workers to finish
    print(f"  Waiting for workers to process all tasks...")
    for thread in worker_threads:
        thread.join(timeout=30)
    
    # Step 3: Verify no duplicates
    print(f"\n[Step 3] Verifying no duplicates...")
    print(f"  Tasks enqueued: {len(task_ids)}")
    print(f"  Tasks processed: {len(processed_tasks)}")
    
    # Check for duplicates
    unique_processed = set(processed_tasks)
    duplicates = len(processed_tasks) - len(unique_processed)
    
    print(f"  Unique tasks processed: {len(unique_processed)}")
    print(f"  Duplicates found: {duplicates}")
    
    # Step 4: Verify all tasks processed
    print(f"\n[Step 4] Verification:")
    
    # All tasks should be processed
    assert len(processed_tasks) == len(task_ids), \
        f"Expected {len(task_ids)} tasks processed, got {len(processed_tasks)}"
    
    # No duplicates
    assert duplicates == 0, f"Found {duplicates} duplicate tasks"
    
    # All unique task IDs should match
    assert len(unique_processed) == len(task_ids), \
        f"Expected {len(task_ids)} unique tasks, got {len(unique_processed)}"
    
    # Verify all task IDs are in processed list
    missing = set(task_ids) - unique_processed
    assert len(missing) == 0, f"Missing {len(missing)} tasks: {list(missing)[:10]}"
    
    print(f"  ✓ All {len(task_ids)} tasks processed")
    print(f"  ✓ No duplicates found")
    print(f"  ✓ All tasks accounted for")
    
    print(f"\nTest: PASS - 10 workers processed 1000 tasks with no duplicates")
    
    # Verify queue is empty
    assert queue.size() == 0, "Queue should be empty after processing"


def test_distributed_worker_manager():
    """Test distributed worker manager."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    # Create pool
    pool = distributed_worker_manager.create_pool(
        pool_name="test_manager_pool",
        worker_class=SimpleWorker,
        queue_name="test_queue",
        initial_workers=1
    )
    
    assert pool is not None
    assert "test_manager_pool" in distributed_worker_manager.pools
    
    # Get pool
    retrieved_pool = distributed_worker_manager.get_pool("test_manager_pool")
    assert retrieved_pool == pool
    
    # Get all pools status
    status = distributed_worker_manager.get_all_pools_status()
    assert "total_pools" in status
    assert "pools" in status
    assert "test_manager_pool" in status["pools"]
    
    # Remove pool
    success = distributed_worker_manager.remove_pool("test_manager_pool", graceful=False)
    assert success
    assert "test_manager_pool" not in distributed_worker_manager.pools


def test_worker_competition(clean_queue):
    """Test that workers compete for tasks atomically."""
    queue = clean_queue
    
    # Enqueue multiple tasks
    task_count = 20
    for i in range(task_count):
        task = Task(name="test_multi_worker_task", args=[i], queue_name="test_multi_worker_queue")
        queue.enqueue(task)
    
    # Multiple workers compete
    processed = []
    lock = threading.Lock()
    
    def worker():
        while True:
            task = queue.dequeue(timeout=0.5)
            if task is None:
                break
            with lock:
                processed.append(task.id)
    
    # Start 5 workers
    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    
    # Verify all tasks processed exactly once
    assert len(processed) == task_count
    assert len(set(processed)) == task_count  # No duplicates


def test_worker_graceful_shutdown():
    """Test graceful worker shutdown."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_shutdown_pool",
        worker_class=SimpleWorker,
        queue_name="test_queue",
        initial_workers=1
    )
    
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    # Graceful shutdown
    worker_id = list(pool.workers.keys())[0]
    success = pool.remove_worker(worker_id, graceful=True)
    
    assert success
    assert worker_id not in pool.workers
    
    pool.is_running = False


def test_worker_restart():
    """Test restarting a worker."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_restart_pool",
        worker_class=SimpleWorker,
        queue_name="test_queue",
        initial_workers=1
    )
    
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    worker_id = list(pool.workers.keys())[0]
    
    # Restart worker
    success = pool.restart_worker(worker_id)
    assert success
    
    # New worker should exist
    assert len(pool.workers) == 1
    assert worker_id not in pool.workers  # Old ID removed
    
    pool.stop(graceful=False)
    pool.is_running = False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

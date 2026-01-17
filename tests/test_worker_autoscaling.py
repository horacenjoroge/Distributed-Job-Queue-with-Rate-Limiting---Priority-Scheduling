"""
Tests for worker autoscaling.
"""
import pytest
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task
from jobqueue.core.redis_queue import Queue
from jobqueue.core.worker_pool import WorkerPool
from jobqueue.core.worker_autoscaling import (
    WorkerAutoscaler,
    create_autoscaler,
    get_autoscaler,
    remove_autoscaler
)
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_queue(redis_connection):
    """Clean queue before each test."""
    queue = Queue("test_autoscale_queue")
    queue.purge()
    yield queue
    queue.purge()


@task_registry.register("test_autoscale_task")
def test_autoscale_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_autoscaler_initialization(clean_queue):
    """Test autoscaler initialization."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=100,
        scale_down_threshold=10,
        min_workers=1,
        max_workers=10,
        check_interval=5,
        cooldown_seconds=10
    )
    
    assert autoscaler.pool == pool
    assert autoscaler.scale_up_threshold == 100
    assert autoscaler.scale_down_threshold == 10
    assert autoscaler.min_workers == 1
    assert autoscaler.max_workers == 10
    assert autoscaler.check_interval == 5
    assert autoscaler.cooldown_seconds == 10


def test_get_queue_size(clean_queue):
    """Test getting queue size."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=0
    )
    
    autoscaler = WorkerAutoscaler(pool=pool)
    
    # Empty queue
    size = autoscaler._get_queue_size()
    assert size == 0
    
    # Enqueue tasks
    for i in range(5):
        task = Task(name="test_autoscale_task", args=[i], queue_name="test_autoscale_queue")
        clean_queue.enqueue(task)
    
    size = autoscaler._get_queue_size()
    assert size == 5


def test_health_check(clean_queue):
    """Test health check."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=0
    )
    
    autoscaler = WorkerAutoscaler(pool=pool)
    
    # Health check should pass
    healthy = autoscaler._health_check()
    assert healthy is True


def test_should_scale_up(clean_queue):
    """Test scale up decision logic."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=100,
        max_workers=10
    )
    
    # Queue size below threshold
    for i in range(50):
        task = Task(name="test_autoscale_task", args=[i], queue_name="test_autoscale_queue")
        clean_queue.enqueue(task)
    
    should_scale = autoscaler._should_scale_up(50)
    assert should_scale is False
    
    # Queue size above threshold
    for i in range(60):
        task = Task(name="test_autoscale_task", args=[i+50], queue_name="test_autoscale_queue")
        clean_queue.enqueue(task)
    
    should_scale = autoscaler._should_scale_up(110)
    assert should_scale is True
    
    pool.stop(graceful=False)
    pool.is_running = False


def test_should_scale_down(clean_queue):
    """Test scale down decision logic."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=5
    )
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_down_threshold=10,
        min_workers=1
    )
    
    # Queue size above threshold
    should_scale = autoscaler._should_scale_down(20)
    assert should_scale is False
    
    # Queue size below threshold
    should_scale = autoscaler._should_scale_down(5)
    assert should_scale is True
    
    # At min workers
    while len(pool.workers) > 1:
        worker_id = list(pool.workers.keys())[0]
        pool.remove_worker(worker_id, graceful=False)
    
    should_scale = autoscaler._should_scale_down(5)
    assert should_scale is False  # At min workers
    
    pool.stop(graceful=False)
    pool.is_running = False


def test_cooldown_period(clean_queue):
    """Test cooldown period prevents rapid scaling."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    pool.is_running = True
    
    autoscaler = WorkerAutoscaler(
        pool=pool,
        cooldown_seconds=5
    )
    
    # No previous scale action
    assert autoscaler._can_scale() is True
    
    # Record scale action
    autoscaler.last_scale_action = datetime.utcnow()
    
    # Immediately after, should not scale
    assert autoscaler._can_scale() is False
    
    # Wait for cooldown
    time.sleep(6)
    
    # Should be able to scale now
    assert autoscaler._can_scale() is True
    
    pool.is_running = False


def test_min_max_worker_limits(clean_queue):
    """Test min/max worker limits."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    autoscaler = WorkerAutoscaler(
        pool=pool,
        min_workers=2,
        max_workers=5
    )
    
    # Try to scale down below min
    should_scale = autoscaler._should_scale_down(0)
    # Should not scale if at min
    if len(pool.workers) <= autoscaler.min_workers:
        assert should_scale is False
    
    # Scale up to max
    pool.scale_up(10)
    
    # Try to scale up above max
    should_scale = autoscaler._should_scale_up(1000)
    # Should not scale if at max
    if len(pool.workers) >= autoscaler.max_workers:
        assert should_scale is False
    
    pool.stop(graceful=False)
    pool.is_running = False


def test_queue_grows_verify_workers_scale_up(clean_queue):
    """
    Test main case: Queue grows, verify workers scale up.
    """
    print("\n" + "=" * 60)
    print("Test: Queue Grows, Verify Workers Scale Up")
    print("=" * 60)
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_autoscale_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    print(f"\nScenario:")
    print(f"  1. Start with 1 worker")
    print(f"  2. Enqueue tasks to grow queue size > 100")
    print(f"  3. Enable autoscaling")
    print(f"  4. Verify workers scale up\n")
    
    # Step 1: Initial state
    print(f"[Step 1] Initial state:")
    initial_workers = len(pool.workers)
    print(f"  Initial workers: {initial_workers}")
    assert initial_workers == 1
    
    # Step 2: Grow queue
    print(f"\n[Step 2] Growing queue size...")
    for i in range(150):
        task = Task(
            name="test_autoscale_task",
            args=[i],
            queue_name="test_autoscale_queue"
        )
        clean_queue.enqueue(task)
    
    queue_size = clean_queue.size()
    print(f"  Queue size: {queue_size}")
    assert queue_size >= 150
    
    # Step 3: Enable autoscaling
    print(f"\n[Step 3] Enabling autoscaling...")
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=100,
        scale_down_threshold=10,
        min_workers=1,
        max_workers=10,
        check_interval=2,  # Check every 2 seconds for test
        cooldown_seconds=3  # Short cooldown for test
    )
    
    autoscaler.start()
    print(f"  Autoscaler started")
    print(f"  Scale up threshold: {autoscaler.scale_up_threshold}")
    print(f"  Current queue size: {queue_size}")
    
    # Step 4: Wait for autoscaling
    print(f"\n[Step 4] Waiting for autoscaling to trigger...")
    time.sleep(5)  # Wait for check interval + scaling
    
    # Check if scaled up
    current_workers = len(pool.workers)
    print(f"  Workers after autoscaling: {current_workers}")
    print(f"  Workers added: {current_workers - initial_workers}")
    
    # Verify scaling occurred
    status = autoscaler.get_status()
    print(f"\n[Step 5] Autoscaler status:")
    print(f"  Is running: {status['is_running']}")
    print(f"  Queue size: {status['queue_size']}")
    print(f"  Current workers: {status['current_workers']}")
    print(f"  Should scale up: {status['should_scale_up']}")
    
    # Verify workers increased
    assert current_workers > initial_workers, \
        f"Expected workers to increase from {initial_workers}, got {current_workers}"
    
    print(f"\nVerification:")
    print(f"  ✓ Queue size > threshold: {queue_size > autoscaler.scale_up_threshold}")
    print(f"  ✓ Workers scaled up: {current_workers > initial_workers}")
    print(f"  ✓ Workers within limits: {autoscaler.min_workers <= current_workers <= autoscaler.max_workers}")
    
    # Check scaling history
    history = autoscaler.get_scaling_history()
    if history:
        print(f"\nScaling history:")
        for entry in history[-3:]:  # Last 3 entries
            print(f"  {entry['action']}: {entry.get('workers_added', entry.get('workers_removed', 0))} workers at {entry['timestamp']}")
    
    print(f"\nTest: PASS - Queue grew and workers scaled up")
    
    # Cleanup
    autoscaler.stop()
    pool.stop(graceful=False)
    pool.is_running = False


def test_autoscaler_start_stop(clean_queue):
    """Test autoscaler start/stop."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    pool.is_running = True
    
    autoscaler = WorkerAutoscaler(pool=pool, check_interval=1)
    
    # Start
    autoscaler.start()
    assert autoscaler.is_running is True
    assert autoscaler.monitor_thread is not None
    assert autoscaler.monitor_thread.is_alive()
    
    # Wait a bit
    time.sleep(1.5)
    
    # Stop
    autoscaler.stop()
    assert autoscaler.is_running is False
    
    pool.is_running = False


def test_autoscaler_status(clean_queue):
    """Test getting autoscaler status."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=2
    )
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=100,
        scale_down_threshold=10,
        min_workers=1,
        max_workers=10
    )
    
    status = autoscaler.get_status()
    
    assert "pool_name" in status
    assert "is_running" in status
    assert "queue_size" in status
    assert "current_workers" in status
    assert "scale_up_threshold" in status
    assert "scale_down_threshold" in status
    assert "min_workers" in status
    assert "max_workers" in status
    assert "should_scale_up" in status
    assert "should_scale_down" in status
    
    pool.stop(graceful=False)
    pool.is_running = False


def test_autoscaler_scaling_history(clean_queue):
    """Test scaling history tracking."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    pool.is_running = True
    pool.start()
    time.sleep(0.5)
    
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=50,
        cooldown_seconds=1
    )
    
    # Manually trigger scale up (for testing)
    for i in range(60):
        task = Task(name="test_autoscale_task", args=[i], queue_name="test_autoscale_queue")
        clean_queue.enqueue(task)
    
    # Scale up
    autoscaler._scale_up()
    
    # Check history
    history = autoscaler.get_scaling_history()
    assert len(history) > 0
    assert history[-1]["action"] == "scale_up"
    assert "workers_added" in history[-1]
    assert "queue_size" in history[-1]
    
    pool.stop(graceful=False)
    pool.is_running = False


def test_create_autoscaler(clean_queue):
    """Test creating autoscaler via helper function."""
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="test_create_pool",
        worker_class=SimpleWorker,
        queue_name="test_autoscale_queue",
        initial_workers=1
    )
    pool.is_running = True
    
    autoscaler = create_autoscaler(
        pool=pool,
        scale_up_threshold=100,
        scale_down_threshold=10
    )
    
    assert autoscaler is not None
    assert get_autoscaler("test_create_pool") == autoscaler
    
    # Remove
    success = remove_autoscaler("test_create_pool")
    assert success
    assert get_autoscaler("test_create_pool") is None
    
    pool.is_running = False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

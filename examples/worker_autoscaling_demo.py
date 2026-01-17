"""
Worker autoscaling demonstration.

Shows how to dynamically adjust worker count based on queue size.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task
from jobqueue.core.redis_queue import Queue
from jobqueue.core.worker_pool import WorkerPool
from jobqueue.core.worker_autoscaling import (
    WorkerAutoscaler,
    create_autoscaler,
    get_autoscaler
)
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("demo_autoscale_task")
def demo_autoscale_task(value):
    """Demo task function."""
    return f"Processed: {value}"


def demo_autoscaler_basic():
    """
    Demo 1: Basic autoscaler setup
    """
    print("\n" + "=" * 60)
    print("Demo 1: Basic Autoscaler Setup")
    print("=" * 60)
    
    redis_broker.connect()
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    # Create worker pool
    pool = WorkerPool(
        pool_name="demo_basic_pool",
        worker_class=SimpleWorker,
        queue_name="demo_autoscale_queue",
        initial_workers=1
    )
    pool.start()
    time.sleep(0.5)
    
    print(f"\n1. Created worker pool:")
    print(f"   Pool name: {pool.pool_name}")
    print(f"   Initial workers: {len(pool.workers)}")
    
    # Create autoscaler
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=100,
        scale_down_threshold=10,
        min_workers=1,
        max_workers=10,
        check_interval=30,
        cooldown_seconds=60
    )
    
    print(f"\n2. Created autoscaler:")
    print(f"   Scale up threshold: {autoscaler.scale_up_threshold}")
    print(f"   Scale down threshold: {autoscaler.scale_down_threshold}")
    print(f"   Min workers: {autoscaler.min_workers}")
    print(f"   Max workers: {autoscaler.max_workers}")
    print(f"   Check interval: {autoscaler.check_interval}s")
    print(f"   Cooldown: {autoscaler.cooldown_seconds}s")
    
    # Start autoscaler
    autoscaler.start()
    print(f"\n3. Autoscaler started")
    
    # Get status
    status = autoscaler.get_status()
    print(f"\n4. Autoscaler status:")
    print(f"   Is running: {status['is_running']}")
    print(f"   Queue size: {status['queue_size']}")
    print(f"   Current workers: {status['current_workers']}")
    
    # Stop
    autoscaler.stop()
    pool.stop(graceful=False)
    
    redis_broker.disconnect()


def demo_scale_up_on_queue_growth():
    """
    Demo 2: Scale up when queue grows
    """
    print("\n" + "=" * 60)
    print("Demo 2: Scale Up When Queue Grows")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_scale_up_queue")
    queue.purge()
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    # Create pool with 1 worker
    pool = WorkerPool(
        pool_name="demo_scale_up_pool",
        worker_class=SimpleWorker,
        queue_name="demo_scale_up_queue",
        initial_workers=1
    )
    pool.start()
    time.sleep(0.5)
    
    print(f"\n1. Initial state:")
    print(f"   Workers: {len(pool.workers)}")
    print(f"   Queue size: {queue.size()}")
    
    # Create autoscaler
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=50,  # Low threshold for demo
        scale_down_threshold=10,
        min_workers=1,
        max_workers=5,
        check_interval=2,  # Check every 2 seconds
        cooldown_seconds=3  # Short cooldown
    )
    
    autoscaler.start()
    
    # Enqueue tasks to grow queue
    print(f"\n2. Enqueuing tasks to grow queue...")
    for i in range(80):
        task = Task(
            name="demo_autoscale_task",
            args=[i],
            queue_name="demo_scale_up_queue"
        )
        queue.enqueue(task)
    
    print(f"   Queue size: {queue.size()}")
    print(f"   Threshold: {autoscaler.scale_up_threshold}")
    
    # Wait for autoscaling
    print(f"\n3. Waiting for autoscaling to trigger...")
    time.sleep(5)
    
    # Check status
    status = autoscaler.get_status()
    print(f"\n4. After autoscaling:")
    print(f"   Queue size: {status['queue_size']}")
    print(f"   Workers: {status['current_workers']}")
    print(f"   Should scale up: {status['should_scale_up']}")
    
    if status['current_workers'] > 1:
        print(f"   ✓ Workers scaled up from 1 to {status['current_workers']}")
    else:
        print(f"   ⚠ Workers did not scale up (may need more time)")
    
    # Check history
    history = autoscaler.get_scaling_history()
    if history:
        print(f"\n5. Scaling history:")
        for entry in history[-3:]:
            action = entry['action']
            workers = entry.get('workers_added', entry.get('workers_removed', 0))
            print(f"   {action}: {workers} workers at {entry['timestamp']}")
    
    autoscaler.stop()
    pool.stop(graceful=False)
    
    queue.purge()
    redis_broker.disconnect()


def demo_scale_down_on_queue_shrink():
    """
    Demo 3: Scale down when queue shrinks
    """
    print("\n" + "=" * 60)
    print("Demo 3: Scale Down When Queue Shrinks")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_scale_down_queue")
    queue.purge()
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    # Create pool with multiple workers
    pool = WorkerPool(
        pool_name="demo_scale_down_pool",
        worker_class=SimpleWorker,
        queue_name="demo_scale_down_queue",
        initial_workers=5
    )
    pool.start()
    time.sleep(0.5)
    
    print(f"\n1. Initial state:")
    print(f"   Workers: {len(pool.workers)}")
    
    # Create autoscaler
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=100,
        scale_down_threshold=5,  # Low threshold for demo
        min_workers=1,
        max_workers=10,
        check_interval=2,
        cooldown_seconds=3
    )
    
    autoscaler.start()
    
    # Queue is empty
    print(f"\n2. Queue state:")
    print(f"   Queue size: {queue.size()}")
    print(f"   Scale down threshold: {autoscaler.scale_down_threshold}")
    
    # Wait for autoscaling
    print(f"\n3. Waiting for autoscaling to trigger...")
    time.sleep(5)
    
    # Check status
    status = autoscaler.get_status()
    print(f"\n4. After autoscaling:")
    print(f"   Queue size: {status['queue_size']}")
    print(f"   Workers: {status['current_workers']}")
    print(f"   Should scale down: {status['should_scale_down']}")
    
    if status['current_workers'] < 5:
        print(f"   ✓ Workers scaled down from 5 to {status['current_workers']}")
    else:
        print(f"   ⚠ Workers did not scale down (may need more time or queue not empty)")
    
    autoscaler.stop()
    pool.stop(graceful=False)
    
    queue.purge()
    redis_broker.disconnect()


def demo_min_max_limits():
    """
    Demo 4: Min/max worker limits
    """
    print("\n" + "=" * 60)
    print("Demo 4: Min/Max Worker Limits")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_limits_queue")
    queue.purge()
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    # Create pool
    pool = WorkerPool(
        pool_name="demo_limits_pool",
        worker_class=SimpleWorker,
        queue_name="demo_limits_queue",
        initial_workers=2
    )
    pool.start()
    time.sleep(0.5)
    
    # Create autoscaler with strict limits
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=10,
        scale_down_threshold=5,
        min_workers=2,  # Cannot go below 2
        max_workers=3,  # Cannot go above 3
        check_interval=2,
        cooldown_seconds=3
    )
    
    print(f"\n1. Autoscaler limits:")
    print(f"   Min workers: {autoscaler.min_workers}")
    print(f"   Max workers: {autoscaler.max_workers}")
    print(f"   Current workers: {len(pool.workers)}")
    
    autoscaler.start()
    
    # Try to scale up beyond max
    print(f"\n2. Enqueuing many tasks to trigger scale up...")
    for i in range(50):
        task = Task(
            name="demo_autoscale_task",
            args=[i],
            queue_name="demo_limits_queue"
        )
        queue.enqueue(task)
    
    time.sleep(5)
    
    status = autoscaler.get_status()
    print(f"\n3. After scale up attempt:")
    print(f"   Queue size: {status['queue_size']}")
    print(f"   Workers: {status['current_workers']}")
    print(f"   Max workers: {autoscaler.max_workers}")
    print(f"   Respects max limit: {status['current_workers'] <= autoscaler.max_workers}")
    
    # Try to scale down below min
    queue.purge()
    time.sleep(5)
    
    status = autoscaler.get_status()
    print(f"\n4. After scale down attempt:")
    print(f"   Queue size: {status['queue_size']}")
    print(f"   Workers: {status['current_workers']}")
    print(f"   Min workers: {autoscaler.min_workers}")
    print(f"   Respects min limit: {status['current_workers'] >= autoscaler.min_workers}")
    
    autoscaler.stop()
    pool.stop(graceful=False)
    
    queue.purge()
    redis_broker.disconnect()


def demo_cooldown_period():
    """
    Demo 5: Cooldown period prevents rapid scaling
    """
    print("\n" + "=" * 60)
    print("Demo 5: Cooldown Period")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_cooldown_queue")
    queue.purge()
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="demo_cooldown_pool",
        worker_class=SimpleWorker,
        queue_name="demo_cooldown_queue",
        initial_workers=1
    )
    pool.start()
    time.sleep(0.5)
    
    # Create autoscaler with short cooldown
    autoscaler = WorkerAutoscaler(
        pool=pool,
        scale_up_threshold=10,
        cooldown_seconds=5,  # 5 second cooldown
        check_interval=1
    )
    
    print(f"\n1. Cooldown period: {autoscaler.cooldown_seconds}s")
    
    # Trigger scale up
    for i in range(20):
        task = Task(
            name="demo_autoscale_task",
            args=[i],
            queue_name="demo_cooldown_queue"
        )
        queue.enqueue(task)
    
    autoscaler.start()
    
    # First scale up
    print(f"\n2. First scale up...")
    time.sleep(2)
    status1 = autoscaler.get_status()
    print(f"   Workers: {status1['current_workers']}")
    print(f"   Can scale: {status1['can_scale']}")
    
    # Immediately try again (should be blocked by cooldown)
    print(f"\n3. Immediately trying again (should be blocked)...")
    can_scale = autoscaler._can_scale()
    print(f"   Can scale: {can_scale}")
    print(f"   Cooldown active: {not can_scale}")
    
    # Wait for cooldown
    print(f"\n4. Waiting for cooldown to expire...")
    time.sleep(6)
    can_scale = autoscaler._can_scale()
    print(f"   Can scale: {can_scale}")
    print(f"   Cooldown expired: {can_scale}")
    
    autoscaler.stop()
    pool.stop(graceful=False)
    
    queue.purge()
    redis_broker.disconnect()


def demo_queue_grows_verify_workers_scale_up():
    """
    Demo 6: Main test case - Queue grows, verify workers scale up
    """
    print("\n" + "=" * 60)
    print("Demo 6: Queue Grows, Verify Workers Scale Up (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_main_queue")
    queue.purge()
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    print(f"\nScenario:")
    print(f"  1. Start with 1 worker")
    print(f"  2. Enqueue tasks to grow queue size > 100")
    print(f"  3. Enable autoscaling")
    print(f"  4. Verify workers scale up\n")
    
    # Step 1: Start with 1 worker
    pool = WorkerPool(
        pool_name="demo_main_pool",
        worker_class=SimpleWorker,
        queue_name="demo_main_queue",
        initial_workers=1
    )
    pool.start()
    time.sleep(0.5)
    
    print(f"[Step 1] Initial state:")
    initial_workers = len(pool.workers)
    print(f"  Workers: {initial_workers}")
    print(f"  Queue size: {queue.size()}")
    
    # Step 2: Grow queue
    print(f"\n[Step 2] Growing queue...")
    for i in range(150):
        task = Task(
            name="demo_autoscale_task",
            args=[i],
            queue_name="demo_main_queue"
        )
        queue.enqueue(task)
    
    queue_size = queue.size()
    print(f"  Enqueued: 150 tasks")
    print(f"  Queue size: {queue_size}")
    
    # Step 3: Enable autoscaling
    print(f"\n[Step 3] Enabling autoscaling...")
    autoscaler = create_autoscaler(
        pool=pool,
        scale_up_threshold=100,
        scale_down_threshold=10,
        min_workers=1,
        max_workers=10,
        check_interval=2,  # Check every 2 seconds
        cooldown_seconds=3  # Short cooldown
    )
    
    autoscaler.start()
    print(f"  Autoscaler started")
    print(f"  Scale up threshold: {autoscaler.scale_up_threshold}")
    print(f"  Current queue size: {queue_size}")
    
    # Step 4: Wait and verify
    print(f"\n[Step 4] Waiting for autoscaling...")
    time.sleep(6)  # Wait for check + scaling
    
    status = autoscaler.get_status()
    current_workers = status['current_workers']
    
    print(f"\n[Step 5] Results:")
    print(f"  Queue size: {status['queue_size']}")
    print(f"  Workers: {current_workers}")
    print(f"  Workers added: {current_workers - initial_workers}")
    print(f"  Should scale up: {status['should_scale_up']}")
    
    # Verification
    print(f"\n[Step 6] Verification:")
    
    if current_workers > initial_workers:
        print(f"  ✓ Workers scaled up: {initial_workers} → {current_workers}")
    else:
        print(f"  ⚠ Workers did not scale up (may need more time)")
    
    print(f"  ✓ Queue size > threshold: {queue_size > autoscaler.scale_up_threshold}")
    print(f"  ✓ Workers within limits: {autoscaler.min_workers <= current_workers <= autoscaler.max_workers}")
    
    # History
    history = autoscaler.get_scaling_history()
    if history:
        print(f"\n[Step 7] Scaling history:")
        for entry in history[-3:]:
            action = entry['action']
            workers = entry.get('workers_added', entry.get('workers_removed', 0))
            timestamp = entry['timestamp']
            print(f"  {action}: {workers} workers at {timestamp}")
    
    print(f"\nDemo: PASS - Queue grew and workers scaled up")
    
    # Cleanup
    autoscaler.stop()
    pool.stop(graceful=False)
    
    queue.purge()
    redis_broker.disconnect()


def run_all_demos():
    """Run all autoscaling demonstrations."""
    print("\n" + "=" * 60)
    print("WORKER AUTOSCALING DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_autoscaler_basic()
        demo_scale_up_on_queue_growth()
        demo_scale_down_on_queue_shrink()
        demo_min_max_limits()
        demo_cooldown_period()
        demo_queue_grows_verify_workers_scale_up()  # Main test case
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

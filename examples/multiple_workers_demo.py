"""
Multiple workers demonstration.

Shows how to run multiple workers safely with no duplicate processing.
"""
import time
import threading
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.redis_queue import Queue
from jobqueue.core.worker_pool import WorkerPool, distributed_worker_manager
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("demo_multi_worker_task")
def demo_multi_worker_task(value):
    """Demo task function."""
    return f"Processed: {value}"


def demo_unique_worker_ids():
    """
    Demo 1: Unique worker IDs
    """
    print("\n" + "=" * 60)
    print("Demo 1: Unique Worker IDs (Hostname + PID)")
    print("=" * 60)
    
    from jobqueue.worker.base_worker import Worker
    
    worker1 = Worker()
    worker2 = Worker()
    
    print(f"\nWorker 1:")
    print(f"  Worker ID: {worker1.worker_id}")
    print(f"  Hostname: {worker1.hostname}")
    print(f"  PID: {worker1.pid}")
    
    print(f"\nWorker 2:")
    print(f"  Worker ID: {worker2.worker_id}")
    print(f"  Hostname: {worker2.hostname}")
    print(f"  PID: {worker2.pid}")
    
    print(f"\nFormat: worker-{worker1.hostname}-{worker1.pid}")
    print(f"Unique: {worker1.worker_id != worker2.worker_id}")


def demo_brpop_atomicity():
    """
    Demo 2: BRPOP atomicity
    """
    print("\n" + "=" * 60)
    print("Demo 2: BRPOP Atomicity (Redis Handles Race Conditions)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_atomic_queue")
    queue.purge()
    
    # Enqueue a single task
    task = Task(
        name="demo_multi_worker_task",
        args=[1],
        queue_name="demo_atomic_queue"
    )
    queue.enqueue(task)
    
    print(f"\n1. Enqueued 1 task: {task.id}")
    print(f"   Queue size: {queue.size()}")
    
    # Multiple workers try to dequeue
    results = []
    lock = threading.Lock()
    
    def worker_dequeue():
        dequeued = queue.dequeue(timeout=1)
        if dequeued:
            with lock:
                results.append(dequeued.id)
    
    print(f"\n2. Starting 5 workers to compete for the task...")
    threads = [threading.Thread(target=worker_dequeue) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    print(f"\n3. Results:")
    print(f"   Tasks dequeued: {len(results)}")
    print(f"   Task IDs: {results}")
    print(f"   Only one worker got the task: {len(results) == 1}")
    print(f"   Redis BRPOP is atomic: {len(results) == 1}")
    
    queue.purge()
    redis_broker.disconnect()


def demo_worker_pool():
    """
    Demo 3: Worker pool management
    """
    print("\n" + "=" * 60)
    print("Demo 3: Worker Pool Management")
    print("=" * 60)
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="demo_pool",
        worker_class=SimpleWorker,
        queue_name="demo_queue",
        initial_workers=3
    )
    
    print(f"\n1. Created worker pool:")
    print(f"   Pool name: {pool.pool_name}")
    print(f"   Queue name: {pool.queue_name}")
    print(f"   Initial workers: {pool.initial_workers}")
    
    print(f"\n2. Starting pool...")
    pool.start()
    time.sleep(1)  # Give workers time to start
    
    status = pool.get_pool_status()
    print(f"   Total workers: {status['total_workers']}")
    print(f"   Alive workers: {status['alive_workers']}")
    
    print(f"\n3. Scaling up by 2 workers...")
    new_workers = pool.scale_up(2)
    print(f"   Added workers: {len(new_workers)}")
    print(f"   New worker IDs: {new_workers[:2]}")
    
    status = pool.get_pool_status()
    print(f"   Total workers: {status['total_workers']}")
    
    print(f"\n4. Scaling down by 1 worker...")
    removed = pool.scale_down(1)
    print(f"   Removed workers: {len(removed)}")
    
    status = pool.get_pool_status()
    print(f"   Total workers: {status['total_workers']}")
    
    print(f"\n5. Stopping pool...")
    pool.stop(graceful=True)
    print(f"   Pool stopped")


def demo_worker_scaling():
    """
    Demo 4: Worker scaling
    """
    print("\n" + "=" * 60)
    print("Demo 4: Worker Scaling (Up/Down)")
    print("=" * 60)
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    pool = WorkerPool(
        pool_name="demo_scale_pool",
        worker_class=SimpleWorker,
        queue_name="demo_queue",
        initial_workers=2
    )
    
    pool.start()
    time.sleep(0.5)
    
    print(f"\n1. Initial state:")
    status = pool.get_pool_status()
    print(f"   Workers: {status['total_workers']}")
    
    print(f"\n2. Scale up by 3:")
    pool.scale_up(3)
    status = pool.get_pool_status()
    print(f"   Workers: {status['total_workers']}")
    
    print(f"\n3. Scale down by 2:")
    pool.scale_down(2)
    status = pool.get_pool_status()
    print(f"   Workers: {status['total_workers']}")
    
    pool.stop(graceful=False)


def demo_ten_workers_thousand_tasks_verify_no_duplicates():
    """
    Demo 5: 10 workers, 1000 tasks, verify no duplicates (Main test case)
    """
    print("\n" + "=" * 60)
    print("Demo 5: 10 Workers, 1000 Tasks, Verify No Duplicates (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_multi_queue")
    queue.purge()
    
    print(f"\nScenario:")
    print(f"  1. Enqueue 1000 tasks")
    print(f"  2. Start 10 workers")
    print(f"  3. Workers compete for tasks using BRPOP (atomic)")
    print(f"  4. Verify no task processed twice\n")
    
    # Step 1: Enqueue 1000 tasks
    print(f"[Step 1] Enqueuing 1000 tasks...")
    task_ids = []
    start_time = time.time()
    
    for i in range(1000):
        task = Task(
            name="demo_multi_worker_task",
            args=[i],
            queue_name="demo_multi_queue"
        )
        queue.enqueue(task)
        task_ids.append(task.id)
    
    enqueue_time = time.time() - start_time
    print(f"  Enqueued: {len(task_ids)} tasks in {enqueue_time:.2f}s")
    print(f"  Queue size: {queue.size()}")
    
    # Step 2: Simulate 10 workers
    print(f"\n[Step 2] Starting 10 workers...")
    processed_tasks = []
    processed_lock = threading.Lock()
    worker_stats = {}
    
    def worker_process(worker_id):
        """Worker function."""
        worker_processed = []
        start = time.time()
        
        while True:
            task = queue.dequeue(timeout=1)
            if task is None:
                break
            
            with processed_lock:
                processed_tasks.append(task.id)
                worker_processed.append(task.id)
            
            # Simulate processing
            time.sleep(0.001)
        
        duration = time.time() - start
        worker_stats[worker_id] = {
            "processed": len(worker_processed),
            "duration": duration
        }
    
    # Start 10 worker threads
    worker_threads = []
    for i in range(10):
        thread = threading.Thread(
            target=worker_process,
            args=(f"worker-{i}",),
            name=f"worker-{i}"
        )
        worker_threads.append(thread)
        thread.start()
    
    # Wait for all workers
    print(f"  Workers processing tasks...")
    process_start = time.time()
    
    for thread in worker_threads:
        thread.join(timeout=60)
    
    process_time = time.time() - process_start
    
    # Step 3: Verify no duplicates
    print(f"\n[Step 3] Verifying no duplicates...")
    unique_processed = set(processed_tasks)
    duplicates = len(processed_tasks) - len(unique_processed)
    
    print(f"  Tasks enqueued: {len(task_ids)}")
    print(f"  Tasks processed: {len(processed_tasks)}")
    print(f"  Unique tasks: {len(unique_processed)}")
    print(f"  Duplicates: {duplicates}")
    print(f"  Processing time: {process_time:.2f}s")
    print(f"  Throughput: {len(processed_tasks) / process_time:.2f} tasks/sec")
    
    # Step 4: Worker statistics
    print(f"\n[Step 4] Worker Statistics:")
    for worker_id, stats in worker_stats.items():
        print(f"  {worker_id}: {stats['processed']} tasks in {stats['duration']:.2f}s")
    
    # Verification
    print(f"\n[Step 5] Verification:")
    
    assert len(processed_tasks) == len(task_ids), \
        f"Expected {len(task_ids)} tasks, got {len(processed_tasks)}"
    print(f"  ✓ All tasks processed")
    
    assert duplicates == 0, f"Found {duplicates} duplicates"
    print(f"  ✓ No duplicates")
    
    assert len(unique_processed) == len(task_ids), \
        f"Expected {len(task_ids)} unique tasks, got {len(unique_processed)}"
    print(f"  ✓ All unique")
    
    missing = set(task_ids) - unique_processed
    assert len(missing) == 0, f"Missing {len(missing)} tasks"
    print(f"  ✓ All tasks accounted for")
    
    print(f"\nTest: PASS - 10 workers processed 1000 tasks with no duplicates")
    print(f"  Redis BRPOP atomicity ensures no duplicate processing")
    
    queue.purge()
    redis_broker.disconnect()


def demo_distributed_worker_manager():
    """
    Demo 6: Distributed worker manager
    """
    print("\n" + "=" * 60)
    print("Demo 6: Distributed Worker Manager")
    print("=" * 60)
    
    from jobqueue.worker.simple_worker import SimpleWorker
    
    # Create multiple pools
    print(f"\n1. Creating worker pools...")
    pool1 = distributed_worker_manager.create_pool(
        pool_name="pool1",
        worker_class=SimpleWorker,
        queue_name="queue1",
        initial_workers=2
    )
    
    pool2 = distributed_worker_manager.create_pool(
        pool_name="pool2",
        worker_class=SimpleWorker,
        queue_name="queue2",
        initial_workers=1
    )
    
    print(f"   Created pool1: {pool1.pool_name}")
    print(f"   Created pool2: {pool2.pool_name}")
    
    # Get all pools status
    print(f"\n2. Getting all pools status...")
    status = distributed_worker_manager.get_all_pools_status()
    print(f"   Total pools: {status['total_pools']}")
    print(f"   Pools: {list(status['pools'].keys())}")
    
    # Cleanup
    distributed_worker_manager.remove_pool("pool1", graceful=False)
    distributed_worker_manager.remove_pool("pool2", graceful=False)


def demo_worker_competition():
    """
    Demo 7: Worker competition for tasks
    """
    print("\n" + "=" * 60)
    print("Demo 7: Worker Competition for Tasks")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_competition_queue")
    queue.purge()
    
    # Enqueue tasks
    task_count = 50
    print(f"\n1. Enqueuing {task_count} tasks...")
    for i in range(task_count):
        task = Task(
            name="demo_multi_worker_task",
            args=[i],
            queue_name="demo_competition_queue"
        )
        queue.enqueue(task)
    
    print(f"   Queue size: {queue.size()}")
    
    # Multiple workers compete
    print(f"\n2. Starting 5 workers to compete...")
    processed = []
    lock = threading.Lock()
    
    def worker(worker_id):
        count = 0
        while True:
            task = queue.dequeue(timeout=0.5)
            if task is None:
                break
            with lock:
                processed.append(task.id)
            count += 1
        print(f"   {worker_id}: processed {count} tasks")
    
    threads = [threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    print(f"\n3. Results:")
    print(f"   Tasks processed: {len(processed)}")
    print(f"   Unique tasks: {len(set(processed))}")
    print(f"   Duplicates: {len(processed) - len(set(processed))}")
    print(f"   All tasks processed: {len(processed) == task_count}")
    print(f"   No duplicates: {len(processed) == len(set(processed))}")
    
    queue.purge()
    redis_broker.disconnect()


def run_all_demos():
    """Run all multiple workers demonstrations."""
    print("\n" + "=" * 60)
    print("MULTIPLE WORKERS DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_unique_worker_ids()
        demo_brpop_atomicity()
        demo_worker_pool()
        demo_worker_scaling()
        demo_ten_workers_thousand_tasks_verify_no_duplicates()  # Main test case
        demo_distributed_worker_manager()
        demo_worker_competition()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

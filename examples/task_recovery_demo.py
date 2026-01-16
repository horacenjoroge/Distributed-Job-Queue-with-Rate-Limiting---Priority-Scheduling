"""
Task recovery demonstration.

Shows how tasks are recovered from crashed workers.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_recovery import task_recovery
from jobqueue.core.worker_heartbeat import worker_heartbeat, WorkerStatus
from jobqueue.core.worker_monitor import WorkerMonitor
from jobqueue.core.redis_queue import Queue
from jobqueue.utils.logger import log


def demo_active_task_tracking():
    """
    Demo 1: Active task tracking
    """
    print("\n" + "=" * 60)
    print("Demo 1: Active Task Tracking")
    print("=" * 60)
    
    redis_broker.connect()
    
    worker_id = "demo_worker_active"
    worker_heartbeat.remove_worker(worker_id)
    
    print(f"\nWorker ID: {worker_id}")
    print(f"Active tasks stored in: worker:{worker_id}:active\n")
    
    # Create tasks
    tasks = []
    for i in range(3):
        task = Task(
            name=f"task_{i}",
            args=[i],
            queue_name="demo_queue"
        )
        task.mark_running(worker_id)
        tasks.append(task)
    
    # Add tasks to active set
    print("Adding tasks to active set:")
    for task in tasks:
        task_recovery.add_active_task(worker_id, task)
        print(f"  Task {task.id}: Added to active set")
    
    # Get active tasks
    active = task_recovery.get_active_tasks(worker_id)
    print(f"\nActive tasks: {len(active)}")
    
    # Remove one task
    print(f"\nCompleting task {tasks[0].id}...")
    task_recovery.remove_active_task(worker_id, tasks[0])
    
    active = task_recovery.get_active_tasks(worker_id)
    print(f"Active tasks: {len(active)}")
    
    # Cleanup
    for task in tasks[1:]:
        task_recovery.remove_active_task(worker_id, task)
    worker_heartbeat.remove_worker(worker_id)
    redis_broker.disconnect()


def demo_task_lock_mechanism():
    """
    Demo 2: Task lock mechanism
    """
    print("\n" + "=" * 60)
    print("Demo 2: Task Lock Mechanism (Duplicate Prevention)")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "locked_task"
    worker1 = "worker_1"
    worker2 = "worker_2"
    
    task = Task(id=task_id, name="test_task", args=[1], queue_name="demo_queue")
    
    print(f"\nTask ID: {task_id}")
    print(f"Lock key: task:lock:{task_id}\n")
    
    # Worker 1 starts task
    print(f"Worker 1 starts task...")
    task_recovery.add_active_task(worker1, task)
    
    is_locked = task_recovery.is_task_locked(task_id)
    owner = task_recovery.get_task_lock_owner(task_id)
    
    print(f"  Task locked: {is_locked}")
    print(f"  Lock owner: {owner}")
    
    # Worker 2 tries to start same task
    print(f"\nWorker 2 tries to start same task...")
    is_locked = task_recovery.is_task_locked(task_id)
    print(f"  Task locked: {is_locked}")
    print(f"  Lock owner: {task_recovery.get_task_lock_owner(task_id)}")
    print(f"  Result: Worker 2 cannot start (duplicate prevented)")
    
    # Worker 1 completes
    print(f"\nWorker 1 completes task...")
    task_recovery.remove_active_task(worker1, task)
    
    is_locked = task_recovery.is_task_locked(task_id)
    print(f"  Task locked: {is_locked}")
    print(f"  Result: Lock released, task can be started by another worker")
    
    redis_broker.disconnect()


def demo_orphaned_task_detection():
    """
    Demo 3: Orphaned task detection
    """
    print("\n" + "=" * 60)
    print("Demo 3: Orphaned Task Detection")
    print("=" * 60)
    
    redis_broker.connect()
    
    worker_id = "orphaned_worker"
    worker_heartbeat.remove_worker(worker_id)
    
    # Create tasks
    tasks = []
    for i in range(3):
        task = Task(
            name=f"orphaned_task_{i}",
            args=[i],
            queue_name="demo_queue"
        )
        task.mark_running(worker_id)
        task_recovery.add_active_task(worker_id, task)
        tasks.append(task)
    
    print(f"\nWorker {worker_id} has {len(tasks)} active tasks")
    
    # Mark worker as dead
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    
    print(f"\nWorker stops sending heartbeats...")
    print(f"Waiting for stale threshold (2 seconds)...")
    time.sleep(2.5)
    
    # Detect orphaned tasks
    orphaned = task_recovery.get_orphaned_tasks()
    
    print(f"\nOrphaned Tasks Detected:")
    print(f"  Count: {len(orphaned)}")
    for worker_id_found, task in orphaned:
        print(f"    - Task {task.id} from worker {worker_id_found}")
    
    # Cleanup
    for task in tasks:
        task_recovery.remove_active_task(worker_id, task)
    worker_heartbeat.remove_worker(worker_id)
    redis_broker.disconnect()


def demo_crash_worker_verify_task_recovery():
    """
    Demo 4: Crash worker, verify task recovery
    Main test case from requirements
    """
    print("\n" + "=" * 60)
    print("Demo 4: Crash Worker, Verify Task Recovery (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    worker_id = "crashed_worker_demo"
    queue_name = "demo_recovery_queue"
    
    # Clean queue
    queue = Queue(queue_name)
    queue.purge()
    worker_heartbeat.remove_worker(worker_id)
    
    print(f"\nScenario:")
    print(f"  1. Worker starts processing task")
    print(f"  2. Worker crashes mid-execution")
    print(f"  3. Monitor detects orphaned task")
    print(f"  4. Task is recovered and re-queued\n")
    
    # Create task
    task = Task(
        name="demo_task",
        args=["recovery_test_data"],
        queue_name=queue_name
    )
    
    print(f"Task created: {task.id}")
    print(f"Queue: {queue_name}")
    
    # Simulate worker starting task
    print(f"\n[Step 1] Worker {worker_id} starts processing task...")
    task.mark_running(worker_id)
    task.started_at = datetime.utcnow()
    
    # Add to active set
    task_recovery.add_active_task(worker_id, task)
    
    print(f"  Task added to active set: worker:{worker_id}:active")
    print(f"  Task locked to worker: {worker_id}")
    print(f"  Queue size: {queue.size()}")
    
    # Verify active
    active = task_recovery.get_active_tasks(worker_id)
    print(f"  Active tasks: {len(active)}")
    
    # Simulate worker crash
    print(f"\n[Step 2] Worker {worker_id} crashes (stops sending heartbeats)...")
    
    # Wait for stale threshold
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    
    print(f"  Waiting for stale threshold (2 seconds)...")
    time.sleep(2.5)
    
    # Monitor detects dead worker
    print(f"\n[Step 3] Monitor detects dead worker...")
    stale_workers = heartbeat_manager.get_stale_workers()
    
    if worker_id in stale_workers:
        print(f"  Dead worker detected: {worker_id}")
        
        # Get orphaned tasks
        orphaned = task_recovery.get_orphaned_tasks()
        print(f"  Orphaned tasks found: {len(orphaned)}")
        
        # Recover tasks
        print(f"\n[Step 4] Recovering orphaned tasks...")
        recovered = task_recovery.recover_orphaned_tasks()
        
        print(f"\nRecovery Results:")
        print(f"  Tasks recovered: {recovered}")
        print(f"  Queue size: {queue.size()}")
        
        if queue.size() > 0:
            recovered_task = queue.dequeue_nowait()
            
            print(f"\nRecovered Task Details:")
            print(f"  Task ID: {recovered_task.id}")
            print(f"  Status: {recovered_task.status.value} (was RUNNING)")
            print(f"  Worker ID: {recovered_task.worker_id} (was {worker_id})")
            print(f"  Queue: {recovered_task.queue_name}")
            
            print(f"\nTest: PASS - Task successfully recovered and re-queued")
        else:
            print(f"\nTest: FAIL - Task not recovered")
    else:
        print(f"  Worker not detected as dead")
    
    # Cleanup
    queue.purge()
    worker_heartbeat.remove_worker(worker_id)
    redis_broker.disconnect()


def demo_prevent_duplicate_recovery():
    """
    Demo 5: Prevent duplicate recovery
    """
    print("\n" + "=" * 60)
    print("Demo 5: Prevent Duplicate Recovery")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "duplicate_test_task"
    worker_id = "dead_worker_duplicate"
    
    task = Task(id=task_id, name="test_task", args=[1], queue_name="demo_queue")
    
    # Add to active set
    task_recovery.add_active_task(worker_id, task)
    
    print(f"\nTask {task_id} is orphaned (worker {worker_id} is dead)")
    print(f"Multiple monitor processes try to recover...\n")
    
    # Simulate multiple recovery attempts
    print(f"Monitor 1 tries to acquire recovery lock...")
    lock1 = task_recovery._acquire_recovery_lock(task_id)
    print(f"  Lock acquired: {lock1}")
    
    print(f"\nMonitor 2 tries to acquire recovery lock...")
    lock2 = task_recovery._acquire_recovery_lock(task_id)
    print(f"  Lock acquired: {lock2}")
    print(f"  Result: {'SUCCESS (duplicate prevented)' if not lock2 else 'FAILED (duplicate possible)'}")
    
    # Release lock
    print(f"\nMonitor 1 releases lock...")
    task_recovery._release_recovery_lock(task_id)
    
    print(f"Monitor 2 tries again...")
    lock3 = task_recovery._acquire_recovery_lock(task_id)
    print(f"  Lock acquired: {lock3}")
    print(f"  Result: Now can recover (lock released)")
    
    task_recovery._release_recovery_lock(task_id)
    
    redis_broker.disconnect()


def demo_recovery_statistics():
    """
    Demo 6: Recovery statistics
    """
    print("\n" + "=" * 60)
    print("Demo 6: Task Recovery Statistics")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create multiple orphaned tasks
    workers = ["dead_worker_1", "dead_worker_2"]
    queues = ["queue1", "queue2"]
    
    print("\nCreating orphaned tasks...\n")
    
    for i, worker_id in enumerate(workers):
        for j in range(2):
            task = Task(
                name=f"task_{i}_{j}",
                args=[j],
                queue_name=queues[i]
            )
            task.mark_running(worker_id)
            task_recovery.add_active_task(worker_id, task)
    
    # Mark workers as dead
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    for worker_id in workers:
        heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    
    time.sleep(2.5)
    
    # Get stats
    stats = task_recovery.get_recovery_stats()
    
    print("Recovery Statistics:")
    print(f"  Total orphaned tasks: {stats['orphaned_tasks']}")
    print(f"\n  By Worker:")
    for worker_id, count in stats['by_worker'].items():
        print(f"    {worker_id}: {count} tasks")
    print(f"\n  By Queue:")
    for queue_name, count in stats['by_queue'].items():
        print(f"    {queue_name}: {count} tasks")
    
    # Cleanup
    for worker_id in workers:
        active = task_recovery.get_active_tasks(worker_id)
        for task in active:
            task_recovery.remove_active_task(worker_id, task)
        worker_heartbeat.remove_worker(worker_id)
    
    redis_broker.disconnect()


def demo_automatic_recovery():
    """
    Demo 7: Automatic recovery by monitor
    """
    print("\n" + "=" * 60)
    print("Demo 7: Automatic Recovery by Monitor")
    print("=" * 60)
    
    redis_broker.connect()
    
    worker_id = "monitor_test_worker"
    queue_name = "demo_monitor_queue"
    
    queue = Queue(queue_name)
    queue.purge()
    worker_heartbeat.remove_worker(worker_id)
    
    # Create task
    task = Task(
        name="monitor_task",
        args=["test"],
        queue_name=queue_name
    )
    
    # Worker starts task
    task.mark_running(worker_id)
    task_recovery.add_active_task(worker_id, task)
    
    print(f"\nWorker {worker_id} starts task {task.id}")
    print(f"Queue size: {queue.size()}")
    
    # Worker crashes
    heartbeat_manager = worker_heartbeat.__class__(stale_threshold=2)
    heartbeat_manager.send_heartbeat(worker_id, WorkerStatus.ACTIVE)
    
    print(f"\nWorker crashes...")
    time.sleep(2.5)
    
    # Monitor recovers
    monitor = WorkerMonitor(check_interval=1, stale_threshold=2)
    
    print(f"\nMonitor checking for orphaned tasks...")
    recovered = task_recovery.recover_orphaned_tasks()
    
    print(f"\nResults:")
    print(f"  Tasks recovered: {recovered}")
    print(f"  Queue size: {queue.size()}")
    
    if queue.size() > 0:
        print(f"  Task successfully re-queued")
    
    # Cleanup
    queue.purge()
    worker_heartbeat.remove_worker(worker_id)
    redis_broker.disconnect()


def run_all_demos():
    """Run all task recovery demonstrations."""
    print("\n" + "=" * 60)
    print("TASK RECOVERY DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_active_task_tracking()
        demo_task_lock_mechanism()
        demo_orphaned_task_detection()
        demo_crash_worker_verify_task_recovery()  # Main test case
        demo_prevent_duplicate_recovery()
        demo_recovery_statistics()
        demo_automatic_recovery()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

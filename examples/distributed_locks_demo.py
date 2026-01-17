"""
Distributed locks demonstration.

Shows how distributed locks prevent concurrent execution of same task.
"""
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
from jobqueue.utils.logger import log


@task_registry.register("demo_lock_task")
def demo_lock_task(value):
    """Demo task function."""
    time.sleep(0.5)  # Simulate work
    return f"Processed: {value}"


def demo_basic_lock():
    """
    Demo 1: Basic lock acquisition and release
    """
    print("\n" + "=" * 60)
    print("Demo 1: Basic Lock Acquisition and Release")
    print("=" * 60)
    
    redis_broker.connect()
    
    lock = DistributedLock("lock:demo", ttl=60)
    
    print(f"\n1. Acquiring lock...")
    acquired = lock.acquire()
    print(f"   Lock acquired: {acquired}")
    print(f"   Is acquired: {lock.is_acquired}")
    print(f"   Is locked: {lock.is_locked()}")
    
    print(f"\n2. Releasing lock...")
    released = lock.release()
    print(f"   Lock released: {released}")
    print(f"   Is acquired: {lock.is_acquired}")
    print(f"   Is locked: {lock.is_locked()}")
    
    redis_broker.disconnect()


def demo_lock_already_held():
    """
    Demo 2: Lock already held
    """
    print("\n" + "=" * 60)
    print("Demo 2: Lock Already Held")
    print("=" * 60)
    
    redis_broker.connect()
    
    lock1 = DistributedLock("lock:demo", ttl=60)
    lock2 = DistributedLock("lock:demo", ttl=60)
    
    print(f"\n1. First lock acquires...")
    acquired1 = lock1.acquire()
    print(f"   Lock 1 acquired: {acquired1}")
    
    print(f"\n2. Second lock tries to acquire (no timeout)...")
    acquired2 = lock2.acquire(timeout=0)
    print(f"   Lock 2 acquired: {acquired2}")
    print(f"   Lock 2 cannot acquire (lock already held)")
    
    print(f"\n3. Release first lock...")
    lock1.release()
    
    print(f"\n4. Second lock tries again...")
    acquired2 = lock2.acquire()
    print(f"   Lock 2 acquired: {acquired2}")
    
    lock2.release()
    
    redis_broker.disconnect()


def demo_lock_ttl():
    """
    Demo 3: Lock TTL prevents deadlocks
    """
    print("\n" + "=" * 60)
    print("Demo 3: Lock TTL Prevents Deadlocks")
    print("=" * 60)
    
    redis_broker.connect()
    
    lock = DistributedLock("lock:demo", ttl=3)  # 3 second TTL
    
    print(f"\n1. Acquiring lock with 3s TTL...")
    lock.acquire()
    print(f"   Lock acquired")
    print(f"   Remaining TTL: {lock.get_remaining_ttl()}s")
    
    print(f"\n2. Simulating worker death (lock not released)...")
    print(f"   Waiting for TTL expiration...")
    time.sleep(4)
    
    print(f"\n3. Checking lock status...")
    is_locked = lock.is_locked()
    print(f"   Is locked: {is_locked}")
    print(f"   Lock expired automatically (TTL)")
    
    print(f"\n4. New lock can be acquired...")
    lock2 = DistributedLock("lock:demo", ttl=60)
    acquired = lock2.acquire()
    print(f"   New lock acquired: {acquired}")
    
    lock2.release()
    
    redis_broker.disconnect()


def demo_lock_renewal():
    """
    Demo 4: Lock renewal for long tasks
    """
    print("\n" + "=" * 60)
    print("Demo 4: Lock Renewal for Long Tasks")
    print("=" * 60)
    
    redis_broker.connect()
    
    lock = DistributedLock("lock:demo", ttl=5)
    
    print(f"\n1. Acquiring lock with 5s TTL...")
    lock.acquire()
    print(f"   Initial TTL: {lock.get_remaining_ttl()}s")
    
    print(f"\n2. Simulating long-running task...")
    for i in range(3):
        time.sleep(1)
        print(f"   After {i+1}s: TTL = {lock.get_remaining_ttl()}s")
        
        # Renew lock
        if lock.get_remaining_ttl() < 3:
            renewed = lock.renew(additional_ttl=5)
            print(f"   Lock renewed: {renewed}, new TTL: {lock.get_remaining_ttl()}s")
    
    print(f"\n3. Lock still held after renewal...")
    print(f"   Is locked: {lock.is_locked()}")
    print(f"   Remaining TTL: {lock.get_remaining_ttl()}s")
    
    lock.release()
    
    redis_broker.disconnect()


def demo_same_task_twice_second_waits_for_first():
    """
    Demo 5: Main test case - Same task twice → second waits for first
    """
    print("\n" + "=" * 60)
    print("Demo 5: Same Task Twice → Second Waits for First (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_lock_queue")
    queue.purge()
    
    print(f"\nScenario:")
    print(f"  1. Enqueue same task twice")
    print(f"  2. First task acquires lock and executes")
    print(f"  3. Second task waits for lock")
    print(f"  4. First task completes and releases lock")
    print(f"  5. Second task acquires lock and executes\n")
    
    # Step 1: Create tasks
    print(f"[Step 1] Creating tasks...")
    task1 = Task(
        name="demo_lock_task",
        args=[1],
        queue_name="demo_lock_queue"
    )
    task1.compute_and_set_signature()
    
    task2 = Task(
        name="demo_lock_task",
        args=[1],  # Same args = same signature
        queue_name="demo_lock_queue"
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
        """Worker function."""
        # Acquire lock
        lock = task_lock_manager.acquire_task_lock(
            task_signature=task.task_signature,
            ttl=60,
            timeout=10,
            retry_interval=0.1
        )
        
        if lock:
            with execution_lock:
                execution_order.append(f"Task {task_num} started")
                print(f"  Task {task_num} started")
            
            # Simulate work
            time.sleep(0.5)
            
            with execution_lock:
                execution_order.append(f"Task {task_num} completed")
                print(f"  Task {task_num} completed")
            
            # Release lock
            lock.release()
        else:
            with execution_lock:
                execution_order.append(f"Task {task_num} failed to acquire lock")
                print(f"  Task {task_num} failed to acquire lock")
    
    # Start both workers
    thread1 = threading.Thread(target=worker_process, args=(1, task1))
    thread2 = threading.Thread(target=worker_process, args=(2, task2))
    
    print(f"\n[Step 4] Starting workers...")
    thread1.start()
    time.sleep(0.1)  # Small delay
    thread2.start()
    
    # Wait for both threads
    thread1.join(timeout=5)
    thread2.join(timeout=5)
    
    # Step 5: Verify execution order
    print(f"\n[Step 5] Execution order:")
    for i, event in enumerate(execution_order, 1):
        print(f"  {i}. {event}")
    
    # Verification
    print(f"\n[Step 6] Verification:")
    assert len(execution_order) == 4
    assert execution_order[0] == "Task 1 started"
    assert execution_order[1] == "Task 1 completed"
    assert execution_order[2] == "Task 2 started"
    assert execution_order[3] == "Task 2 completed"
    
    print(f"  ✓ Task 1 started first")
    print(f"  ✓ Task 1 completed before Task 2 started")
    print(f"  ✓ Task 2 started after Task 1 completed")
    print(f"  ✓ Task 2 completed")
    
    print(f"\nDemo: PASS - Same task twice, second waited for first")
    
    queue.purge()
    redis_broker.disconnect()


def demo_lock_context_manager():
    """
    Demo 6: Lock as context manager
    """
    print("\n" + "=" * 60)
    print("Demo 6: Lock as Context Manager")
    print("=" * 60)
    
    redis_broker.connect()
    
    print(f"\n1. Using lock as context manager...")
    with DistributedLock("lock:demo", ttl=60) as lock:
        print(f"   Lock acquired: {lock.is_acquired}")
        print(f"   Is locked: {lock.is_locked()}")
        # Do work here
        time.sleep(0.1)
    
    print(f"\n2. After context exit...")
    print(f"   Lock released automatically")
    print(f"   Is locked: {lock.is_locked()}")
    
    redis_broker.disconnect()


def run_all_demos():
    """Run all distributed locks demonstrations."""
    print("\n" + "=" * 60)
    print("DISTRIBUTED LOCKS DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_basic_lock()
        demo_lock_already_held()
        demo_lock_ttl()
        demo_lock_renewal()
        demo_same_task_twice_second_waits_for_first()  # Main test case
        demo_lock_context_manager()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

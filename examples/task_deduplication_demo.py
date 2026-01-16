"""
Task deduplication demonstration.

Shows how to prevent duplicate task execution.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_deduplication import task_deduplication
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("demo_dedup_task")
def demo_dedup_task(value):
    """Demo task function."""
    return f"Processed: {value}"


def demo_task_signature():
    """
    Demo 1: Task signature generation
    """
    print("\n" + "=" * 60)
    print("Demo 1: Task Signature Generation")
    print("=" * 60)
    
    # Create tasks with same signature
    task1 = Task(name="demo_dedup_task", args=[1, 2], kwargs={"key": "value"})
    task1.compute_and_set_signature()
    
    task2 = Task(name="demo_dedup_task", args=[1, 2], kwargs={"key": "value"})
    task2.compute_and_set_signature()
    
    print(f"\nTask 1:")
    print(f"  Name: {task1.name}")
    print(f"  Args: {task1.args}")
    print(f"  Kwargs: {task1.kwargs}")
    print(f"  Signature: {task1.task_signature[:32]}...")
    
    print(f"\nTask 2:")
    print(f"  Name: {task2.name}")
    print(f"  Args: {task2.args}")
    print(f"  Kwargs: {task2.kwargs}")
    print(f"  Signature: {task2.task_signature[:32]}...")
    
    print(f"\nSignatures match: {task1.task_signature == task2.task_signature}")
    
    # Different task
    task3 = Task(name="demo_dedup_task", args=[1, 3], kwargs={"key": "value"})
    task3.compute_and_set_signature()
    
    print(f"\nTask 3 (different args):")
    print(f"  Args: {task3.args}")
    print(f"  Signature: {task3.task_signature[:32]}...")
    print(f"  Different from Task 1: {task1.task_signature != task3.task_signature}")


def demo_register_and_check():
    """
    Demo 2: Register and check for duplicates
    """
    print("\n" + "=" * 60)
    print("Demo 2: Register and Check for Duplicates")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create task
    task = Task(
        name="demo_dedup_task",
        args=["test"],
        queue_name="demo_queue",
        unique=True
    )
    task.compute_and_set_signature()
    
    print(f"\nTask:")
    print(f"  Task ID: {task.id}")
    print(f"  Signature: {task.task_signature[:32]}...")
    print(f"  Unique: {task.unique}")
    
    # Register task
    print(f"\nRegistering task for deduplication...")
    success = task_deduplication.register_task(task)
    print(f"  Registered: {success}")
    
    # Check for duplicate
    print(f"\nChecking for duplicate...")
    duplicate_id = task_deduplication.check_duplicate(task)
    
    if duplicate_id:
        print(f"  Duplicate found: {duplicate_id}")
        print(f"  Same as original: {duplicate_id == task.id}")
    else:
        print(f"  No duplicate found")
    
    # Cleanup
    task_deduplication.remove_task(task)
    redis_broker.disconnect()


def demo_enqueue_duplicate():
    """
    Demo 3: Enqueue duplicate tasks
    """
    print("\n" + "=" * 60)
    print("Demo 3: Enqueue Duplicate Tasks")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_dedup_queue")
    queue.purge()
    
    # Create first task
    task1 = Task(
        name="demo_dedup_task",
        args=["duplicate_test"],
        queue_name="demo_dedup_queue",
        unique=True
    )
    
    print(f"\n1. Enqueuing first task...")
    print(f"   Task ID: {task1.id}")
    task_id1 = queue.enqueue(task1)
    print(f"   Enqueued: {task_id1}")
    print(f"   Queue size: {queue.size()}")
    
    # Create duplicate task
    task2 = Task(
        name="demo_dedup_task",
        args=["duplicate_test"],
        queue_name="demo_dedup_queue",
        unique=True
    )
    
    print(f"\n2. Enqueuing duplicate task...")
    print(f"   Task ID: {task2.id}")
    task_id2 = queue.enqueue(task2)
    print(f"   Enqueued: {task_id2}")
    print(f"   Queue size: {queue.size()}")
    
    print(f"\n3. Results:")
    print(f"   First task ID: {task_id1}")
    print(f"   Second task ID: {task_id2}")
    print(f"   Same task ID: {task_id1 == task_id2}")
    print(f"   Queue size: {queue.size()}")
    print(f"   Duplicate prevented: {task_id1 == task_id2 and queue.size() == 1}")
    
    # Cleanup
    queue.purge()
    redis_broker.disconnect()


def demo_enqueue_same_task_twice_verify_only_runs_once():
    """
    Demo 4: Enqueue same task twice, verify only runs once (Main test case)
    """
    print("\n" + "=" * 60)
    print("Demo 4: Enqueue Same Task Twice, Verify Only Runs Once (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_main_queue")
    queue.purge()
    
    print(f"\nScenario:")
    print(f"  1. Enqueue task with unique=True")
    print(f"  2. Enqueue same task again (same name, args, kwargs)")
    print(f"  3. Verify only one task in queue")
    print(f"  4. Verify same task_id returned\n")
    
    # Step 1: Create and enqueue first task
    task1 = Task(
        name="demo_dedup_task",
        args=["main_test"],
        queue_name="demo_main_queue",
        unique=True
    )
    
    print(f"[Step 1] Enqueuing first task...")
    task_id1 = queue.enqueue(task1)
    print(f"  Task ID: {task_id1}")
    print(f"  Queue size: {queue.size()}")
    
    # Step 2: Create and enqueue duplicate
    task2 = Task(
        name="demo_dedup_task",
        args=["main_test"],
        queue_name="demo_main_queue",
        unique=True
    )
    
    print(f"\n[Step 2] Enqueuing duplicate task...")
    task_id2 = queue.enqueue(task2)
    print(f"  Task ID: {task_id2}")
    print(f"  Queue size: {queue.size()}")
    
    # Step 3: Verify
    print(f"\n[Step 3] Verification:")
    print(f"  First task ID: {task_id1}")
    print(f"  Second task ID: {task_id2}")
    print(f"  Same task ID: {task_id1 == task_id2}")
    print(f"  Queue size: {queue.size()}")
    
    # Step 4: Verify signatures
    task1.compute_and_set_signature()
    task2.compute_and_set_signature()
    
    print(f"\n[Step 4] Signature verification:")
    print(f"  Task 1 signature: {task1.task_signature[:32]}...")
    print(f"  Task 2 signature: {task2.task_signature[:32]}...")
    print(f"  Signatures match: {task1.task_signature == task2.task_signature}")
    
    # Final verification
    if task_id1 == task_id2 and queue.size() == 1:
        print(f"\nTest: PASS - Duplicate task detected, only one task enqueued")
    else:
        print(f"\nTest: FAIL - Duplicate not properly handled")
    
    # Cleanup
    queue.purge()
    redis_broker.disconnect()


def demo_deduplication_ttl():
    """
    Demo 5: Deduplication TTL
    """
    print("\n" + "=" * 60)
    print("Demo 5: Deduplication TTL")
    print("=" * 60)
    
    redis_broker.connect()
    
    task = Task(
        name="demo_dedup_task",
        args=["ttl_test"],
        queue_name="demo_queue",
        unique=True
    )
    task.compute_and_set_signature()
    
    print(f"\nRegistering task with 3 second TTL...")
    task_deduplication.register_task(task, ttl=3)
    
    # Check immediately
    duplicate_id = task_deduplication.check_duplicate(task)
    print(f"  Duplicate check: {duplicate_id is not None}")
    
    # Wait for expiration
    print(f"\nWaiting for TTL expiration (3 seconds)...")
    time.sleep(3.5)
    
    # Check after expiration
    duplicate_id = task_deduplication.check_duplicate(task)
    print(f"  Duplicate check after expiration: {duplicate_id is not None}")
    print(f"  TTL expired: {duplicate_id is None}")
    
    redis_broker.disconnect()


def demo_pending_vs_completed():
    """
    Demo 6: Pending vs completed task handling
    """
    print("\n" + "=" * 60)
    print("Demo 6: Pending vs Completed Task Handling")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_queue")
    queue.purge()
    
    # Create and enqueue task
    task1 = Task(
        name="demo_dedup_task",
        args=["status_test"],
        queue_name="demo_queue",
        unique=True
    )
    task1.status = TaskStatus.PENDING
    
    task_id1 = queue.enqueue(task1)
    task_deduplication.update_task_status(task1)
    
    print(f"\n1. Task 1 (PENDING):")
    print(f"   Task ID: {task_id1}")
    print(f"   Status: {task1.status.value}")
    
    # Try to enqueue duplicate
    task2 = Task(
        name="demo_dedup_task",
        args=["status_test"],
        queue_name="demo_queue",
        unique=True
    )
    
    task_id2 = queue.enqueue(task2)
    print(f"\n2. Task 2 (duplicate, PENDING):")
    print(f"   Task ID: {task_id2}")
    print(f"   Duplicate detected: {task_id1 == task_id2}")
    
    # Mark task1 as completed
    task1.mark_success("result")
    task_deduplication.update_task_status(task1)
    
    print(f"\n3. Task 1 marked as SUCCESS")
    
    # Try to enqueue duplicate again
    task3 = Task(
        name="demo_dedup_task",
        args=["status_test"],
        queue_name="demo_queue",
        unique=True
    )
    
    task_id3 = queue.enqueue(task3)
    print(f"\n4. Task 3 (duplicate, completed task):")
    print(f"   Task ID: {task_id3}")
    print(f"   Duplicate detected: {task_id1 == task_id3}")
    print(f"   Re-execution prevented: {task_id1 == task_id3}")
    
    queue.purge()
    redis_broker.disconnect()


def demo_non_unique_tasks():
    """
    Demo 7: Non-unique tasks are not deduplicated
    """
    print("\n" + "=" * 60)
    print("Demo 7: Non-Unique Tasks Are Not Deduplicated")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_queue")
    queue.purge()
    
    # Create tasks without unique flag
    task1 = Task(
        name="demo_dedup_task",
        args=["non_unique"],
        queue_name="demo_queue",
        unique=False  # Not unique
    )
    
    task2 = Task(
        name="demo_dedup_task",
        args=["non_unique"],
        queue_name="demo_queue",
        unique=False  # Not unique
    )
    
    print(f"\n1. Enqueuing task 1 (unique=False)...")
    task_id1 = queue.enqueue(task1)
    print(f"   Task ID: {task_id1}")
    print(f"   Queue size: {queue.size()}")
    
    print(f"\n2. Enqueuing task 2 (unique=False)...")
    task_id2 = queue.enqueue(task2)
    print(f"   Task ID: {task_id2}")
    print(f"   Queue size: {queue.size()}")
    
    print(f"\n3. Results:")
    print(f"   Different task IDs: {task_id1 != task_id2}")
    print(f"   Both tasks enqueued: {queue.size() == 2}")
    print(f"   Deduplication not applied: {task_id1 != task_id2}")
    
    queue.purge()
    redis_broker.disconnect()


def demo_priority_queue_deduplication():
    """
    Demo 8: Deduplication with priority queue
    """
    print("\n" + "=" * 60)
    print("Demo 8: Deduplication with Priority Queue")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = PriorityQueue("demo_priority_queue")
    queue.purge()
    
    # Create tasks with same signature
    task1 = Task(
        name="demo_dedup_task",
        args=["priority_test"],
        queue_name="demo_priority_queue",
        unique=True
    )
    
    task2 = Task(
        name="demo_dedup_task",
        args=["priority_test"],
        queue_name="demo_priority_queue",
        unique=True
    )
    
    print(f"\n1. Enqueuing task 1 to priority queue...")
    task_id1 = queue.enqueue(task1)
    print(f"   Task ID: {task_id1}")
    print(f"   Queue size: {queue.size()}")
    
    print(f"\n2. Enqueuing duplicate task 2...")
    task_id2 = queue.enqueue(task2)
    print(f"   Task ID: {task_id2}")
    print(f"   Queue size: {queue.size()}")
    
    print(f"\n3. Results:")
    print(f"   Same task ID: {task_id1 == task_id2}")
    print(f"   Only one task: {queue.size() == 1}")
    print(f"   Deduplication works with priority queue: {task_id1 == task_id2}")
    
    queue.purge()
    redis_broker.disconnect()


def run_all_demos():
    """Run all task deduplication demonstrations."""
    print("\n" + "=" * 60)
    print("TASK DEDUPLICATION DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_task_signature()
        demo_register_and_check()
        demo_enqueue_duplicate()
        demo_enqueue_same_task_twice_verify_only_runs_once()  # Main test case
        demo_deduplication_ttl()
        demo_pending_vs_completed()
        demo_non_unique_tasks()
        demo_priority_queue_deduplication()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

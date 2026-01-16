"""
Demo script showing Redis queue operations.
"""
import time
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task, TaskPriority


def demo_basic_operations():
    """Demonstrate basic queue operations."""
    print("\n" + "=" * 60)
    print("Demo 1: Basic Queue Operations")
    print("=" * 60)
    
    # Connect to Redis
    redis_broker.connect()
    
    # Create queue
    queue = Queue("demo_queue")
    queue.purge()  # Clean slate
    
    # Enqueue tasks
    print("\nEnqueuing tasks...")
    task1 = Task(name="process_data", args=[1, 2, 3])
    task2 = Task(name="send_email", kwargs={"to": "user@example.com"})
    task3 = Task(name="generate_report", args=["monthly"])
    
    queue.enqueue(task1)
    queue.enqueue(task2)
    queue.enqueue(task3)
    
    print(f"Queue size: {queue.size()}")
    
    # Dequeue tasks
    print("\nDequeuing tasks...")
    while not queue.is_empty():
        task = queue.dequeue_nowait()
        if task:
            print(f"  Dequeued: {task.name} (ID: {task.id})")
    
    print(f"Queue size after dequeue: {queue.size()}")
    
    # Cleanup
    queue.purge()
    redis_broker.disconnect()


def demo_fifo_ordering():
    """Demonstrate FIFO ordering."""
    print("\n" + "=" * 60)
    print("Demo 2: FIFO Ordering")
    print("=" * 60)
    
    redis_broker.connect()
    queue = Queue("fifo_demo")
    queue.purge()
    
    # Enqueue in order
    print("\nEnqueuing tasks in order:")
    for i in range(1, 6):
        task = Task(name=f"task_{i}")
        queue.enqueue(task)
        print(f"  Enqueued: task_{i}")
    
    # Dequeue and verify order
    print("\nDequeuing tasks (should be in same order):")
    for i in range(1, 6):
        task = queue.dequeue_nowait()
        print(f"  Dequeued: {task.name}")
        assert task.name == f"task_{i}", "FIFO order violated!"
    
    print("\nFIFO order verified!")
    
    queue.purge()
    redis_broker.disconnect()


def demo_blocking_operations():
    """Demonstrate blocking dequeue."""
    print("\n" + "=" * 60)
    print("Demo 3: Blocking Operations")
    print("=" * 60)
    
    redis_broker.connect()
    queue = Queue("blocking_demo")
    queue.purge()
    
    # Try blocking dequeue on empty queue with timeout
    print("\nTrying to dequeue from empty queue (2 second timeout)...")
    start = time.time()
    task = queue.dequeue(timeout=2)
    elapsed = time.time() - start
    
    print(f"  Result: {task}")
    print(f"  Time elapsed: {elapsed:.2f}s")
    
    # Now add a task and dequeue immediately
    print("\nAdding task and dequeuing...")
    test_task = Task(name="immediate_task")
    queue.enqueue(test_task)
    
    start = time.time()
    task = queue.dequeue(timeout=5)
    elapsed = time.time() - start
    
    print(f"  Dequeued: {task.name}")
    print(f"  Time elapsed: {elapsed:.2f}s (should be nearly instant)")
    
    queue.purge()
    redis_broker.disconnect()


def demo_100_tasks():
    """Demonstrate handling 100 tasks (Task 3 requirement)."""
    print("\n" + "=" * 60)
    print("Demo 4: Enqueue/Dequeue 100 Tasks")
    print("=" * 60)
    
    redis_broker.connect()
    queue = Queue("bulk_demo")
    queue.purge()
    
    num_tasks = 100
    
    # Enqueue 100 tasks
    print(f"\nEnqueuing {num_tasks} tasks...")
    start = time.time()
    for i in range(num_tasks):
        task = Task(
            name=f"bulk_task_{i}",
            args=[i],
            kwargs={"index": i, "batch": "demo"}
        )
        queue.enqueue(task)
    enqueue_time = time.time() - start
    
    print(f"  Enqueued {num_tasks} tasks in {enqueue_time:.3f}s")
    print(f"  Queue size: {queue.size()}")
    
    # Dequeue all tasks
    print(f"\nDequeuing {num_tasks} tasks...")
    start = time.time()
    dequeued_count = 0
    while not queue.is_empty():
        task = queue.dequeue_nowait()
        if task:
            dequeued_count += 1
    dequeue_time = time.time() - start
    
    print(f"  Dequeued {dequeued_count} tasks in {dequeue_time:.3f}s")
    print(f"  Queue size: {queue.size()}")
    print(f"  Total time: {enqueue_time + dequeue_time:.3f}s")
    
    queue.purge()
    redis_broker.disconnect()


def demo_multiple_queues():
    """Demonstrate multiple isolated queues."""
    print("\n" + "=" * 60)
    print("Demo 5: Multiple Named Queues")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create multiple queues
    high_queue = Queue("high_priority")
    medium_queue = Queue("medium_priority")
    low_queue = Queue("low_priority")
    
    # Clean all queues
    high_queue.purge()
    medium_queue.purge()
    low_queue.purge()
    
    # Add tasks to different queues
    print("\nAdding tasks to different queues...")
    high_queue.enqueue(Task(name="urgent_task", priority=TaskPriority.HIGH))
    medium_queue.enqueue(Task(name="normal_task", priority=TaskPriority.MEDIUM))
    low_queue.enqueue(Task(name="background_task", priority=TaskPriority.LOW))
    
    print(f"  High priority queue size: {high_queue.size()}")
    print(f"  Medium priority queue size: {medium_queue.size()}")
    print(f"  Low priority queue size: {low_queue.size()}")
    
    # Verify isolation
    print("\nVerifying queue isolation...")
    high_task = high_queue.dequeue_nowait()
    print(f"  From high queue: {high_task.name}")
    print(f"  Medium queue still has: {medium_queue.size()} tasks")
    print(f"  Low queue still has: {low_queue.size()} tasks")
    
    # Cleanup
    high_queue.purge()
    medium_queue.purge()
    low_queue.purge()
    
    redis_broker.disconnect()


def demo_advanced_operations():
    """Demonstrate advanced queue operations."""
    print("\n" + "=" * 60)
    print("Demo 6: Advanced Operations")
    print("=" * 60)
    
    redis_broker.connect()
    queue = Queue("advanced_demo")
    queue.purge()
    
    # Add tasks
    task1 = Task(name="task1")
    task2 = Task(name="task2")
    task3 = Task(name="task3")
    
    queue.enqueue(task1)
    queue.enqueue(task2)
    queue.enqueue(task3)
    
    # Peek without removing
    print("\nPeeking at next task:")
    next_task = queue.peek()
    print(f"  Next task: {next_task.name}")
    print(f"  Queue size (unchanged): {queue.size()}")
    
    # Get all tasks
    print("\nGetting all tasks:")
    all_tasks = queue.get_all_tasks()
    for i, task in enumerate(all_tasks, 1):
        print(f"  {i}. {task.name}")
    print(f"  Queue size (unchanged): {queue.size()}")
    
    # Remove specific task
    print(f"\nRemoving task2 (ID: {task2.id}):")
    success = queue.remove_task(task2.id)
    print(f"  Removed: {success}")
    print(f"  Queue size: {queue.size()}")
    
    # Verify remaining tasks
    print("\nRemaining tasks:")
    remaining = queue.get_all_tasks()
    for task in remaining:
        print(f"  - {task.name}")
    
    queue.purge()
    redis_broker.disconnect()


def main():
    """Run all demos."""
    print("\n" + "#" * 60)
    print("# Redis Queue Demonstration")
    print("#" * 60)
    
    try:
        demo_basic_operations()
        demo_fifo_ordering()
        demo_blocking_operations()
        demo_100_tasks()
        demo_multiple_queues()
        demo_advanced_operations()
        
        print("\n" + "#" * 60)
        print("# All demos completed successfully!")
        print("#" * 60 + "\n")
        
    except Exception as e:
        print(f"\nError running demos: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

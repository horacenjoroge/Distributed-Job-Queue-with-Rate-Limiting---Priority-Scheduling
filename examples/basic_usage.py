"""
Basic usage examples for the job queue system.
"""
import time
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.backend.postgres_backend import postgres_backend
from jobqueue.core.queue import JobQueue
from jobqueue.core.task import TaskPriority


def example_1_simple_task():
    """Example 1: Submit a simple task."""
    print("\n=== Example 1: Simple Task Submission ===")
    
    # Initialize connections
    redis_broker.connect()
    postgres_backend.connect()
    postgres_backend.initialize_schema()
    
    # Create a queue
    queue = JobQueue(name="default")
    
    # Submit a task
    task = queue.submit_task(
        task_name="process_data",
        args=[1, 2, 3],
        kwargs={"multiplier": 2},
        priority=TaskPriority.HIGH
    )
    
    print(f"Task submitted: {task.id}")
    print(f"Status: {task.status}")
    print(f"Priority: {task.priority}")
    
    # Check task status
    time.sleep(1)
    updated_task = queue.get_task(task.id)
    print(f"Updated status: {updated_task.status}")
    
    # Cleanup
    redis_broker.disconnect()
    postgres_backend.disconnect()


def example_2_priority_tasks():
    """Example 2: Submit tasks with different priorities."""
    print("\n=== Example 2: Priority Tasks ===")
    
    # Initialize connections
    redis_broker.connect()
    postgres_backend.connect()
    postgres_backend.initialize_schema()
    
    queue = JobQueue(name="default")
    
    # Submit tasks with different priorities
    high_task = queue.submit_task(
        task_name="urgent_task",
        priority=TaskPriority.HIGH
    )
    
    medium_task = queue.submit_task(
        task_name="normal_task",
        priority=TaskPriority.MEDIUM
    )
    
    low_task = queue.submit_task(
        task_name="background_task",
        priority=TaskPriority.LOW
    )
    
    print(f"High priority task: {high_task.id}")
    print(f"Medium priority task: {medium_task.id}")
    print(f"Low priority task: {low_task.id}")
    
    # Get queue stats
    stats = queue.get_queue_stats()
    print(f"\nQueue stats:")
    print(f"Total queued: {stats['total_queued']}")
    print(f"By priority: {stats['queued_by_priority']}")
    
    # Cleanup
    redis_broker.disconnect()
    postgres_backend.disconnect()


def example_3_task_dependencies():
    """Example 3: Tasks with dependencies."""
    print("\n=== Example 3: Task Dependencies ===")
    
    # Initialize connections
    redis_broker.connect()
    postgres_backend.connect()
    postgres_backend.initialize_schema()
    
    queue = JobQueue(name="default")
    
    # Submit parent task
    parent_task = queue.submit_task(
        task_name="fetch_data",
        priority=TaskPriority.HIGH
    )
    
    print(f"Parent task: {parent_task.id}")
    
    # Submit child task that depends on parent
    child_task = queue.submit_task(
        task_name="process_data",
        depends_on=[parent_task.id],
        priority=TaskPriority.MEDIUM
    )
    
    print(f"Child task: {child_task.id}")
    print(f"Depends on: {child_task.depends_on}")
    print(f"Is ready: {child_task.is_ready()}")
    
    # Cleanup
    redis_broker.disconnect()
    postgres_backend.disconnect()


def example_4_task_cancellation():
    """Example 4: Cancel a task."""
    print("\n=== Example 4: Task Cancellation ===")
    
    # Initialize connections
    redis_broker.connect()
    postgres_backend.connect()
    postgres_backend.initialize_schema()
    
    queue = JobQueue(name="default")
    
    # Submit a task
    task = queue.submit_task(
        task_name="long_running_task",
        priority=TaskPriority.LOW
    )
    
    print(f"Task submitted: {task.id}")
    print(f"Status: {task.status}")
    
    # Cancel the task
    success = queue.cancel_task(task.id)
    print(f"Cancellation success: {success}")
    
    # Check updated status
    updated_task = queue.get_task(task.id)
    print(f"Updated status: {updated_task.status}")
    
    # Cleanup
    redis_broker.disconnect()
    postgres_backend.disconnect()


def example_5_queue_monitoring():
    """Example 5: Monitor queue statistics."""
    print("\n=== Example 5: Queue Monitoring ===")
    
    # Initialize connections
    redis_broker.connect()
    postgres_backend.connect()
    postgres_backend.initialize_schema()
    
    queue = JobQueue(name="default")
    
    # Submit some tasks
    for i in range(5):
        queue.submit_task(
            task_name=f"task_{i}",
            priority=TaskPriority.MEDIUM
        )
    
    # Get detailed stats
    stats = queue.get_queue_stats()
    
    print(f"Queue name: {stats['queue_name']}")
    print(f"Total queued: {stats['total_queued']}")
    print(f"\nQueued by priority:")
    for priority, count in stats['queued_by_priority'].items():
        print(f"  {priority}: {count}")
    
    print(f"\nStatus counts:")
    for status, count in stats['status_counts'].items():
        print(f"  {status}: {count}")
    
    # Cleanup
    redis_broker.disconnect()
    postgres_backend.disconnect()


if __name__ == "__main__":
    print("Job Queue System - Usage Examples")
    print("=" * 50)
    
    try:
        example_1_simple_task()
        example_2_priority_tasks()
        example_3_task_dependencies()
        example_4_task_cancellation()
        example_5_queue_monitoring()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()

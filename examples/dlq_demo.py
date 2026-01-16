"""
Dead Letter Queue demonstration.

Shows how failed tasks are moved to DLQ after max retries.
"""
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority, TaskStatus
from jobqueue.core.dead_letter_queue import dead_letter_queue, add_to_dlq
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


# Counter for tracking failures
failure_counter = {}


@task_registry.register("unreliable_task")
def unreliable_task(task_id: str, fail_count: int = 3):
    """
    Task that fails a specified number of times.
    
    Args:
        task_id: Unique task ID
        fail_count: Number of times to fail
    """
    if task_id not in failure_counter:
        failure_counter[task_id] = 0
    
    failure_counter[task_id] += 1
    attempt = failure_counter[task_id]
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Attempt {attempt}")
    
    if attempt <= fail_count:
        raise Exception(f"Intentional failure on attempt {attempt}")
    
    return f"Success after {attempt} attempts"


def demo_task_fails_3_times_to_dlq():
    """
    Demo 1: Task fails 3 times → lands in DLQ
    Main test case from requirements
    """
    print("\n" + "=" * 60)
    print("Demo 1: Task Fails 3 Times → Lands in DLQ (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "dlq_test_task"
    failure_counter[task_id] = 0
    
    # Clean DLQ
    dead_letter_queue.purge()
    
    # Create task with max_retries=3
    task = Task(
        name="unreliable_task",
        args=[task_id, 3],  # Fail 3 times
        max_retries=3,
        queue_name="demo_dlq"
    )
    
    print(f"\nTask created: {task.id}")
    print(f"Max retries: {task.max_retries}")
    print(f"Will fail 3 times, then move to DLQ")
    
    # Simulate task execution with failures
    print(f"\n{'Attempt':<10} {'Result':<15} {'Retry Count':<15} {'Status'}")
    print("-" * 60)
    
    for attempt in range(4):
        try:
            result = unreliable_task(task_id, 3)
            task.mark_success(result)
            print(f"{attempt+1:<10} {'Success':<15} {task.retry_count:<15} {task.status.value}")
            break
        except Exception as e:
            if task.can_retry():
                task.record_retry_attempt(str(e), 2.0 ** task.retry_count)
                task.increment_retry()
                print(f"{attempt+1:<10} {'Failed':<15} {task.retry_count:<15} Retry")
            else:
                task.mark_failed(str(e))
                print(f"{attempt+1:<10} {'Failed':<15} {task.retry_count:<15} Max retries")
                break
    
    # Move to DLQ (worker would do this automatically)
    if not task.can_retry() and task.status == TaskStatus.FAILED:
        try:
            raise Exception(task.error or "Task exceeded max retries")
        except Exception as e:
            add_to_dlq(task, f"Task failed after {task.max_retries} retries", e)
    
    # Verify task is in DLQ
    print(f"\nDLQ Status:")
    print(f"  Size: {dead_letter_queue.size()}")
    
    if dead_letter_queue.size() > 0:
        entry = dead_letter_queue.get_task_by_id(task.id)
        if entry:
            print(f"  Task ID: {entry['task_id']}")
            print(f"  Failure Reason: {entry['failure_reason']}")
            print(f"  Retry Count: {entry['retry_count']}")
            print(f"  Max Retries: {entry['max_retries']}")
            print(f"  Added At: {entry['added_at']}")
            print(f"  Has Stack Trace: {entry['stack_trace'] is not None}")
    
    # Cleanup
    dead_letter_queue.purge()
    redis_broker.disconnect()


def demo_view_dlq_tasks():
    """
    Demo 2: View tasks in Dead Letter Queue
    """
    print("\n" + "=" * 60)
    print("Demo 2: View Dead Letter Queue Tasks")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean DLQ
    dead_letter_queue.purge()
    
    # Add multiple failed tasks
    print("\nAdding 5 failed tasks to DLQ...\n")
    
    for i in range(5):
        task = Task(
            name=f"failed_task_{i}",
            args=[i],
            max_retries=3,
            retry_count=3,
            queue_name="demo_queue"
        )
        task.mark_failed(f"Error {i}")
        
        try:
            raise ValueError(f"Task {i} failed")
        except ValueError as e:
            add_to_dlq(task, f"Task {i} exceeded max retries", e)
    
    # View all tasks
    print(f"DLQ Size: {dead_letter_queue.size()}\n")
    
    tasks = dead_letter_queue.get_tasks(limit=10)
    
    print(f"{'Task ID':<40} {'Task Name':<20} {'Failure Reason':<30}")
    print("-" * 90)
    
    for entry in tasks:
        task_id_short = entry['task_id'][:38] + "..." if len(entry['task_id']) > 40 else entry['task_id']
        print(f"{task_id_short:<40} {entry['task_name']:<20} {entry['failure_reason'][:28]:<30}")
    
    # Cleanup
    dead_letter_queue.purge()
    redis_broker.disconnect()


def demo_dlq_statistics():
    """
    Demo 3: DLQ Statistics
    """
    print("\n" + "=" * 60)
    print("Demo 3: Dead Letter Queue Statistics")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean DLQ
    dead_letter_queue.purge()
    
    # Add tasks from different queues and task names
    task_configs = [
        ("api_call", "queue1", 3),
        ("api_call", "queue1", 2),
        ("db_query", "queue2", 2),
        ("file_process", "queue1", 1),
        ("email_send", "queue3", 1),
    ]
    
    print("\nAdding tasks to DLQ...\n")
    
    for task_name, queue_name, count in task_configs:
        for i in range(count):
            task = Task(
                name=task_name,
                args=[i],
                max_retries=3,
                retry_count=3,
                queue_name=queue_name
            )
            task.mark_failed("Error")
            add_to_dlq(task, f"{task_name} failed")
    
    # Get statistics
    stats = dead_letter_queue.get_stats()
    
    print("DLQ Statistics:")
    print(f"  Total Size: {stats['size']}")
    print(f"\n  By Task Name:")
    for task_name, count in stats['by_task_name'].items():
        print(f"    {task_name}: {count}")
    
    print(f"\n  By Queue:")
    for queue_name, count in stats['by_queue'].items():
        print(f"    {queue_name}: {count}")
    
    print(f"\n  Oldest Entry: {stats['oldest_entry']}")
    print(f"  Newest Entry: {stats['newest_entry']}")
    
    # Cleanup
    dead_letter_queue.purge()
    redis_broker.disconnect()


def demo_retry_from_dlq():
    """
    Demo 4: Retry task from Dead Letter Queue
    """
    print("\n" + "=" * 60)
    print("Demo 4: Retry Task from Dead Letter Queue")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_retry_queue")
    queue.purge()
    dead_letter_queue.purge()
    
    # Add task to DLQ
    task = Task(
        name="test_task",
        args=["retry_test"],
        max_retries=3,
        retry_count=3,
        queue_name="demo_retry_queue"
    )
    task.mark_failed("Test error")
    
    try:
        raise Exception("Test exception")
    except Exception as e:
        add_to_dlq(task, "Test failure", e)
    
    print(f"\nTask in DLQ: {task.id}")
    print(f"DLQ Size: {dead_letter_queue.size()}")
    print(f"Queue Size: {queue.size()}")
    
    # Retry task
    print(f"\nRetrying task from DLQ...")
    retried_task = dead_letter_queue.retry_task(task.id, reset_retry_count=True)
    
    if retried_task:
        print(f"  Task retried: {retried_task.id}")
        print(f"  Retry count reset: {retried_task.retry_count}")
        print(f"  Status: {retried_task.status.value}")
        print(f"  DLQ Size: {dead_letter_queue.size()}")
        print(f"  Queue Size: {queue.size()}")
    
    # Cleanup
    queue.purge()
    dead_letter_queue.purge()
    redis_broker.disconnect()


def demo_purge_dlq():
    """
    Demo 5: Purge Dead Letter Queue
    """
    print("\n" + "=" * 60)
    print("Demo 5: Purge Dead Letter Queue")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean DLQ
    dead_letter_queue.purge()
    
    # Add tasks
    print("\nAdding 10 tasks to DLQ...")
    
    for i in range(10):
        task = Task(
            name=f"task_{i}",
            args=[i],
            max_retries=3,
            retry_count=3
        )
        task.mark_failed("Error")
        add_to_dlq(task, f"Failure {i}")
    
    print(f"DLQ Size before purge: {dead_letter_queue.size()}")
    
    # Purge
    purged = dead_letter_queue.purge()
    
    print(f"Tasks purged: {purged}")
    print(f"DLQ Size after purge: {dead_letter_queue.size()}")
    
    redis_broker.disconnect()


def demo_dlq_alert_threshold():
    """
    Demo 6: DLQ Alert Threshold
    """
    print("\n" + "=" * 60)
    print("Demo 6: Dead Letter Queue Alert Threshold")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean DLQ
    dead_letter_queue.purge()
    
    # Add tasks below threshold
    print("\nAdding 50 tasks (below threshold of 100)...")
    
    for i in range(50):
        task = Task(name=f"task_{i}", args=[i], max_retries=3, retry_count=3)
        task.mark_failed("Error")
        add_to_dlq(task, f"Failure {i}")
    
    # Check threshold
    exceeds, size = dead_letter_queue.check_alert_threshold(threshold=100)
    
    print(f"\nDLQ Size: {size}")
    print(f"Threshold: 100")
    print(f"Exceeds Threshold: {exceeds}")
    print(f"Alert: {'YES' if exceeds else 'NO'}")
    
    # Add more tasks to exceed threshold
    print(f"\nAdding 51 more tasks...")
    
    for i in range(51):
        task = Task(name=f"task_{i+50}", args=[i+50], max_retries=3, retry_count=3)
        task.mark_failed("Error")
        add_to_dlq(task, f"Failure {i+50}")
    
    # Check threshold again
    exceeds, size = dead_letter_queue.check_alert_threshold(threshold=100)
    
    print(f"\nDLQ Size: {size}")
    print(f"Threshold: 100")
    print(f"Exceeds Threshold: {exceeds}")
    print(f"Alert: {'YES' if exceeds else 'NO'}")
    
    # Cleanup
    dead_letter_queue.purge()
    redis_broker.disconnect()


def demo_stack_trace_storage():
    """
    Demo 7: Stack Trace Storage in DLQ
    """
    print("\n" + "=" * 60)
    print("Demo 7: Stack Trace Storage in DLQ")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean DLQ
    dead_letter_queue.purge()
    
    # Create task
    task = Task(
        name="test_task",
        args=[1],
        max_retries=3,
        retry_count=3
    )
    task.mark_failed("Test error")
    
    # Create exception with stack trace
    try:
        def inner_function():
            raise ValueError("Inner error")
        
        def outer_function():
            inner_function()
        
        outer_function()
    except ValueError as e:
        exception = e
    
    # Add to DLQ with exception
    add_to_dlq(task, "Test failure with stack trace", exception)
    
    # Retrieve and show stack trace
    entry = dead_letter_queue.get_task_by_id(task.id)
    
    if entry:
        print(f"\nTask ID: {entry['task_id']}")
        print(f"Failure Reason: {entry['failure_reason']}")
        print(f"\nStack Trace:")
        print("-" * 60)
        print(entry['stack_trace'])
        print("-" * 60)
    
    # Cleanup
    dead_letter_queue.purge()
    redis_broker.disconnect()


def run_all_demos():
    """Run all Dead Letter Queue demonstrations."""
    print("\n" + "=" * 60)
    print("DEAD LETTER QUEUE DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_task_fails_3_times_to_dlq()  # Main test case
        demo_view_dlq_tasks()
        demo_dlq_statistics()
        demo_retry_from_dlq()
        demo_purge_dlq()
        demo_dlq_alert_threshold()
        demo_stack_trace_storage()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

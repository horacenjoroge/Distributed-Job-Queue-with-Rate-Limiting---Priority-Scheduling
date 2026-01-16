"""
Task cancellation demonstration.

Shows how to cancel pending and running tasks.
"""
import time
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskStatus
from jobqueue.core.task_cancellation import (
    task_cancellation,
    CancellationReason
)
from jobqueue.core.redis_queue import Queue
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("demo_cancel_task")
def demo_cancel_task(value):
    """Demo task function."""
    return f"Processed: {value}"


def demo_request_cancellation():
    """
    Demo 1: Request cancellation
    """
    print("\n" + "=" * 60)
    print("Demo 1: Request Cancellation")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "demo_task_1"
    
    print(f"\nRequesting cancellation for task {task_id}...")
    success = task_cancellation.request_cancellation(
        task_id,
        reason=CancellationReason.USER_REQUESTED,
        force=False
    )
    
    print(f"  Cancellation requested: {success}")
    
    # Check cancellation
    should_cancel, force, reason = task_cancellation.should_cancel(task_id)
    print(f"  Should cancel: {should_cancel}")
    print(f"  Force: {force}")
    print(f"  Reason: {reason}")
    
    # Cleanup
    task_cancellation.complete_cancellation(task_id)
    redis_broker.disconnect()


def demo_cancel_pending_task():
    """
    Demo 2: Cancel pending task
    """
    print("\n" + "=" * 60)
    print("Demo 2: Cancel Pending Task")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_cancel_queue")
    queue.purge()
    
    # Create and enqueue task
    task = Task(
        name="demo_cancel_task",
        args=["pending_test"],
        queue_name="demo_cancel_queue"
    )
    task.status = TaskStatus.PENDING
    
    print(f"\n1. Enqueuing task...")
    queue.enqueue(task)
    print(f"   Task ID: {task.id}")
    print(f"   Status: {task.status.value}")
    print(f"   Queue size: {queue.size()}")
    
    # Cancel task
    print(f"\n2. Cancelling pending task...")
    success = task_cancellation.cancel_pending_task(
        task,
        reason=CancellationReason.USER_REQUESTED
    )
    
    print(f"   Cancelled: {success}")
    print(f"   New status: {task.status.value}")
    print(f"   Queue size: {queue.size()}")
    print(f"   Removed from queue: {queue.size() == 0}")
    
    queue.purge()
    redis_broker.disconnect()


def demo_cancel_running_task():
    """
    Demo 3: Cancel running task
    """
    print("\n" + "=" * 60)
    print("Demo 3: Cancel Running Task")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Create running task
    task = Task(
        name="demo_cancel_task",
        args=["running_test"],
        queue_name="demo_queue"
    )
    task.status = TaskStatus.RUNNING
    task.worker_id = "worker_1"
    task.started_at = datetime.utcnow()
    
    print(f"\n1. Task running...")
    print(f"   Task ID: {task.id}")
    print(f"   Status: {task.status.value}")
    print(f"   Worker ID: {task.worker_id}")
    
    # Request cancellation
    print(f"\n2. Requesting cancellation...")
    success = task_cancellation.cancel_running_task(
        task,
        reason=CancellationReason.USER_REQUESTED,
        force=False
    )
    
    print(f"   Cancellation requested: {success}")
    
    # Check cancellation
    should_cancel, force, reason = task_cancellation.should_cancel(task.id)
    print(f"   Should cancel: {should_cancel}")
    print(f"   Force: {force}")
    print(f"   Reason: {reason}")
    print(f"   Worker will check should_cancel() and stop execution")
    
    # Cleanup
    task_cancellation.complete_cancellation(task.id)
    redis_broker.disconnect()


def demo_cancel_pending_and_running_tasks():
    """
    Demo 4: Cancel pending and running tasks (Main test case)
    """
    print("\n" + "=" * 60)
    print("Demo 4: Cancel Pending and Running Tasks (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("demo_main_queue")
    queue.purge()
    
    print(f"\nScenario:")
    print(f"  1. Cancel a pending task (remove from queue)")
    print(f"  2. Cancel a running task (request cancellation)")
    print(f"  3. Verify both cancellations\n")
    
    # Test 1: Cancel pending task
    print(f"[Test 1] Cancelling pending task...")
    task1 = Task(
        name="demo_cancel_task",
        args=["pending"],
        queue_name="demo_main_queue"
    )
    task1.status = TaskStatus.PENDING
    
    queue.enqueue(task1)
    print(f"  Task ID: {task1.id}")
    print(f"  Status: {task1.status.value}")
    print(f"  Queue size: {queue.size()}")
    
    success = task_cancellation.cancel_pending_task(
        task1,
        reason=CancellationReason.USER_REQUESTED
    )
    
    print(f"  Cancelled: {success}")
    print(f"  New status: {task1.status.value}")
    print(f"  Queue size: {queue.size()}")
    print(f"  Removed from queue: {queue.size() == 0}")
    
    assert success
    assert task1.status == TaskStatus.CANCELLED
    assert queue.size() == 0
    
    # Test 2: Cancel running task
    print(f"\n[Test 2] Cancelling running task...")
    task2 = Task(
        name="demo_cancel_task",
        args=["running"],
        queue_name="demo_main_queue"
    )
    task2.status = TaskStatus.RUNNING
    task2.worker_id = "worker_1"
    
    print(f"  Task ID: {task2.id}")
    print(f"  Status: {task2.status.value}")
    print(f"  Worker ID: {task2.worker_id}")
    
    success = task_cancellation.cancel_running_task(
        task2,
        reason=CancellationReason.USER_REQUESTED,
        force=False
    )
    
    print(f"  Cancellation requested: {success}")
    
    should_cancel, force, reason = task_cancellation.should_cancel(task2.id)
    print(f"  Should cancel: {should_cancel}")
    print(f"  Force: {force}")
    print(f"  Reason: {reason}")
    
    assert success
    assert should_cancel is True
    
    print(f"\nTest: PASS - Both pending and running tasks cancelled successfully")
    
    # Cleanup
    task_cancellation.complete_cancellation(task2.id)
    queue.purge()
    redis_broker.disconnect()


def demo_cancellation_reasons():
    """
    Demo 5: Cancellation reasons
    """
    print("\n" + "=" * 60)
    print("Demo 5: Cancellation Reasons")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "demo_reason_task"
    
    reasons = [
        (CancellationReason.USER_REQUESTED, "User requested cancellation"),
        (CancellationReason.TIMEOUT, "Task exceeded timeout"),
        (CancellationReason.DEPENDENCY_FAILED, "Dependency task failed"),
        (CancellationReason.WORKER_SHUTDOWN, "Worker shutting down"),
        (CancellationReason.SYSTEM_ERROR, "System error occurred")
    ]
    
    print(f"\nAvailable cancellation reasons:")
    for reason, description in reasons:
        print(f"  {reason.value}: {description}")
    
    print(f"\nTesting each reason...")
    for reason, description in reasons:
        task_cancellation.request_cancellation(task_id, reason=reason)
        should_cancel, _, stored_reason = task_cancellation.should_cancel(task_id)
        
        print(f"  {reason.value}: {stored_reason}")
        assert stored_reason == reason.value
        
        task_cancellation.complete_cancellation(task_id)
    
    redis_broker.disconnect()


def demo_graceful_vs_force():
    """
    Demo 6: Graceful vs force cancellation
    """
    print("\n" + "=" * 60)
    print("Demo 6: Graceful vs Force Cancellation")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "demo_force_task"
    
    # Graceful cancellation
    print(f"\n1. Graceful cancellation...")
    task_cancellation.request_cancellation(task_id, force=False)
    should_cancel, force, _ = task_cancellation.should_cancel(task_id)
    
    print(f"   Should cancel: {should_cancel}")
    print(f"   Force: {force}")
    print(f"   Worker will check should_cancel() and stop gracefully")
    
    task_cancellation.complete_cancellation(task_id)
    
    # Force cancellation
    print(f"\n2. Force cancellation...")
    task_cancellation.request_cancellation(task_id, force=True)
    should_cancel, force, _ = task_cancellation.should_cancel(task_id)
    
    print(f"   Should cancel: {should_cancel}")
    print(f"   Force: {force}")
    print(f"   Worker should force kill the task")
    
    task_cancellation.complete_cancellation(task_id)
    redis_broker.disconnect()


def demo_cancellation_info():
    """
    Demo 7: Get cancellation information
    """
    print("\n" + "=" * 60)
    print("Demo 7: Get Cancellation Information")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "demo_info_task"
    
    # No cancellation initially
    info = task_cancellation.get_cancellation_info(task_id)
    print(f"\n1. No cancellation:")
    print(f"   Info: {info}")
    
    # Request cancellation
    task_cancellation.request_cancellation(
        task_id,
        reason=CancellationReason.TIMEOUT,
        force=True
    )
    
    # Get info
    info = task_cancellation.get_cancellation_info(task_id)
    
    print(f"\n2. After cancellation request:")
    print(f"   Task ID: {info['task_id']}")
    print(f"   Cancelled: {info['cancelled']}")
    print(f"   Force: {info['force']}")
    print(f"   Reason: {info['reason']}")
    
    task_cancellation.complete_cancellation(task_id)
    redis_broker.disconnect()


def demo_worker_cancellation_check():
    """
    Demo 8: Worker cancellation check
    """
    print("\n" + "=" * 60)
    print("Demo 8: Worker Cancellation Check")
    print("=" * 60)
    
    redis_broker.connect()
    
    task_id = "demo_worker_task"
    
    print(f"\nHow workers check for cancellation:")
    print(f"  1. Before starting execution: check should_cancel()")
    print(f"  2. After execution completes: check should_cancel()")
    print(f"  3. If cancelled: mark task as CANCELLED and stop")
    
    # Request cancellation
    task_cancellation.request_cancellation(task_id)
    
    # Simulate worker check
    should_cancel, force, reason = task_cancellation.should_cancel(task_id)
    
    print(f"\nWorker check result:")
    print(f"  Should cancel: {should_cancel}")
    print(f"  Force: {force}")
    print(f"  Reason: {reason}")
    
    if should_cancel:
        print(f"  Action: Mark task as CANCELLED and stop execution")
    
    task_cancellation.complete_cancellation(task_id)
    redis_broker.disconnect()


def run_all_demos():
    """Run all task cancellation demonstrations."""
    print("\n" + "=" * 60)
    print("TASK CANCELLATION DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_request_cancellation()
        demo_cancel_pending_task()
        demo_cancel_running_task()
        demo_cancel_pending_and_running_tasks()  # Main test case
        demo_cancellation_reasons()
        demo_graceful_vs_force()
        demo_cancellation_info()
        demo_worker_cancellation_check()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

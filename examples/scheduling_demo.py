"""
Task scheduling demonstration.

Shows how to schedule tasks with eta, countdown, and cron expressions.
"""
import time
from datetime import datetime, timedelta
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority
from jobqueue.core.scheduled_tasks import scheduled_task_store
from jobqueue.core.scheduler import TaskScheduler
from jobqueue.core.recurring_tasks import recurring_task_manager
from jobqueue.core.redis_queue import Queue
from jobqueue.utils.timezone import schedule_task_at_time
from jobqueue.utils.logger import log


def demo_eta_scheduling():
    """
    Demo 1: Schedule task with absolute time (eta)
    """
    print("\n" + "=" * 60)
    print("Demo 1: ETA Scheduling (Absolute Time)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_scheduled"
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    
    # Schedule task for 10 seconds from now
    eta = datetime.utcnow() + timedelta(seconds=10)
    
    task = Task(
        name="eta_task",
        args=["Hello from ETA task"],
        queue_name=queue_name,
        eta=eta,
        priority=TaskPriority.HIGH
    )
    
    task.set_schedule_time()
    scheduled_task_store.schedule_task(task)
    
    print(f"Current time: {datetime.utcnow().strftime('%H:%M:%S')}")
    print(f"Task scheduled for: {eta.strftime('%H:%M:%S')}")
    print(f"Delay: 10 seconds")
    
    # Check scheduled count
    count = scheduled_task_store.get_scheduled_count(queue_name)
    print(f"\nScheduled tasks: {count}")
    
    # Show stats
    stats = scheduled_task_store.get_stats(queue_name)
    print(f"Next execution: {stats['next_execution_time']}")
    
    # Cleanup
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    redis_broker.disconnect()


def demo_countdown_scheduling():
    """
    Demo 2: Schedule task with countdown (relative time)
    """
    print("\n" + "=" * 60)
    print("Demo 2: Countdown Scheduling (Relative Time)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_countdown"
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    
    # Schedule task with 15 second countdown
    task = Task(
        name="countdown_task",
        args=["Delayed by countdown"],
        queue_name=queue_name,
        countdown=15,  # Execute in 15 seconds
        priority=TaskPriority.MEDIUM
    )
    
    task.set_schedule_time()
    scheduled_task_store.schedule_task(task)
    
    print(f"Current time: {datetime.utcnow().strftime('%H:%M:%S')}")
    print(f"Countdown: 15 seconds")
    print(f"Will execute at: {task.schedule_time.strftime('%H:%M:%S')}")
    
    # Cleanup
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    redis_broker.disconnect()


def demo_scheduler_process():
    """
    Demo 3: Scheduler process moving ready tasks
    """
    print("\n" + "=" * 60)
    print("Demo 3: Scheduler Process")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_scheduler"
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    
    # Schedule tasks with different delays
    print("Scheduling 3 tasks:")
    
    for i in range(3):
        delay = (i + 1) * 5  # 5, 10, 15 seconds
        task = Task(
            name=f"task_{i+1}",
            args=[f"Task {i+1}"],
            queue_name=queue_name,
            countdown=delay
        )
        task.set_schedule_time()
        scheduled_task_store.schedule_task(task)
        
        print(f"  Task {i+1}: {delay}s delay (execute at {task.schedule_time.strftime('%H:%M:%S')})")
    
    print(f"\nScheduled tasks: {scheduled_task_store.get_scheduled_count(queue_name)}")
    
    # Create scheduler
    print("\nStarting scheduler (will run for 20 seconds)...")
    scheduler = TaskScheduler(
        poll_interval=1,
        queues=[queue_name]
    )
    
    # Run scheduler for limited time
    start_time = time.time()
    scheduler.running = True
    
    while time.time() - start_time < 20:
        scheduler._process_queue(queue_name)
        time.sleep(1)
        
        # Show progress
        scheduled_count = scheduled_task_store.get_scheduled_count(queue_name)
        queue = Queue(queue_name)
        executed_count = queue.size()
        
        if scheduled_count > 0 or executed_count > 0:
            elapsed = int(time.time() - start_time)
            print(f"  [{elapsed:2d}s] Scheduled: {scheduled_count}, Ready: {executed_count}")
    
    scheduler.running = False
    
    # Show final stats
    print(f"\nScheduler stats:")
    print(f"  Tasks moved: {scheduler.tasks_moved}")
    
    # Cleanup
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_cron_recurring_tasks():
    """
    Demo 4: Recurring tasks with cron expressions
    """
    print("\n" + "=" * 60)
    print("Demo 4: Recurring Tasks (Cron)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_recurring"
    
    # Register recurring tasks
    print("Registering recurring tasks:\n")
    
    # Every 5 minutes
    task1 = recurring_task_manager.register_recurring_task(
        name="cleanup_job",
        cron_expression="*/5 * * * *",
        task_name="cleanup_temp_files",
        queue_name=queue_name,
        priority=TaskPriority.LOW
    )
    print(f"1. Cleanup Job")
    print(f"   Cron: */5 * * * * (every 5 minutes)")
    print(f"   Next run: {task1.next_run_at.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Daily at midnight
    task2 = recurring_task_manager.register_recurring_task(
        name="daily_report",
        cron_expression="0 0 * * *",
        task_name="generate_daily_report",
        queue_name=queue_name,
        priority=TaskPriority.MEDIUM
    )
    print(f"\n2. Daily Report")
    print(f"   Cron: 0 0 * * * (daily at midnight)")
    print(f"   Next run: {task2.next_run_at.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Hourly
    task3 = recurring_task_manager.register_recurring_task(
        name="health_check",
        cron_expression="@hourly",
        task_name="check_system_health",
        queue_name=queue_name,
        priority=TaskPriority.HIGH
    )
    print(f"\n3. Health Check")
    print(f"   Cron: @hourly")
    print(f"   Next run: {task3.next_run_at.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # List all recurring tasks
    print(f"\n\nAll recurring tasks:")
    tasks = recurring_task_manager.list_recurring_tasks()
    for task_info in tasks:
        print(f"  - {task_info['name']}: {task_info['cron_expression']}")
    
    # Get stats
    stats = recurring_task_manager.get_stats()
    print(f"\nRecurring task stats:")
    print(f"  Total: {stats['total_recurring_tasks']}")
    print(f"  By queue: {stats['by_queue']}")
    print(f"  By priority: {stats['by_priority']}")
    
    # Cleanup
    recurring_task_manager.unregister_recurring_task("cleanup_job")
    recurring_task_manager.unregister_recurring_task("daily_report")
    recurring_task_manager.unregister_recurring_task("health_check")
    
    redis_broker.disconnect()


def demo_timezone_scheduling():
    """
    Demo 5: Schedule tasks in different timezones
    """
    print("\n" + "=" * 60)
    print("Demo 5: Timezone-Aware Scheduling")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_timezone"
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    
    # Schedule for 9 AM EST tomorrow
    print("Scheduling task for 9:00 AM EST tomorrow")
    
    eta_est = schedule_task_at_time(
        hour=9,
        minute=0,
        tz_name="America/New_York",
        days_ahead=1
    )
    
    task = Task(
        name="morning_report",
        args=["Good morning!"],
        queue_name=queue_name,
        eta=eta_est
    )
    
    task.set_schedule_time()
    scheduled_task_store.schedule_task(task)
    
    print(f"Current UTC time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scheduled UTC time: {eta_est.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"That's 9:00 AM EST tomorrow")
    
    # Cleanup
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    redis_broker.disconnect()


def demo_10_second_delay_test():
    """
    Demo 6: Test case - Schedule for +10 seconds, verify delay
    
    This is the main test case from the requirements.
    """
    print("\n" + "=" * 60)
    print("Demo 6: 10-Second Delay Test (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "test_10_second_delay"
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    Queue(queue_name).purge()
    
    # Schedule task for +10 seconds
    print("Test: Schedule task for +10 seconds, verify delay\n")
    
    task = Task(
        name="test_delayed_task",
        args=["This task was delayed by 10 seconds"],
        queue_name=queue_name,
        countdown=10
    )
    
    task.set_schedule_time()
    
    start_time = datetime.utcnow()
    print(f"Start time: {start_time.strftime('%H:%M:%S.%f')[:-3]}")
    print(f"Scheduled for: {task.schedule_time.strftime('%H:%M:%S.%f')[:-3]}")
    print(f"Countdown: 10 seconds")
    
    # Schedule task
    scheduled_task_store.schedule_task(task)
    
    # Verify not ready immediately
    ready_tasks = scheduled_task_store.get_ready_tasks(queue_name)
    print(f"\nImmediately after scheduling:")
    print(f"  Ready tasks: {len(ready_tasks)} (should be 0)")
    
    # Wait and check periodically
    print(f"\nWaiting for task to become ready...")
    
    for i in range(12):
        time.sleep(1)
        elapsed = i + 1
        ready_tasks = scheduled_task_store.get_ready_tasks(queue_name)
        status = "READY" if len(ready_tasks) > 0 else "waiting"
        print(f"  [{elapsed:2d}s] Status: {status}")
        
        if len(ready_tasks) > 0:
            break
    
    # Verify task is now ready
    end_time = datetime.utcnow()
    actual_delay = (end_time - start_time).total_seconds()
    
    print(f"\nResults:")
    print(f"  End time: {end_time.strftime('%H:%M:%S.%f')[:-3]}")
    print(f"  Actual delay: {actual_delay:.2f} seconds")
    print(f"  Expected delay: 10 seconds")
    print(f"  Difference: {abs(actual_delay - 10):.2f} seconds")
    
    if 10 <= actual_delay <= 11:
        print(f"  Test: PASS")
    else:
        print(f"  Test: FAIL (delay outside acceptable range)")
    
    # Cleanup
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_scheduled_task_management():
    """
    Demo 7: Managing scheduled tasks
    """
    print("\n" + "=" * 60)
    print("Demo 7: Scheduled Task Management")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_management"
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    
    # Schedule multiple tasks
    print("Scheduling 5 tasks with different delays:\n")
    
    task_ids = []
    for i in range(5):
        delay = (i + 1) * 10
        task = Task(
            name=f"task_{i+1}",
            args=[f"Task {i+1}"],
            queue_name=queue_name,
            countdown=delay
        )
        task.set_schedule_time()
        scheduled_task_store.schedule_task(task)
        task_ids.append(task.id)
        
        print(f"  Task {i+1}: {delay}s delay")
    
    # Show stats
    stats = scheduled_task_store.get_stats(queue_name)
    print(f"\nScheduled task stats:")
    print(f"  Total: {stats['total_scheduled']}")
    print(f"  Ready: {stats['ready_to_execute']}")
    print(f"  Waiting: {stats['waiting']}")
    print(f"  Next execution: {stats['next_execution_time']}")
    
    # Peek at upcoming tasks
    print(f"\nUpcoming tasks:")
    upcoming = scheduled_task_store.peek_scheduled_tasks(queue_name, limit=3)
    for task, scheduled_time in upcoming:
        print(f"  - {task.name} at {scheduled_time.strftime('%H:%M:%S')}")
    
    # Cancel a task
    print(f"\nCancelling task 3...")
    success = scheduled_task_store.cancel_scheduled_task(task_ids[2], queue_name)
    print(f"  Cancelled: {success}")
    
    # Show updated count
    count = scheduled_task_store.get_scheduled_count(queue_name)
    print(f"  Remaining tasks: {count}")
    
    # Cleanup
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    redis_broker.disconnect()


def run_all_demos():
    """Run all scheduling demonstrations."""
    print("\n" + "=" * 60)
    print("TASK SCHEDULING DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_eta_scheduling()
        demo_countdown_scheduling()
        demo_cron_recurring_tasks()
        demo_timezone_scheduling()
        demo_scheduled_task_management()
        
        # Run the main test case
        demo_10_second_delay_test()
        
        # Skip scheduler demo by default (takes 20 seconds)
        # Uncomment to run:
        # demo_scheduler_process()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

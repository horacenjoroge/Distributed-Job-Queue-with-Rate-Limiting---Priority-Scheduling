"""
Tests for task scheduling functionality.
"""
import pytest
import time
from datetime import datetime, timedelta
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority
from jobqueue.core.scheduled_tasks import scheduled_task_store
from jobqueue.core.scheduler import TaskScheduler
from jobqueue.core.cron_parser import CronParser, validate_cron_expression
from jobqueue.core.recurring_tasks import recurring_task_manager
from jobqueue.core.redis_queue import Queue
from jobqueue.utils.timezone import timezone_converter, schedule_task_at_time


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_scheduled_queue(redis_connection):
    """Clean scheduled tasks before each test."""
    queue_name = "test_scheduled_queue"
    
    # Purge scheduled tasks
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    
    # Clean execution queue
    queue = Queue(queue_name)
    queue.purge()
    
    yield queue_name
    
    # Cleanup
    scheduled_task_store.purge_scheduled_tasks(queue_name)
    queue.purge()


def test_task_eta_field():
    """Test task with eta field."""
    eta = datetime.utcnow() + timedelta(seconds=30)
    
    task = Task(
        name="test_task",
        args=[1, 2],
        eta=eta
    )
    
    assert task.eta == eta
    assert task.is_scheduled()


def test_task_countdown_field():
    """Test task with countdown field."""
    task = Task(
        name="test_task",
        args=[1, 2],
        countdown=60  # 60 seconds
    )
    
    assert task.countdown == 60
    assert task.is_scheduled()
    
    # Calculate schedule time
    task.set_schedule_time()
    assert task.schedule_time is not None
    assert task.schedule_time > datetime.utcnow()


def test_task_schedule_time_calculation():
    """Test schedule time calculation from eta and countdown."""
    # Test with eta
    eta = datetime.utcnow() + timedelta(minutes=5)
    task1 = Task(name="task1", eta=eta)
    task1.set_schedule_time()
    
    assert task1.schedule_time == eta
    
    # Test with countdown
    task2 = Task(name="task2", countdown=300)  # 5 minutes
    task2.set_schedule_time()
    
    assert task2.schedule_time is not None
    expected_time = datetime.utcnow() + timedelta(seconds=300)
    # Allow 2 second tolerance
    assert abs((task2.schedule_time - expected_time).total_seconds()) < 2


def test_task_is_ready_to_execute():
    """Test checking if task is ready to execute."""
    # Task scheduled in past (ready)
    past_time = datetime.utcnow() - timedelta(seconds=10)
    task1 = Task(name="task1", schedule_time=past_time)
    assert task1.is_ready_to_execute()
    
    # Task scheduled in future (not ready)
    future_time = datetime.utcnow() + timedelta(seconds=10)
    task2 = Task(name="task2", schedule_time=future_time)
    assert not task2.is_ready_to_execute()
    
    # Task not scheduled (ready immediately)
    task3 = Task(name="task3")
    assert task3.is_ready_to_execute()


def test_schedule_task_storage(clean_scheduled_queue):
    """Test storing scheduled tasks in Redis."""
    queue_name = clean_scheduled_queue
    
    # Create scheduled task
    schedule_time = datetime.utcnow() + timedelta(seconds=30)
    task = Task(
        name="test_task",
        args=[1, 2, 3],
        queue_name=queue_name,
        schedule_time=schedule_time
    )
    
    # Schedule task
    success = scheduled_task_store.schedule_task(task)
    assert success
    
    # Check count
    count = scheduled_task_store.get_scheduled_count(queue_name)
    assert count == 1
    
    # Get next scheduled time
    next_time = scheduled_task_store.get_next_scheduled_time(queue_name)
    assert next_time is not None
    assert abs((next_time - schedule_time).total_seconds()) < 1


def test_get_ready_tasks(clean_scheduled_queue):
    """Test retrieving ready tasks."""
    queue_name = clean_scheduled_queue
    
    # Schedule tasks with different times
    past_task = Task(
        name="past_task",
        queue_name=queue_name,
        schedule_time=datetime.utcnow() - timedelta(seconds=10)
    )
    
    future_task = Task(
        name="future_task",
        queue_name=queue_name,
        schedule_time=datetime.utcnow() + timedelta(seconds=30)
    )
    
    scheduled_task_store.schedule_task(past_task)
    scheduled_task_store.schedule_task(future_task)
    
    # Get ready tasks
    ready_tasks = scheduled_task_store.get_ready_tasks(queue_name)
    
    # Only past_task should be ready
    assert len(ready_tasks) == 1
    assert ready_tasks[0].name == "past_task"


def test_schedule_task_for_10_seconds_delay(clean_scheduled_queue):
    """Test: Schedule task for +10 seconds, verify delay."""
    queue_name = clean_scheduled_queue
    
    # Create task with 10 second countdown
    task = Task(
        name="delayed_task",
        args=["test"],
        queue_name=queue_name,
        countdown=10
    )
    
    # Set schedule time
    task.set_schedule_time()
    
    # Schedule task
    start_time = datetime.utcnow()
    scheduled_task_store.schedule_task(task)
    
    # Verify not ready immediately
    ready_tasks = scheduled_task_store.get_ready_tasks(queue_name)
    assert len(ready_tasks) == 0
    
    # Wait 11 seconds
    time.sleep(11)
    
    # Verify now ready
    ready_tasks = scheduled_task_store.get_ready_tasks(queue_name)
    assert len(ready_tasks) == 1
    assert ready_tasks[0].name == "delayed_task"
    
    # Verify delay was approximately 10 seconds
    elapsed = datetime.utcnow() - start_time
    assert 10 <= elapsed.total_seconds() <= 12


def test_cron_parser():
    """Test cron expression parsing."""
    # Every 5 minutes
    cron1 = CronParser("*/5 * * * *")
    assert 0 in cron1.fields["minute"]
    assert 5 in cron1.fields["minute"]
    assert 55 in cron1.fields["minute"]
    
    # Every hour at minute 30
    cron2 = CronParser("30 * * * *")
    assert cron2.fields["minute"] == [30]
    assert len(cron2.fields["hour"]) == 24
    
    # Weekdays at 9 AM
    cron3 = CronParser("0 9 * * 1-5")
    assert cron3.fields["minute"] == [0]
    assert cron3.fields["hour"] == [9]
    assert cron3.fields["weekday"] == [1, 2, 3, 4, 5]


def test_cron_shortcuts():
    """Test cron shortcut expressions."""
    shortcuts = {
        "@hourly": "0 * * * *",
        "@daily": "0 0 * * *",
        "@weekly": "0 0 * * 0",
        "@monthly": "0 0 1 * *",
        "@yearly": "0 0 1 1 *"
    }
    
    for shortcut, expected in shortcuts.items():
        cron = CronParser(shortcut)
        assert cron.expression == expected


def test_cron_matches():
    """Test cron expression matching."""
    # Every minute
    cron = CronParser("* * * * *")
    assert cron.matches(datetime.utcnow())
    
    # Specific time: 15:30
    cron2 = CronParser("30 15 * * *")
    dt = datetime(2024, 1, 15, 15, 30, 0)
    assert cron2.matches(dt)
    
    dt_wrong = datetime(2024, 1, 15, 15, 31, 0)
    assert not cron2.matches(dt_wrong)


def test_cron_get_next_run():
    """Test calculating next run time."""
    # Every hour at minute 0
    cron = CronParser("0 * * * *")
    
    # From 10:30, next should be 11:00
    after = datetime(2024, 1, 15, 10, 30, 0)
    next_run = cron.get_next_run(after=after)
    
    assert next_run is not None
    assert next_run.hour == 11
    assert next_run.minute == 0


def test_validate_cron_expression():
    """Test cron expression validation."""
    # Valid expressions
    assert validate_cron_expression("* * * * *")[0]
    assert validate_cron_expression("0 0 * * *")[0]
    assert validate_cron_expression("*/5 * * * *")[0]
    
    # Invalid expressions
    assert not validate_cron_expression("invalid")[0]
    assert not validate_cron_expression("* * *")[0]  # Too few fields


def test_recurring_task_registration(clean_scheduled_queue):
    """Test registering recurring tasks."""
    queue_name = clean_scheduled_queue
    
    # Register recurring task
    task = recurring_task_manager.register_recurring_task(
        name="hourly_cleanup",
        cron_expression="0 * * * *",  # Every hour
        task_name="cleanup_task",
        queue_name=queue_name,
        priority=TaskPriority.LOW
    )
    
    assert task.is_recurring
    assert task.cron_expression == "0 * * * *"
    assert task.next_run_at is not None
    
    # Check it's in registry
    registered = recurring_task_manager.get_recurring_task("hourly_cleanup")
    assert registered is not None
    assert registered.name == "cleanup_task"
    
    # Cleanup
    recurring_task_manager.unregister_recurring_task("hourly_cleanup")


def test_recurring_task_list(clean_scheduled_queue):
    """Test listing recurring tasks."""
    queue_name = clean_scheduled_queue
    
    # Register multiple tasks
    recurring_task_manager.register_recurring_task(
        name="task1",
        cron_expression="*/5 * * * *",
        task_name="task_func_1",
        queue_name=queue_name
    )
    
    recurring_task_manager.register_recurring_task(
        name="task2",
        cron_expression="0 0 * * *",
        task_name="task_func_2",
        queue_name=queue_name
    )
    
    # List tasks
    tasks = recurring_task_manager.list_recurring_tasks()
    assert len(tasks) == 2
    
    task_names = [t["name"] for t in tasks]
    assert "task1" in task_names
    assert "task2" in task_names
    
    # Cleanup
    recurring_task_manager.unregister_recurring_task("task1")
    recurring_task_manager.unregister_recurring_task("task2")


def test_scheduler_moves_ready_tasks(clean_scheduled_queue):
    """Test scheduler moves ready tasks to execution queue."""
    queue_name = clean_scheduled_queue
    
    # Create ready task (scheduled in past)
    task = Task(
        name="ready_task",
        args=[1, 2],
        queue_name=queue_name,
        schedule_time=datetime.utcnow() - timedelta(seconds=5)
    )
    
    scheduled_task_store.schedule_task(task)
    
    # Create scheduler
    scheduler = TaskScheduler(
        poll_interval=1,
        queues=[queue_name]
    )
    
    # Process queue once
    scheduler._process_queue(queue_name)
    
    # Check task moved to execution queue
    queue = Queue(queue_name)
    assert queue.size() == 1
    
    # Check removed from scheduled store
    count = scheduled_task_store.get_scheduled_count(queue_name)
    assert count == 0


def test_timezone_conversion():
    """Test timezone conversions."""
    # Create datetime in EST
    dt_est = datetime(2024, 1, 15, 9, 0, 0)  # 9 AM EST
    
    # Convert to UTC
    dt_utc = timezone_converter.to_utc(dt_est, from_timezone="America/New_York")
    
    # EST is UTC-5 (or UTC-4 during DST)
    # 9 AM EST should be 2 PM UTC (14:00) or 1 PM UTC (13:00)
    assert dt_utc.hour in [13, 14]
    
    # Convert back
    dt_back = timezone_converter.from_utc(dt_utc, to_timezone="America/New_York")
    assert dt_back.hour == 9


def test_schedule_task_at_specific_time():
    """Test scheduling task at specific time in timezone."""
    # Schedule for 9 AM EST tomorrow
    eta = schedule_task_at_time(
        hour=9,
        minute=0,
        tz_name="America/New_York",
        days_ahead=1
    )
    
    assert eta is not None
    assert isinstance(eta, datetime)
    
    # Should be in the future
    assert eta > datetime.utcnow()


def test_scheduled_task_stats(clean_scheduled_queue):
    """Test scheduled task statistics."""
    queue_name = clean_scheduled_queue
    
    # Schedule multiple tasks
    for i in range(5):
        task = Task(
            name=f"task_{i}",
            queue_name=queue_name,
            schedule_time=datetime.utcnow() + timedelta(seconds=i * 10)
        )
        scheduled_task_store.schedule_task(task)
    
    # Get stats
    stats = scheduled_task_store.get_stats(queue_name)
    
    assert stats["total_scheduled"] == 5
    assert stats["queue"] == queue_name
    assert stats["next_execution_time"] is not None


def test_cancel_scheduled_task(clean_scheduled_queue):
    """Test cancelling a scheduled task."""
    queue_name = clean_scheduled_queue
    
    # Schedule task
    task = Task(
        name="cancel_me",
        queue_name=queue_name,
        schedule_time=datetime.utcnow() + timedelta(seconds=60)
    )
    
    scheduled_task_store.schedule_task(task)
    assert scheduled_task_store.get_scheduled_count(queue_name) == 1
    
    # Cancel task
    success = scheduled_task_store.cancel_scheduled_task(task.id, queue_name)
    assert success
    
    # Verify removed
    assert scheduled_task_store.get_scheduled_count(queue_name) == 0


def test_peek_scheduled_tasks(clean_scheduled_queue):
    """Test peeking at scheduled tasks without removing them."""
    queue_name = clean_scheduled_queue
    
    # Schedule tasks
    for i in range(3):
        task = Task(
            name=f"task_{i}",
            queue_name=queue_name,
            schedule_time=datetime.utcnow() + timedelta(seconds=i * 10)
        )
        scheduled_task_store.schedule_task(task)
    
    # Peek at tasks
    tasks = scheduled_task_store.peek_scheduled_tasks(queue_name, limit=2)
    
    assert len(tasks) == 2
    
    # Verify still in store
    assert scheduled_task_store.get_scheduled_count(queue_name) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

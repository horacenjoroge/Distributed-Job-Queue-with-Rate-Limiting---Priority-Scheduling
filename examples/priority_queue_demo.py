"""
Demo of priority queue with HIGH/MEDIUM/LOW priorities.
"""
import time
from jobqueue.core.priority_queue import PriorityQueue
from jobqueue.core.task import Task, TaskPriority
from jobqueue.broker.redis_broker import redis_broker


print("=" * 70)
print("Priority Queue Demo")
print("=" * 70)


# Connect to Redis
print("\nStep 1: Connecting to Redis...")
redis_broker.connect()

# Create priority queue
queue = PriorityQueue("demo_priority_queue")
queue.purge()  # Clean slate

print(f"Priority queue '{queue.name}' initialized")
print(f"  Aging enabled: {queue.enable_aging}")
print(f"  Aging threshold: {queue.aging_threshold}s")


# Demo 1: Basic Priority Ordering
print("\n" + "=" * 70)
print("Demo 1: Priority Ordering (HIGH > MEDIUM > LOW)")
print("=" * 70)

# Enqueue tasks in mixed order
print("\nEnqueuing tasks in order: LOW, HIGH, MEDIUM")
low_task = Task(name="backup_database", priority=TaskPriority.LOW)
high_task = Task(name="critical_alert", priority=TaskPriority.HIGH)
medium_task = Task(name="send_report", priority=TaskPriority.MEDIUM)

queue.enqueue(low_task)
print(f"  Enqueued: LOW priority task ({low_task.name})")

queue.enqueue(high_task)
print(f"  Enqueued: HIGH priority task ({high_task.name})")

queue.enqueue(medium_task)
print(f"  Enqueued: MEDIUM priority task ({medium_task.name})")

print(f"\nQueue size: {queue.size()}")
print(f"Size by priority: {queue.size_by_priority()}")

# Dequeue in priority order
print("\nDequeuing tasks (should be HIGH, MEDIUM, LOW):")
for i in range(3):
    task = queue.dequeue_nowait()
    if task:
        print(f"  {i+1}. {task.priority.upper()} - {task.name}")


# Demo 2: FIFO within same priority
print("\n" + "=" * 70)
print("Demo 2: FIFO within Same Priority")
print("=" * 70)

print("\nEnqueuing 3 HIGH priority tasks in order:")
tasks = []
for i in range(1, 4):
    task = Task(name=f"urgent_task_{i}", priority=TaskPriority.HIGH)
    queue.enqueue(task)
    tasks.append(task)
    print(f"  {i}. {task.name}")
    time.sleep(0.01)  # Small delay to ensure different timestamps

print("\nDequeuing (should maintain FIFO order):")
for i in range(3):
    task = queue.dequeue_nowait()
    if task:
        print(f"  {i+1}. {task.name}")
        assert task.id == tasks[i].id, "FIFO order violated!"

print("FIFO order verified!")


# Demo 3: Dynamic Priority Adjustment
print("\n" + "=" * 70)
print("Demo 3: Dynamic Priority Adjustment")
print("=" * 70)

# Enqueue tasks
print("\nEnqueuing tasks:")
task1 = Task(name="routine_cleanup", priority=TaskPriority.LOW)
task2 = Task(name="data_sync", priority=TaskPriority.MEDIUM)

queue.enqueue(task1)
queue.enqueue(task2)

print(f"  {task1.name}: LOW priority")
print(f"  {task2.name}: MEDIUM priority")

# Change priorities
print("\nChanging priorities:")
print(f"  Promoting {task1.name} to HIGH...")
queue.change_task_priority(task1.id, TaskPriority.HIGH)

print(f"  Demoting {task2.name} to LOW...")
queue.demote_task(task2.id)

# Verify order
print("\nDequeuing after priority changes:")
first = queue.dequeue_nowait()
second = queue.dequeue_nowait()

print(f"  1st: {first.name} (priority: {first.priority})")
print(f"  2nd: {second.name} (priority: {second.priority})")


# Demo 4: Mixed Priority Scenario
print("\n" + "=" * 70)
print("Demo 4: Real-World Mixed Priority Scenario")
print("=" * 70)

print("\nEnqueuing 20 tasks with mixed priorities:")

tasks_to_add = [
    ("process_payment", TaskPriority.HIGH),
    ("send_welcome_email", TaskPriority.MEDIUM),
    ("cleanup_temp_files", TaskPriority.LOW),
    ("fraud_detection", TaskPriority.HIGH),
    ("generate_invoice", TaskPriority.MEDIUM),
    ("archive_logs", TaskPriority.LOW),
    ("password_reset", TaskPriority.HIGH),
    ("newsletter_send", TaskPriority.LOW),
    ("update_inventory", TaskPriority.MEDIUM),
    ("backup_database", TaskPriority.LOW),
]

enqueued_tasks = []
for name, priority in tasks_to_add:
    task = Task(name=name, priority=priority)
    queue.enqueue(task)
    enqueued_tasks.append(task)
    print(f"  {priority.upper():6s} - {name}")
    time.sleep(0.01)

print(f"\nTotal enqueued: {queue.size()}")
print(f"By priority: {queue.size_by_priority()}")

print("\nProcessing order:")
priority_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}

for i in range(queue.size()):
    task = queue.dequeue_nowait()
    if task:
        priority_counts[task.priority.upper()] += 1
        print(f"  {i+1:2d}. {task.priority.upper():6s} - {task.name}")

print(f"\nProcessed: {priority_counts}")


# Demo 5: Starvation Prevention
print("\n" + "=" * 70)
print("Demo 5: Starvation Prevention with Aging")
print("=" * 70)

print("\nCreating queue with aging enabled:")
aged_queue = PriorityQueue("aging_demo", enable_aging=True, aging_threshold_seconds=2.0)
aged_queue.purge()

# Add low priority task
import datetime
old_task = Task(name="old_low_priority", priority=TaskPriority.LOW)
# Simulate an old task
old_task.created_at = datetime.datetime.utcnow() - datetime.timedelta(seconds=5)

aged_queue.enqueue(old_task)
print(f"  Enqueued OLD LOW priority task (age: 5 seconds)")

# Add new high priority task
new_high_task = Task(name="new_high_priority", priority=TaskPriority.HIGH)
aged_queue.enqueue(new_high_task)
print(f"  Enqueued NEW HIGH priority task")

print("\nBefore aging:")
print(f"  Queue size: {aged_queue.size()}")

# Apply aging
print("\nApplying aging to boost old tasks...")
boosted_count = aged_queue.apply_aging()
print(f"  Boosted {boosted_count} task(s)")

# Check for starved tasks
starved = aged_queue.get_starved_tasks(threshold_seconds=3.0)
if starved:
    print(f"\nFound {len(starved)} potentially starved task(s)")

aged_queue.purge()


# Demo 6: Bulk Operations
print("\n" + "=" * 70)
print("Demo 6: Bulk Priority Changes")
print("=" * 70)

# Create multiple LOW priority tasks
print("\nCreating 5 LOW priority tasks:")
task_ids = []
for i in range(5):
    task = Task(name=f"batch_job_{i}", priority=TaskPriority.LOW)
    queue.enqueue(task)
    task_ids.append(task.id)
    print(f"  {i+1}. {task.name}")

print(f"\nQueue size by priority: {queue.size_by_priority()}")

# Bulk promote all to HIGH
print("\nPromoting all tasks to HIGH priority...")
updated = queue.bulk_change_priority(task_ids, TaskPriority.HIGH)
print(f"  Updated {updated} tasks")

print(f"\nQueue size by priority after bulk update: {queue.size_by_priority()}")


# Cleanup
print("\n" + "=" * 70)
print("Cleanup")
print("=" * 70)

queue.purge()
redis_broker.disconnect()

print("Demo completed successfully!")
print("=" * 70)

print("\nKey Concepts Demonstrated:")
print("  1. Priority ordering (HIGH > MEDIUM > LOW)")
print("  2. FIFO within same priority level")
print("  3. Dynamic priority adjustment")
print("  4. Mixed priority task processing")
print("  5. Starvation prevention with aging")
print("  6. Bulk priority operations")
print("=" * 70)

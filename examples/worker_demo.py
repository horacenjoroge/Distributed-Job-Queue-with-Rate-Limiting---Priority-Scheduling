"""
Demo of worker processing tasks from queue.
"""
import time
from jobqueue.worker.simple_worker import SimpleWorker
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task import Task
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.decorators import task
from jobqueue.core.task_registry import task_registry


print("=" * 70)
print("Worker Demo - Task Processing")
print("=" * 70)


# Define and register tasks
print("\nStep 1: Registering tasks...")

@task(name="send_email")
def send_email(to: str, subject: str):
    """Simulate sending an email."""
    print(f"    Sending email to {to}: {subject}")
    time.sleep(0.1)  # Simulate work
    return f"Email sent to {to}"

@task(name="process_data")
def process_data(data: list):
    """Process data."""
    print(f"    Processing {len(data)} items")
    result = [x * 2 for x in data]
    time.sleep(0.1)  # Simulate work
    return result

@task(name="calculate")
def calculate(a: int, b: int, operation: str = "add"):
    """Perform calculation."""
    if operation == "add":
        result = a + b
    elif operation == "multiply":
        result = a * b
    else:
        result = 0
    print(f"    Calculating: {a} {operation} {b} = {result}")
    return result

# Register tasks
task_registry.register_from_decorator(send_email)
task_registry.register_from_decorator(process_data)
task_registry.register_from_decorator(calculate)

print("Registered tasks:")
for task_name in task_registry.list_tasks():
    print(f"  - {task_name}")


# Connect to Redis and setup queue
print("\nStep 2: Connecting to Redis and setting up queue...")
redis_broker.connect()
queue = Queue("demo_queue")
queue.purge()  # Clean slate

print(f"Queue '{queue.queue_name}' ready")


# Enqueue some tasks
print("\nStep 3: Enqueuing tasks...")

tasks_to_process = [
    Task(name="send_email", args=["user@example.com", "Welcome!"]),
    Task(name="process_data", args=[[1, 2, 3, 4, 5]]),
    Task(name="calculate", args=[10, 5], kwargs={"operation": "add"}),
    Task(name="calculate", args=[10, 5], kwargs={"operation": "multiply"}),
    Task(name="send_email", args=["admin@example.com", "Daily Report"]),
]

task_ids = []
for task in tasks_to_process:
    task_ids.append(task.id)
    queue.enqueue(task)
    print(f"  Enqueued: {task.name} (ID: {task.id[:8]}...)")

print(f"Total tasks enqueued: {len(tasks_to_process)}")
print(f"Queue size: {queue.size()}")


# Start worker
print("\nStep 4: Starting worker...")
print("-" * 70)

worker = SimpleWorker(queue_name="demo_queue", poll_timeout=2)

# Start worker in a thread (since it blocks)
import threading
worker_thread = threading.Thread(target=worker.start)
worker_thread.daemon = True
worker_thread.start()

print(f"Worker {worker.worker_id} started")
print(f"  Hostname: {worker.hostname}")
print(f"  PID: {worker.pid}")


# Wait for tasks to process
print("\nStep 5: Processing tasks...")
time.sleep(5)  # Give worker time to process all tasks


# Stop worker
print("\n" + "-" * 70)
print("Step 6: Stopping worker...")
worker.stop()
time.sleep(1)  # Wait for clean shutdown


# Check results
print("\nStep 7: Checking results...")
print("-" * 70)

stats = worker.get_stats()
print(f"Worker Statistics:")
print(f"  Worker ID: {stats['worker_id']}")
print(f"  Tasks processed: {stats['tasks_processed']}")
print(f"  Tasks failed: {stats['tasks_failed']}")
print(f"  Queue: {stats['queue_name']}")

print(f"\nRetrieving task results:")
for task_id in task_ids:
    result_task = worker.get_result(task_id)
    if result_task:
        print(f"  Task {task_id[:8]}... : {result_task.status}")
        if result_task.result:
            print(f"    Result: {result_task.result}")
    else:
        print(f"  Task {task_id[:8]}... : Result not found (may have expired)")


# Cleanup
print("\nStep 8: Cleanup...")
queue.purge()
redis_broker.disconnect()
print("Redis disconnected")


print("\n" + "=" * 70)
print("Worker Demo Completed!")
print("=" * 70)
print("\nKey Points Demonstrated:")
print("  1. Task registration with @task decorator")
print("  2. Worker initialization and configuration")
print("  3. Task enqueuing to Redis queue")
print("  4. Worker processing tasks from queue")
print("  5. Task execution with args/kwargs")
print("  6. Result storage in Redis")
print("  7. Graceful worker shutdown")
print("  8. Worker statistics and result retrieval")
print("=" * 70)

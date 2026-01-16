"""
Demo of task registration and discovery features.
"""
import asyncio
from jobqueue.core.decorators import task
from jobqueue.core.task_registry import task_registry
from jobqueue.core.discovery import auto_discover_and_register
from jobqueue.core.reloader import hot_reload_manager
from jobqueue.core.task_versioning import task_version_manager


print("=" * 70)
print("Task Registration & Discovery Demo")
print("=" * 70)


# Demo 1: Basic task decoration
print("\nDemo 1: Basic Task Decoration")
print("-" * 70)

@task(name="send_email", max_retries=3)
def send_email(to: str, subject: str, body: str):
    """Send an email task."""
    print(f"Sending email to {to}: {subject}")
    return f"Email sent to {to}"

@task(name="process_data", timeout=600)
def process_data(data: list, multiplier: int = 2):
    """Process data task."""
    result = [x * multiplier for x in data]
    print(f"Processed {len(data)} items")
    return result

# Register tasks
task_registry.register_from_decorator(send_email)
task_registry.register_from_decorator(process_data)

print(f"Registered tasks: {task_registry.list_tasks()}")


# Demo 2: Execute tasks
print("\nDemo 2: Executing Registered Tasks")
print("-" * 70)

result1 = task_registry.execute("send_email", "user@example.com", "Hello", "Test email body")
print(f"Result: {result1}")

result2 = task_registry.execute("process_data", [1, 2, 3, 4, 5], multiplier=3)
print(f"Result: {result2}")


# Demo 3: Async tasks
print("\nDemo 3: Async Task Support")
print("-" * 70)

@task(name="fetch_data_async")
async def fetch_data_async(url: str):
    """Async data fetching task."""
    print(f"Fetching data from {url}...")
    await asyncio.sleep(0.1)  # Simulate network delay
    return f"Data from {url}"

task_registry.register_from_decorator(fetch_data_async)
print(f"Registered async task: fetch_data_async")


# Demo 4: Task metadata
print("\nDemo 4: Task Metadata")
print("-" * 70)

metadata = task_registry.get_metadata("send_email")
print(f"Task: send_email")
print(f"  Max retries: {metadata['max_retries']}")
print(f"  Timeout: {metadata['timeout']}")
print(f"  Priority: {metadata['priority']}")
print(f"  Queue: {metadata['queue']}")


# Demo 5: Task versioning
print("\nDemo 5: Task Versioning")
print("-" * 70)

def calculate_v1(a, b):
    """Version 1: Simple calculation."""
    return a + b

def calculate_v2(a, b, c=0):
    """Version 2: Added optional parameter."""
    return a + b + c

# Register versions
task_version_manager.register_version("calculate", calculate_v1, "1.0")
task_version_manager.register_version("calculate", calculate_v2, "2.0")

# Check for changes
if task_version_manager.has_signature_changed("calculate", calculate_v2):
    print("Signature has changed from v1.0 to v2.0!")
    
    diff = task_version_manager.compare_signatures(calculate_v1, calculate_v2)
    print(f"  Parameters added: {diff['parameter_changes']['added']}")
    print(f"  Parameters removed: {diff['parameter_changes']['removed']}")

all_versions = task_version_manager.get_all_versions("calculate")
print(f"All versions of 'calculate': {all_versions}")


# Demo 6: Filter tasks by queue
print("\nDemo 6: Filter Tasks by Queue")
print("-" * 70)

@task(name="high_priority_task", queue="high_priority")
def high_priority_task():
    return "High priority work"

@task(name="low_priority_task", queue="low_priority")
def low_priority_task():
    return "Low priority work"

task_registry.register_from_decorator(high_priority_task)
task_registry.register_from_decorator(low_priority_task)

high_tasks = task_registry.filter_by_queue("high_priority")
print(f"High priority queue tasks: {high_tasks}")

low_tasks = task_registry.filter_by_queue("low_priority")
print(f"Low priority queue tasks: {low_tasks}")


# Demo 7: Task naming conventions
print("\nDemo 7: Task Naming Conventions")
print("-" * 70)

# Using function name as task name
@task()
def auto_named_task():
    return "Using function name"

# Using custom name
@task(name="custom.task.name")
def function_with_custom_name():
    return "Using custom name"

task_registry.register_from_decorator(auto_named_task)
task_registry.register_from_decorator(function_with_custom_name)

print(f"Auto-named task: auto_named_task")
print(f"Custom-named task: custom.task.name")


# Demo 8: Handle task not found
print("\nDemo 8: Error Handling")
print("-" * 70)

try:
    task_registry.execute("nonexistent_task")
except ValueError as e:
    print(f"Expected error: {e}")


# Demo 9: Task with various argument types
print("\nDemo 9: Complex Task Arguments")
print("-" * 70)

@task()
def complex_task(*args, **kwargs):
    """Task with variable arguments."""
    print(f"  Args: {args}")
    print(f"  Kwargs: {kwargs}")
    return len(args) + len(kwargs)

task_registry.register_from_decorator(complex_task)
result = task_registry.execute("complex_task", 1, 2, 3, x=4, y=5, z=6)
print(f"Result: {result} arguments total")


# Demo 10: List all tasks with metadata
print("\nDemo 10: List All Tasks with Metadata")
print("-" * 70)

all_tasks = task_registry.list_with_metadata()
print(f"Total registered tasks: {len(all_tasks)}")
for task_info in all_tasks[:5]:  # Show first 5
    print(f"  - {task_info['name']}")
    if task_info['metadata']:
        print(f"    Priority: {task_info['metadata'].get('priority', 'N/A')}")
        print(f"    Max retries: {task_info['metadata'].get('max_retries', 'N/A')}")


print("\n" + "=" * 70)
print("Demo completed successfully!")
print("=" * 70)

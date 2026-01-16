"""
Task dependencies and chaining demonstration.

Shows how to create dependent tasks, chains, groups, and complex DAGs.
"""
from datetime import datetime
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority, TaskStatus
from jobqueue.core.task_dependencies import task_dependency_graph
from jobqueue.core.task_chain import signature, chain, group, chord
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


# Register demo task functions
@task_registry.register("fetch_data")
def fetch_data(source):
    """Simulate fetching data from a source."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching data from {source}")
    return {"source": source, "data": [1, 2, 3, 4, 5]}


@task_registry.register("process_data")
def process_data(data_dict):
    """Simulate processing data."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing data")
    data = data_dict.get("data", [])
    return {"processed": [x * 2 for x in data]}


@task_registry.register("save_results")
def save_results(processed_dict):
    """Simulate saving results."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Saving results")
    return {"saved": True, "count": len(processed_dict.get("processed", []))}


@task_registry.register("send_notification")
def send_notification(message):
    """Simulate sending notification."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Notification: {message}")
    return {"sent": True}


@task_registry.register("aggregate_results")
def aggregate_results(*args):
    """Aggregate multiple results."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Aggregating {len(args)} results")
    return {"total_results": len(args)}


def demo_basic_dependencies():
    """
    Demo 1: Basic task dependencies
    Task B depends on Task A
    """
    print("\n" + "=" * 60)
    print("Demo 1: Basic Dependencies")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_deps"
    Queue(queue_name).purge()
    
    # Create parent task
    parent_task = Task(
        name="fetch_data",
        args=["database"],
        queue_name=queue_name,
        priority=TaskPriority.HIGH
    )
    
    # Create child task that depends on parent
    child_task = Task(
        name="process_data",
        args=[{"data": [1, 2, 3]}],
        queue_name=queue_name,
        depends_on=[parent_task.id]
    )
    
    # Add dependencies to graph
    task_dependency_graph.add_dependencies(child_task)
    
    print(f"Parent task: {parent_task.id}")
    print(f"Child task: {child_task.id}")
    print(f"Child depends on: {child_task.depends_on}")
    
    # Enqueue both tasks
    queue = Queue(queue_name)
    queue.enqueue(parent_task)
    queue.enqueue(child_task)
    
    print(f"\nEnqueued 2 tasks")
    print(f"Queue size: {queue.size()}")
    
    # Cleanup
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_chain_5_tasks():
    """
    Demo 2: Chain 5 tasks sequentially
    Main test case: Chain 5 tasks, verify execution order
    """
    print("\n" + "=" * 60)
    print("Demo 2: Chain 5 Tasks (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_chain_5"
    Queue(queue_name).purge()
    
    print("Creating chain of 5 tasks:\n")
    
    # Create chain manually
    tasks = []
    previous_id = None
    
    task_names = ["fetch_data", "process_data", "save_results", "send_notification", "aggregate_results"]
    
    for i, task_name in enumerate(task_names, 1):
        depends_on = [previous_id] if previous_id else []
        
        task = Task(
            name=task_name,
            args=[f"step_{i}"],
            queue_name=queue_name,
            depends_on=depends_on
        )
        
        if depends_on:
            task_dependency_graph.add_dependencies(task)
        
        tasks.append(task)
        previous_id = task.id
        
        print(f"  Task {i}: {task_name}")
        print(f"    ID: {task.id}")
        print(f"    Depends on: {depends_on if depends_on else 'None (first task)'}")
    
    # Calculate execution order
    task_ids = [t.id for t in tasks]
    execution_order = task_dependency_graph.get_execution_order(task_ids)
    
    print(f"\nExecution order (by level):")
    for level_num, level_tasks in enumerate(execution_order):
        print(f"  Level {level_num}: {len(level_tasks)} task(s)")
        for task_id in level_tasks:
            task = next(t for t in tasks if t.id == task_id)
            print(f"    - {task.name}")
    
    print(f"\nTotal levels: {len(execution_order)} (sequential chain)")
    print(f"Each level has 1 task (tasks execute one after another)")
    
    # Cleanup
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_chain_helper():
    """
    Demo 3: Using chain() helper function
    """
    print("\n" + "=" * 60)
    print("Demo 3: chain() Helper Function")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_chain_helper"
    Queue(queue_name).purge()
    
    print("Creating chain using chain() helper:\n")
    
    # Create chain using helper
    task_chain = chain(
        signature("fetch_data", args=["api"], queue_name=queue_name),
        signature("process_data", args=[{"data": [1, 2, 3]}], queue_name=queue_name),
        signature("save_results", args=[{"processed": [2, 4, 6]}], queue_name=queue_name)
    )
    
    print("Chain structure:")
    print("  fetch_data -> process_data -> save_results")
    
    # Apply chain (creates and enqueues tasks)
    tasks = task_chain.apply_async()
    
    print(f"\nCreated {len(tasks)} tasks:")
    for i, task in enumerate(tasks, 1):
        deps = "None" if not task.depends_on else f"Task {i-1}"
        print(f"  Task {i}: {task.name} (depends on: {deps})")
    
    # Cleanup
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_pipe_operator():
    """
    Demo 4: Using | operator for chaining
    """
    print("\n" + "=" * 60)
    print("Demo 4: Pipe Operator (|) Chaining")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_pipe"
    Queue(queue_name).purge()
    
    print("Creating chain using | operator:\n")
    
    # Create chain using | operator
    task_chain = (
        signature("fetch_data", args=["database"], queue_name=queue_name) |
        signature("process_data", args=[{"data": []}], queue_name=queue_name) |
        signature("save_results", args=[{}], queue_name=queue_name) |
        signature("send_notification", args=["Complete!"], queue_name=queue_name)
    )
    
    print("Chain: fetch_data | process_data | save_results | send_notification")
    
    tasks = task_chain.apply_async()
    
    print(f"\nCreated {len(tasks)} chained tasks")
    
    # Cleanup
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_group_parallel():
    """
    Demo 5: Parallel execution with group()
    """
    print("\n" + "=" * 60)
    print("Demo 5: Parallel Execution with group()")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_group"
    Queue(queue_name).purge()
    
    print("Creating group of parallel tasks:\n")
    
    # Create group (tasks execute in parallel)
    task_group = group(
        signature("fetch_data", args=["source1"], queue_name=queue_name),
        signature("fetch_data", args=["source2"], queue_name=queue_name),
        signature("fetch_data", args=["source3"], queue_name=queue_name)
    )
    
    tasks = task_group.apply_async()
    
    print(f"Created {len(tasks)} parallel tasks:")
    for i, task in enumerate(tasks, 1):
        print(f"  Task {i}: {task.name}({task.args})")
        print(f"    Dependencies: {task.depends_on if task.depends_on else 'None (parallel)'}")
    
    print("\nAll tasks can execute simultaneously")
    
    # Cleanup
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_chord_pattern():
    """
    Demo 6: Chord pattern (parallel + callback)
    """
    print("\n" + "=" * 60)
    print("Demo 6: Chord Pattern (Parallel + Callback)")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_chord"
    Queue(queue_name).purge()
    
    print("Creating chord: 3 parallel tasks + 1 callback\n")
    
    # Parallel tasks
    parallel_tasks = [
        signature("fetch_data", args=["source1"], queue_name=queue_name),
        signature("fetch_data", args=["source2"], queue_name=queue_name),
        signature("fetch_data", args=["source3"], queue_name=queue_name)
    ]
    
    # Callback task
    callback = signature("aggregate_results", args=[], queue_name=queue_name)
    
    # Create chord
    tasks = chord(parallel_tasks, callback)
    
    print("Chord structure:")
    print("  fetch_data(source1) \\")
    print("  fetch_data(source2)  -> aggregate_results")
    print("  fetch_data(source3) /")
    
    print(f"\nCreated {len(tasks)} tasks (3 parallel + 1 callback)")
    print(f"Callback task depends on all 3 parallel tasks")
    
    # Cleanup
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_complex_dag():
    """
    Demo 7: Complex DAG with multiple levels
    """
    print("\n" + "=" * 60)
    print("Demo 7: Complex DAG Structure")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_dag"
    Queue(queue_name).purge()
    
    print("Creating complex DAG:\n")
    print("         task1")
    print("        /     \\")
    print("    task2     task3")
    print("        \\     /")
    print("         task4")
    print()
    
    # Create DAG
    task1 = Task(name="fetch_data", args=["root"], queue_name=queue_name)
    task2 = Task(name="process_data", args=[{}], queue_name=queue_name, depends_on=[task1.id])
    task3 = Task(name="process_data", args=[{}], queue_name=queue_name, depends_on=[task1.id])
    task4 = Task(name="aggregate_results", args=[], queue_name=queue_name, depends_on=[task2.id, task3.id])
    
    # Add dependencies
    task_dependency_graph.add_dependencies(task2)
    task_dependency_graph.add_dependencies(task3)
    task_dependency_graph.add_dependencies(task4)
    
    # Calculate execution order
    tasks = [task1, task2, task3, task4]
    execution_order = task_dependency_graph.get_execution_order([t.id for t in tasks])
    
    print("Execution order by level:")
    for level_num, level_tasks in enumerate(execution_order):
        print(f"\n  Level {level_num} ({len(level_tasks)} task(s) - can run in parallel):")
        for task_id in level_tasks:
            task = next(t for t in tasks if t.id == task_id)
            print(f"    - {task.name}")
    
    print(f"\nTotal execution levels: {len(execution_order)}")
    print(f"Level 1 has {len(execution_order[1])} tasks (parallel execution)")
    
    # Cleanup
    Queue(queue_name).purge()
    redis_broker.disconnect()


def demo_circular_dependency_prevention():
    """
    Demo 8: Circular dependency detection
    """
    print("\n" + "=" * 60)
    print("Demo 8: Circular Dependency Detection")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_circular"
    
    print("Attempting to create circular dependency:\n")
    
    # Create tasks
    task1 = Task(name="fetch_data", args=[1], queue_name=queue_name)
    task2 = Task(name="process_data", args=[2], queue_name=queue_name, depends_on=[task1.id])
    
    # Add task2 (valid)
    task_dependency_graph.add_dependencies(task2)
    print(f"Created: Task2 depends on Task1 (valid)")
    
    # Try to create circular: task3 depends on both task2 and task1
    # This creates a cycle: task1 -> task2 -> task1 (through task3)
    task3 = Task(name="save_results", args=[3], queue_name=queue_name, depends_on=[task2.id])
    
    try:
        # Now try to make task1 depend on task3 (would create cycle)
        task1_with_dep = Task(name="fetch_data", args=[1], queue_name=queue_name, depends_on=[task3.id])
        task_dependency_graph.add_dependencies(task1_with_dep)
        print("ERROR: Circular dependency was not detected!")
    except ValueError as e:
        print(f"\nCircular dependency detected (as expected):")
        print(f"  Error: {e}")
    
    redis_broker.disconnect()


def demo_failed_parent_handling():
    """
    Demo 9: Handling parent task failure
    """
    print("\n" + "=" * 60)
    print("Demo 9: Parent Task Failure Handling")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue_name = "demo_failure"
    
    print("Creating chain with failure simulation:\n")
    
    # Create chain
    task1 = Task(name="fetch_data", args=["api"], queue_name=queue_name)
    task2 = Task(name="process_data", args=[{}], queue_name=queue_name, depends_on=[task1.id])
    task3 = Task(name="save_results", args=[{}], queue_name=queue_name, depends_on=[task2.id])
    
    task_dependency_graph.add_dependencies(task2)
    task_dependency_graph.add_dependencies(task3)
    
    print("Chain: task1 -> task2 -> task3")
    
    # Simulate task1 failure
    print(f"\nSimulating task1 failure...")
    task_dependency_graph.set_task_status(task1.id, TaskStatus.FAILED)
    
    # Cancel dependent tasks
    cancelled = task_dependency_graph.cancel_dependent_tasks(task1.id)
    
    print(f"\nCancelled {len(cancelled)} dependent tasks:")
    for task_id in cancelled:
        status = task_dependency_graph.get_task_status(task_id)
        task_name = "task2" if task_id == task2.id else "task3"
        print(f"  - {task_name}: {status.value}")
    
    redis_broker.disconnect()


def run_all_demos():
    """Run all dependency demonstrations."""
    print("\n" + "=" * 60)
    print("TASK DEPENDENCY DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_basic_dependencies()
        demo_chain_5_tasks()  # Main test case
        demo_chain_helper()
        demo_pipe_operator()
        demo_group_parallel()
        demo_chord_pattern()
        demo_complex_dag()
        demo_circular_dependency_prevention()
        demo_failed_parent_handling()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()

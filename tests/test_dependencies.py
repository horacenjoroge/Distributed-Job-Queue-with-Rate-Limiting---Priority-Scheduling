"""
Tests for task dependencies and chaining.
"""
import pytest
import time
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority, TaskStatus
from jobqueue.core.task_dependencies import task_dependency_graph
from jobqueue.core.task_chain import (
    signature, chain, group, chord, TaskSignature, TaskChain, TaskGroup
)
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_test_queue(redis_connection):
    """Clean test queue and dependency graph."""
    queue_name = "test_deps_queue"
    
    # Clean queue
    queue = Queue(queue_name)
    queue.purge()
    
    yield queue_name
    
    # Cleanup
    queue.purge()


# Register test task functions
@task_registry.register("test_task_1")
def test_task_1(value):
    return f"task1:{value}"


@task_registry.register("test_task_2")
def test_task_2(value):
    return f"task2:{value}"


@task_registry.register("test_task_3")
def test_task_3(value):
    return f"task3:{value}"


@task_registry.register("test_task_4")
def test_task_4(value):
    return f"task4:{value}"


@task_registry.register("test_task_5")
def test_task_5(value):
    return f"task5:{value}"


def test_basic_dependencies(clean_test_queue):
    """Test basic task dependencies."""
    queue_name = clean_test_queue
    
    # Create parent task
    parent = Task(
        name="test_task_1",
        args=[1],
        queue_name=queue_name
    )
    
    # Create child task that depends on parent
    child = Task(
        name="test_task_2",
        args=[2],
        queue_name=queue_name,
        depends_on=[parent.id]
    )
    
    # Add dependencies
    task_dependency_graph.add_dependencies(child)
    
    # Check dependencies
    deps = task_dependency_graph.get_dependencies(child.id)
    assert len(deps) == 1
    assert parent.id in deps
    
    # Check dependents (reverse mapping)
    dependents = task_dependency_graph.get_dependents(parent.id)
    assert len(dependents) == 1
    assert child.id in dependents


def test_circular_dependency_detection(clean_test_queue):
    """Test detection of circular dependencies."""
    queue_name = clean_test_queue
    
    # Create tasks
    task1 = Task(name="test_task_1", args=[1], queue_name=queue_name)
    task2 = Task(name="test_task_2", args=[2], queue_name=queue_name, depends_on=[task1.id])
    
    # Add task2 dependencies (valid)
    task_dependency_graph.add_dependencies(task2)
    
    # Try to create circular dependency: task1 depends on task2
    task3 = Task(name="test_task_3", args=[3], queue_name=queue_name, depends_on=[task2.id, task1.id])
    
    # This should raise ValueError
    with pytest.raises(ValueError, match="Circular dependency"):
        task_dependency_graph.add_dependencies(task3)


def test_dependencies_satisfied_check(clean_test_queue):
    """Test checking if dependencies are satisfied."""
    queue_name = clean_test_queue
    
    # Create parent task
    parent = Task(name="test_task_1", args=[1], queue_name=queue_name)
    
    # Create child task
    child = Task(name="test_task_2", args=[2], queue_name=queue_name, depends_on=[parent.id])
    
    task_dependency_graph.add_dependencies(child)
    
    # Initially, parent is not complete
    satisfied, pending = task_dependency_graph.are_dependencies_satisfied(child.id)
    assert not satisfied
    assert parent.id in pending
    
    # Mark parent as successful
    task_dependency_graph.set_task_status(parent.id, TaskStatus.SUCCESS)
    
    # Now dependencies should be satisfied
    satisfied, pending = task_dependency_graph.are_dependencies_satisfied(child.id)
    assert satisfied
    assert len(pending) == 0


def test_failed_dependency_detection(clean_test_queue):
    """Test detection of failed dependencies."""
    queue_name = clean_test_queue
    
    # Create parent and child
    parent = Task(name="test_task_1", args=[1], queue_name=queue_name)
    child = Task(name="test_task_2", args=[2], queue_name=queue_name, depends_on=[parent.id])
    
    task_dependency_graph.add_dependencies(child)
    
    # Mark parent as failed
    task_dependency_graph.set_task_status(parent.id, TaskStatus.FAILED)
    
    # Check for failed dependencies
    has_failures, failed = task_dependency_graph.has_failed_dependencies(child.id)
    assert has_failures
    assert parent.id in failed


def test_cancel_dependent_tasks(clean_test_queue):
    """Test cancelling dependent tasks when parent fails."""
    queue_name = clean_test_queue
    
    # Create chain: parent -> child1 -> child2
    parent = Task(name="test_task_1", args=[1], queue_name=queue_name)
    child1 = Task(name="test_task_2", args=[2], queue_name=queue_name, depends_on=[parent.id])
    child2 = Task(name="test_task_3", args=[3], queue_name=queue_name, depends_on=[child1.id])
    
    task_dependency_graph.add_dependencies(child1)
    task_dependency_graph.add_dependencies(child2)
    
    # Mark parent as failed
    task_dependency_graph.set_task_status(parent.id, TaskStatus.FAILED)
    
    # Cancel dependent tasks
    cancelled = task_dependency_graph.cancel_dependent_tasks(parent.id)
    
    # Both children should be cancelled
    assert len(cancelled) == 2
    assert child1.id in cancelled
    assert child2.id in cancelled
    
    # Check statuses
    assert task_dependency_graph.get_task_status(child1.id) == TaskStatus.CANCELLED
    assert task_dependency_graph.get_task_status(child2.id) == TaskStatus.CANCELLED


def test_chain_5_tasks(clean_test_queue):
    """
    Test: Chain 5 tasks, verify execution order.
    Main test case from requirements.
    """
    queue_name = clean_test_queue
    
    # Create chain of 5 tasks
    tasks = []
    previous_id = None
    
    for i in range(1, 6):
        depends_on = [previous_id] if previous_id else []
        
        task = Task(
            name=f"test_task_{i}",
            args=[i],
            queue_name=queue_name,
            depends_on=depends_on
        )
        
        if depends_on:
            task_dependency_graph.add_dependencies(task)
        
        tasks.append(task)
        previous_id = task.id
    
    # Verify chain structure
    assert len(tasks) == 5
    
    # Task 1 has no dependencies
    deps1 = task_dependency_graph.get_dependencies(tasks[0].id)
    assert len(deps1) == 0
    
    # Task 2 depends on Task 1
    deps2 = task_dependency_graph.get_dependencies(tasks[1].id)
    assert len(deps2) == 1
    assert tasks[0].id in deps2
    
    # Task 5 depends on Task 4
    deps5 = task_dependency_graph.get_dependencies(tasks[4].id)
    assert len(deps5) == 1
    assert tasks[3].id in deps5
    
    # Verify execution order
    execution_order = task_dependency_graph.get_execution_order([t.id for t in tasks])
    
    # Should have 5 levels (sequential chain)
    assert len(execution_order) == 5
    
    # Each level should have 1 task
    for level in execution_order:
        assert len(level) == 1
    
    # Verify order matches creation order
    for i, level in enumerate(execution_order):
        assert tasks[i].id in level


def test_signature_creation():
    """Test creating task signatures."""
    sig = signature("test_task_1", args=[123], priority=TaskPriority.HIGH)
    
    assert sig.name == "test_task_1"
    assert sig.args == [123]
    assert sig.priority == TaskPriority.HIGH


def test_chain_helper(clean_test_queue):
    """Test chain() helper function."""
    queue_name = clean_test_queue
    queue = Queue(queue_name)
    
    # Create chain
    task_chain = chain(
        signature("test_task_1", args=[1], queue_name=queue_name),
        signature("test_task_2", args=[2], queue_name=queue_name),
        signature("test_task_3", args=[3], queue_name=queue_name)
    )
    
    # Apply chain
    tasks = task_chain.apply_async()
    
    assert len(tasks) == 3
    
    # Verify dependencies
    assert len(tasks[0].depends_on) == 0  # First task has no deps
    assert tasks[1].id in tasks[1].depends_on is False  # Second depends on first
    assert len(tasks[1].depends_on) == 1
    assert len(tasks[2].depends_on) == 1  # Third depends on second


def test_pipe_operator_chaining(clean_test_queue):
    """Test using | operator for chaining."""
    queue_name = clean_test_queue
    
    # Create chain using | operator
    sig1 = signature("test_task_1", args=[1], queue_name=queue_name)
    sig2 = signature("test_task_2", args=[2], queue_name=queue_name)
    sig3 = signature("test_task_3", args=[3], queue_name=queue_name)
    
    task_chain = sig1 | sig2 | sig3
    
    assert isinstance(task_chain, TaskChain)
    assert len(task_chain.signatures) == 3


def test_group_helper(clean_test_queue):
    """Test group() helper for parallel execution."""
    queue_name = clean_test_queue
    
    # Create group
    task_group = group(
        signature("test_task_1", args=[1], queue_name=queue_name),
        signature("test_task_2", args=[2], queue_name=queue_name),
        signature("test_task_3", args=[3], queue_name=queue_name)
    )
    
    # Apply group
    tasks = task_group.apply_async()
    
    assert len(tasks) == 3
    
    # All tasks should have no dependencies (parallel)
    for task in tasks:
        assert len(task.depends_on) == 0


def test_chord_pattern(clean_test_queue):
    """Test chord pattern (parallel + callback)."""
    queue_name = clean_test_queue
    
    # Create chord: 3 parallel tasks + 1 callback
    parallel_sigs = [
        signature("test_task_1", args=[1], queue_name=queue_name),
        signature("test_task_2", args=[2], queue_name=queue_name),
        signature("test_task_3", args=[3], queue_name=queue_name)
    ]
    
    callback_sig = signature("test_task_4", args=[4], queue_name=queue_name)
    
    tasks = chord(parallel_sigs, callback_sig)
    
    assert len(tasks) == 4  # 3 parallel + 1 callback
    
    # First 3 tasks should have no dependencies
    for i in range(3):
        assert len(tasks[i].depends_on) == 0
    
    # Callback should depend on all 3 parallel tasks
    callback_task = tasks[3]
    assert len(callback_task.depends_on) == 3
    
    for i in range(3):
        assert tasks[i].id in callback_task.depends_on


def test_execution_order_parallel_tasks(clean_test_queue):
    """Test execution order calculation with parallel tasks."""
    queue_name = clean_test_queue
    
    # Create DAG:
    #     task1
    #    /     \
    # task2   task3
    #    \     /
    #     task4
    
    task1 = Task(name="test_task_1", args=[1], queue_name=queue_name)
    task2 = Task(name="test_task_2", args=[2], queue_name=queue_name, depends_on=[task1.id])
    task3 = Task(name="test_task_3", args=[3], queue_name=queue_name, depends_on=[task1.id])
    task4 = Task(name="test_task_4", args=[4], queue_name=queue_name, depends_on=[task2.id, task3.id])
    
    task_dependency_graph.add_dependencies(task2)
    task_dependency_graph.add_dependencies(task3)
    task_dependency_graph.add_dependencies(task4)
    
    # Calculate execution order
    order = task_dependency_graph.get_execution_order([task1.id, task2.id, task3.id, task4.id])
    
    # Should have 3 levels:
    # Level 0: [task1]
    # Level 1: [task2, task3] (parallel)
    # Level 2: [task4]
    
    assert len(order) == 3
    assert len(order[0]) == 1  # task1
    assert len(order[1]) == 2  # task2, task3 (parallel)
    assert len(order[2]) == 1  # task4
    
    # Verify task1 is first
    assert task1.id in order[0]
    
    # Verify task2 and task3 are in second level (parallel)
    assert task2.id in order[1]
    assert task3.id in order[1]
    
    # Verify task4 is last
    assert task4.id in order[2]


def test_complex_dependency_graph(clean_test_queue):
    """Test complex dependency graph with multiple levels."""
    queue_name = clean_test_queue
    
    # Create more complex DAG
    tasks = []
    
    # Level 0: 2 independent tasks
    t1 = Task(name="test_task_1", args=[1], queue_name=queue_name)
    t2 = Task(name="test_task_2", args=[2], queue_name=queue_name)
    
    # Level 1: 2 tasks depending on level 0
    t3 = Task(name="test_task_3", args=[3], queue_name=queue_name, depends_on=[t1.id])
    t4 = Task(name="test_task_4", args=[4], queue_name=queue_name, depends_on=[t2.id])
    
    # Level 2: 1 task depending on level 1
    t5 = Task(name="test_task_5", args=[5], queue_name=queue_name, depends_on=[t3.id, t4.id])
    
    tasks = [t1, t2, t3, t4, t5]
    
    # Add dependencies
    task_dependency_graph.add_dependencies(t3)
    task_dependency_graph.add_dependencies(t4)
    task_dependency_graph.add_dependencies(t5)
    
    # Calculate execution order
    order = task_dependency_graph.get_execution_order([t.id for t in tasks])
    
    # Should have 3 levels
    assert len(order) == 3
    
    # Level 0: t1, t2 (parallel)
    assert len(order[0]) == 2
    
    # Level 1: t3, t4 (parallel)
    assert len(order[1]) == 2
    
    # Level 2: t5
    assert len(order[2]) == 1


def test_dependency_graph_stats():
    """Test getting dependency graph statistics."""
    stats = task_dependency_graph.get_stats()
    
    assert "tasks_with_dependencies" in stats
    assert isinstance(stats["tasks_with_dependencies"], int)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

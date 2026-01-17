"""
Tests for task routing to specific worker types.
"""
import pytest
import time
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority, WorkerType
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_routing import task_router, TaskRouter
from jobqueue.core.task_registry import task_registry


@pytest.fixture(scope="function")
def redis_connection():
    """Setup Redis connection for tests."""
    redis_broker.connect()
    yield redis_broker
    redis_broker.disconnect()


@pytest.fixture(scope="function")
def clean_queues(redis_connection):
    """Clean queues before each test."""
    # Clean all possible queues
    for worker_type in WorkerType:
        for priority in TaskPriority:
            queue_name = TaskRouter.get_routing_key(worker_type, priority)
            queue = Queue(queue_name)
            queue.purge()
    yield
    # Cleanup
    for worker_type in WorkerType:
        for priority in TaskPriority:
            queue_name = TaskRouter.get_routing_key(worker_type, priority)
            queue = Queue(queue_name)
            queue.purge()


@task_registry.register("test_routing_task")
def test_routing_task(value):
    """Test task function."""
    return f"Processed: {value}"


def test_routing_key_generation():
    """Test routing key generation."""
    key = TaskRouter.get_routing_key(WorkerType.CPU, TaskPriority.HIGH)
    assert key == "queue:cpu:high"
    
    key = TaskRouter.get_routing_key(WorkerType.IO, TaskPriority.MEDIUM)
    assert key == "queue:io:medium"
    
    key = TaskRouter.get_routing_key(WorkerType.GPU, TaskPriority.LOW)
    assert key == "queue:gpu:low"


def test_get_worker_queues():
    """Test getting queues for a worker type."""
    queues = task_router.get_worker_queues(WorkerType.CPU)
    
    assert len(queues) == 3
    assert "queue:cpu:high" in queues
    assert "queue:cpu:medium" in queues
    assert "queue:cpu:low" in queues
    
    # Test with specific priorities
    queues = task_router.get_worker_queues(WorkerType.IO, priorities=[TaskPriority.HIGH])
    assert len(queues) == 1
    assert "queue:io:high" in queues


def test_route_task(clean_queues):
    """Test routing a task to appropriate queue."""
    task = Task(
        name="test_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.HIGH
    )
    
    queue_name = task_router.route_task(task)
    
    assert queue_name == "queue:cpu:high"
    
    # Verify task is in the queue
    queue = Queue(queue_name)
    assert queue.size() == 1
    
    # Dequeue and verify
    dequeued = queue.dequeue_nowait()
    assert dequeued is not None
    assert dequeued.id == task.id
    assert dequeued.worker_type == WorkerType.CPU


def test_cpu_task_to_cpu_worker_only(clean_queues):
    """
    Test main case: CPU task → CPU worker only.
    """
    print("\n" + "=" * 60)
    print("Test: CPU Task → CPU Worker Only")
    print("=" * 60)
    
    print(f"\nScenario:")
    print(f"  1. Create CPU task")
    print(f"  2. Route task to CPU queue")
    print(f"  3. Verify task is in CPU queue only")
    print(f"  4. Verify task is NOT in other queues\n")
    
    # Step 1: Create CPU task
    print(f"[Step 1] Creating CPU task...")
    task = Task(
        name="test_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.MEDIUM
    )
    print(f"  Task ID: {task.id}")
    print(f"  Worker type: {task.worker_type.value}")
    print(f"  Priority: {task.priority.value}")
    
    # Step 2: Route task
    print(f"\n[Step 2] Routing task...")
    queue_name = task_router.route_task(task)
    print(f"  Routed to: {queue_name}")
    assert queue_name == "queue:cpu:medium"
    
    # Step 3: Verify task is in CPU queue
    print(f"\n[Step 3] Verifying task in CPU queue...")
    cpu_queue = Queue("queue:cpu:medium")
    assert cpu_queue.size() == 1
    print(f"  CPU queue size: {cpu_queue.size()}")
    
    # Step 4: Verify task is NOT in other queues
    print(f"\n[Step 4] Verifying task NOT in other queues...")
    io_queue = Queue("queue:io:medium")
    gpu_queue = Queue("queue:gpu:medium")
    default_queue = Queue("queue:default:medium")
    
    assert io_queue.size() == 0
    assert gpu_queue.size() == 0
    assert default_queue.size() == 0
    
    print(f"  IO queue size: {io_queue.size()}")
    print(f"  GPU queue size: {gpu_queue.size()}")
    print(f"  Default queue size: {default_queue.size()}")
    
    print(f"\nTest: PASS - CPU task routed to CPU queue only")


def test_route_with_fallback(clean_queues):
    """Test routing with fallback to default queue."""
    task = Task(
        name="test_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.HIGH
    )
    
    # Route with fallback
    queue_name = task_router.route_with_fallback(task)
    
    # Should route to CPU queue
    assert queue_name == "queue:cpu:high"
    
    # Verify task is in queue
    queue = Queue(queue_name)
    assert queue.size() == 1


def test_default_worker_type(clean_queues):
    """Test that default worker type routes to default queue."""
    task = Task(
        name="test_routing_task",
        args=[1],
        worker_type=WorkerType.DEFAULT,
        priority=TaskPriority.MEDIUM
    )
    
    queue_name = task_router.route_task(task)
    
    assert queue_name == "queue:default:medium"
    
    # Verify task is in default queue
    queue = Queue(queue_name)
    assert queue.size() == 1


def test_queue_enqueue_with_routing(clean_queues):
    """Test queue enqueue with routing enabled."""
    queue = Queue("default")
    
    # Create CPU task
    task = Task(
        name="test_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.HIGH
    )
    
    # Enqueue with routing
    task_id = queue.enqueue(task, use_routing=True)
    
    # Verify task was routed to CPU queue
    cpu_queue = Queue("queue:cpu:high")
    assert cpu_queue.size() == 1
    
    # Verify task is NOT in default queue
    assert queue.size() == 0


def test_queue_enqueue_without_routing(clean_queues):
    """Test queue enqueue without routing."""
    queue = Queue("default")
    
    # Create CPU task
    task = Task(
        name="test_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.HIGH
    )
    
    # Enqueue without routing
    task_id = queue.enqueue(task, use_routing=False)
    
    # Verify task is in default queue (not routed)
    assert queue.size() == 1
    
    # Verify task is NOT in CPU queue
    cpu_queue = Queue("queue:cpu:high")
    assert cpu_queue.size() == 0


def test_worker_subscribes_to_queues():
    """Test that workers subscribe to appropriate queues."""
    from jobqueue.worker.base_worker import Worker
    
    # CPU worker
    cpu_worker = Worker(worker_type=WorkerType.CPU)
    assert cpu_worker.worker_type == WorkerType.CPU
    assert len(cpu_worker.subscribed_queues) == 3
    assert "queue:cpu:high" in cpu_worker.subscribed_queues
    assert "queue:cpu:medium" in cpu_worker.subscribed_queues
    assert "queue:cpu:low" in cpu_worker.subscribed_queues
    
    # IO worker
    io_worker = Worker(worker_type=WorkerType.IO)
    assert io_worker.worker_type == WorkerType.IO
    assert len(io_worker.subscribed_queues) == 3
    assert "queue:io:high" in io_worker.subscribed_queues
    
    # GPU worker
    gpu_worker = Worker(worker_type=WorkerType.GPU)
    assert gpu_worker.worker_type == WorkerType.GPU
    assert len(gpu_worker.subscribed_queues) == 3
    assert "queue:gpu:high" in gpu_worker.subscribed_queues


def test_get_all_worker_type_queues():
    """Test getting all queues for all worker types."""
    all_queues = task_router.get_all_worker_type_queues()
    
    assert WorkerType.CPU in all_queues
    assert WorkerType.IO in all_queues
    assert WorkerType.GPU in all_queues
    assert WorkerType.DEFAULT in all_queues
    
    # Each should have 3 queues (high, medium, low)
    for worker_type, queues in all_queues.items():
        assert len(queues) == 3


def test_routing_different_priorities(clean_queues):
    """Test routing tasks with different priorities."""
    # High priority CPU task
    task1 = Task(
        name="test_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.HIGH
    )
    queue1 = task_router.route_task(task1)
    assert queue1 == "queue:cpu:high"
    
    # Medium priority CPU task
    task2 = Task(
        name="test_routing_task",
        args=[2],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.MEDIUM
    )
    queue2 = task_router.route_task(task2)
    assert queue2 == "queue:cpu:medium"
    
    # Low priority CPU task
    task3 = Task(
        name="test_routing_task",
        args=[3],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.LOW
    )
    queue3 = task_router.route_task(task3)
    assert queue3 == "queue:cpu:low"
    
    # Verify all in different queues
    assert queue1 != queue2 != queue3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

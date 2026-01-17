"""
Task routing demonstration.

Shows how to route tasks to specific worker types (cpu, io, gpu).
"""
from jobqueue.broker.redis_broker import redis_broker
from jobqueue.core.task import Task, TaskPriority, WorkerType
from jobqueue.core.redis_queue import Queue
from jobqueue.core.task_routing import task_router
from jobqueue.core.task_registry import task_registry
from jobqueue.utils.logger import log


@task_registry.register("demo_routing_task")
def demo_routing_task(value):
    """Demo task function."""
    return f"Processed: {value}"


def demo_routing_key_generation():
    """
    Demo 1: Routing key generation
    """
    print("\n" + "=" * 60)
    print("Demo 1: Routing Key Generation")
    print("=" * 60)
    
    print(f"\nRouting key format: queue:{{worker_type}}:{{priority}}")
    
    # CPU tasks
    print(f"\n1. CPU tasks:")
    print(f"   High priority:   {task_router.get_routing_key(WorkerType.CPU, TaskPriority.HIGH)}")
    print(f"   Medium priority: {task_router.get_routing_key(WorkerType.CPU, TaskPriority.MEDIUM)}")
    print(f"   Low priority:    {task_router.get_routing_key(WorkerType.CPU, TaskPriority.LOW)}")
    
    # IO tasks
    print(f"\n2. IO tasks:")
    print(f"   High priority:   {task_router.get_routing_key(WorkerType.IO, TaskPriority.HIGH)}")
    print(f"   Medium priority: {task_router.get_routing_key(WorkerType.IO, TaskPriority.MEDIUM)}")
    print(f"   Low priority:    {task_router.get_routing_key(WorkerType.IO, TaskPriority.LOW)}")
    
    # GPU tasks
    print(f"\n3. GPU tasks:")
    print(f"   High priority:   {task_router.get_routing_key(WorkerType.GPU, TaskPriority.HIGH)}")
    print(f"   Medium priority: {task_router.get_routing_key(WorkerType.GPU, TaskPriority.MEDIUM)}")
    print(f"   Low priority:    {task_router.get_routing_key(WorkerType.GPU, TaskPriority.LOW)}")


def demo_route_task():
    """
    Demo 2: Route task to worker type queue
    """
    print("\n" + "=" * 60)
    print("Demo 2: Route Task to Worker Type Queue")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean queues
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            queue = Queue(queue_name)
            queue.purge()
    
    # Create CPU task
    print(f"\n1. Creating CPU task...")
    cpu_task = Task(
        name="demo_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.HIGH
    )
    print(f"   Task ID: {cpu_task.id}")
    print(f"   Worker type: {cpu_task.worker_type.value}")
    print(f"   Priority: {cpu_task.priority.value}")
    
    # Route task
    print(f"\n2. Routing task...")
    queue_name = task_router.route_task(cpu_task)
    print(f"   Routed to: {queue_name}")
    
    # Verify task is in CPU queue
    print(f"\n3. Verifying task in queue...")
    cpu_queue = Queue(queue_name)
    print(f"   Queue size: {cpu_queue.size()}")
    assert cpu_queue.size() == 1
    
    # Verify task is NOT in other queues
    print(f"\n4. Verifying task NOT in other queues...")
    io_queue = Queue("queue:io:high")
    gpu_queue = Queue("queue:gpu:high")
    print(f"   IO queue size: {io_queue.size()}")
    print(f"   GPU queue size: {gpu_queue.size()}")
    assert io_queue.size() == 0
    assert gpu_queue.size() == 0
    
    print(f"\n✓ CPU task routed to CPU queue only")
    
    # Cleanup
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            queue = Queue(queue_name)
            queue.purge()
    
    redis_broker.disconnect()


def demo_worker_subscription():
    """
    Demo 3: Workers subscribe to specific queues
    """
    print("\n" + "=" * 60)
    print("Demo 3: Workers Subscribe to Specific Queues")
    print("=" * 60)
    
    from jobqueue.worker.base_worker import Worker
    
    # CPU worker
    print(f"\n1. CPU Worker:")
    cpu_worker = Worker(worker_type=WorkerType.CPU)
    print(f"   Worker type: {cpu_worker.worker_type.value}")
    print(f"   Subscribed queues: {cpu_worker.subscribed_queues}")
    assert len(cpu_worker.subscribed_queues) == 3
    assert "queue:cpu:high" in cpu_worker.subscribed_queues
    
    # IO worker
    print(f"\n2. IO Worker:")
    io_worker = Worker(worker_type=WorkerType.IO)
    print(f"   Worker type: {io_worker.worker_type.value}")
    print(f"   Subscribed queues: {io_worker.subscribed_queues}")
    assert len(io_worker.subscribed_queues) == 3
    assert "queue:io:high" in io_worker.subscribed_queues
    
    # GPU worker
    print(f"\n3. GPU Worker:")
    gpu_worker = Worker(worker_type=WorkerType.GPU)
    print(f"   Worker type: {gpu_worker.worker_type.value}")
    print(f"   Subscribed queues: {gpu_worker.subscribed_queues}")
    assert len(gpu_worker.subscribed_queues) == 3
    assert "queue:gpu:high" in gpu_worker.subscribed_queues


def demo_cpu_task_to_cpu_worker_only():
    """
    Demo 4: Main test case - CPU task → CPU worker only
    """
    print("\n" + "=" * 60)
    print("Demo 4: CPU Task → CPU Worker Only (Main Test Case)")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean queues
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            queue = Queue(queue_name)
            queue.purge()
    
    print(f"\nScenario:")
    print(f"  1. Create CPU task")
    print(f"  2. Route task to CPU queue")
    print(f"  3. Verify task is in CPU queue only")
    print(f"  4. Verify task is NOT in other queues\n")
    
    # Step 1: Create CPU task
    print(f"[Step 1] Creating CPU task...")
    cpu_task = Task(
        name="demo_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.MEDIUM
    )
    print(f"  Task ID: {cpu_task.id}")
    print(f"  Worker type: {cpu_task.worker_type.value}")
    print(f"  Priority: {cpu_task.priority.value}")
    
    # Step 2: Route task
    print(f"\n[Step 2] Routing task...")
    queue_name = task_router.route_task(cpu_task)
    print(f"  Routed to: {queue_name}")
    assert queue_name == "queue:cpu:medium"
    
    # Step 3: Verify task is in CPU queue
    print(f"\n[Step 3] Verifying task in CPU queue...")
    cpu_queue = Queue("queue:cpu:medium")
    print(f"  CPU queue size: {cpu_queue.size()}")
    assert cpu_queue.size() == 1
    
    # Step 4: Verify task is NOT in other queues
    print(f"\n[Step 4] Verifying task NOT in other queues...")
    io_queue = Queue("queue:io:medium")
    gpu_queue = Queue("queue:gpu:medium")
    default_queue = Queue("queue:default:medium")
    
    print(f"  IO queue size: {io_queue.size()}")
    print(f"  GPU queue size: {gpu_queue.size()}")
    print(f"  Default queue size: {default_queue.size()}")
    
    assert io_queue.size() == 0
    assert gpu_queue.size() == 0
    assert default_queue.size() == 0
    
    print(f"\n[Step 5] Verification:")
    print(f"  ✓ CPU task routed to CPU queue")
    print(f"  ✓ Task NOT in IO queue")
    print(f"  ✓ Task NOT in GPU queue")
    print(f"  ✓ Task NOT in default queue")
    
    print(f"\nDemo: PASS - CPU task routed to CPU worker only")
    
    # Cleanup
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            queue = Queue(queue_name)
            queue.purge()
    
    redis_broker.disconnect()


def demo_queue_enqueue_with_routing():
    """
    Demo 5: Queue enqueue with routing
    """
    print("\n" + "=" * 60)
    print("Demo 5: Queue Enqueue with Routing")
    print("=" * 60)
    
    redis_broker.connect()
    
    queue = Queue("default")
    
    # Clean queues
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            q = Queue(queue_name)
            q.purge()
    queue.purge()
    
    # Create CPU task
    print(f"\n1. Creating CPU task...")
    cpu_task = Task(
        name="demo_routing_task",
        args=[1],
        worker_type=WorkerType.CPU,
        priority=TaskPriority.HIGH
    )
    
    # Enqueue with routing
    print(f"\n2. Enqueuing with routing enabled...")
    task_id = queue.enqueue(cpu_task, use_routing=True)
    print(f"   Task ID: {task_id}")
    
    # Verify task was routed to CPU queue
    print(f"\n3. Verifying task in CPU queue...")
    cpu_queue = Queue("queue:cpu:high")
    print(f"   CPU queue size: {cpu_queue.size()}")
    assert cpu_queue.size() == 1
    
    # Verify task is NOT in default queue
    print(f"\n4. Verifying task NOT in default queue...")
    print(f"   Default queue size: {queue.size()}")
    assert queue.size() == 0
    
    print(f"\n✓ Task routed to CPU queue, not default queue")
    
    # Cleanup
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            q = Queue(queue_name)
            q.purge()
    queue.purge()
    
    redis_broker.disconnect()


def demo_fallback_to_default():
    """
    Demo 6: Fallback to default queue
    """
    print("\n" + "=" * 60)
    print("Demo 6: Fallback to Default Queue")
    print("=" * 60)
    
    redis_broker.connect()
    
    # Clean queues
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            queue = Queue(queue_name)
            queue.purge()
    
    # Create task with default worker type
    print(f"\n1. Creating task with default worker type...")
    task = Task(
        name="demo_routing_task",
        args=[1],
        worker_type=WorkerType.DEFAULT,
        priority=TaskPriority.MEDIUM
    )
    
    # Route task
    print(f"\n2. Routing task...")
    queue_name = task_router.route_task(task)
    print(f"   Routed to: {queue_name}")
    assert queue_name == "queue:default:medium"
    
    # Verify task is in default queue
    print(f"\n3. Verifying task in default queue...")
    default_queue = Queue(queue_name)
    print(f"   Default queue size: {default_queue.size()}")
    assert default_queue.size() == 1
    
    print(f"\n✓ Task routed to default queue")
    
    # Cleanup
    for wt in WorkerType:
        for p in TaskPriority:
            queue_name = task_router.get_routing_key(wt, p)
            queue = Queue(queue_name)
            queue.purge()
    
    redis_broker.disconnect()


def run_all_demos():
    """Run all task routing demonstrations."""
    print("\n" + "=" * 60)
    print("TASK ROUTING DEMONSTRATIONS")
    print("=" * 60)
    
    try:
        demo_routing_key_generation()
        demo_route_task()
        demo_worker_subscription()
        demo_cpu_task_to_cpu_worker_only()  # Main test case
        demo_queue_enqueue_with_routing()
        demo_fallback_to_default()
        
        print("\n" + "=" * 60)
        print("All demonstrations completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_all_demos()
